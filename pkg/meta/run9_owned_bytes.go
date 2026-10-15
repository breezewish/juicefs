package meta

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// A mounted Run9 writable epoch keeps current how many bytes of its own
// slices are still read: the slices that checkpoint GC will keep after the
// next close. The count uses GC's own rule on the live metadata, so it needs
// no knowledge of which operation changed what. To stay cheap it recounts
// only where metadata changed.
//
// Inode keys encode the inode little-endian, so the two key bytes after the
// prefix split all inodes evenly into 65,536 contiguous key ranges. Each range
// tracks unique slices. A committed transaction marks the ranges it wrote; a
// background worker recounts those ranges and joins their slice sets.
const run9OwnedBuckets = 1 << 16

// More marked ranges than this are recounted by one pass over all inode keys.
const run9OwnedFullPass = run9OwnedBuckets / 8

// Run9OwnedRecountInterval is how long the worker rests after each recount.
// The count is for people watching a disk, who do not need it by the second.
var Run9OwnedRecountInterval = 30 * time.Second

// Run9OwnedEpochRange is an inclusive ownership range, shared with GC's proof.
type Run9OwnedEpochRange struct {
	StartEpoch uint64 `json:"start_epoch"`
	EndEpoch   uint64 `json:"end_epoch"`
}

// Run9OwnedBytesConfig describes the source's ownership and existing Fork proofs.
type Run9OwnedBytesConfig struct {
	Epoch           uint64
	AllocationStart uint64
	ProtectedIDs    []uint64
	ForkFloor       uint64
	LastClosedRaw   *int64
	Basis           uint64
	OwnershipRanges []Run9OwnedEpochRange
}

type run9OwnedSliceReference struct {
	size  uint32
	count uint32
}

// Each bucket retains unique slice IDs, and references joins those sets across
// buckets. Reflinks then count once even when their files occupy different key
// ranges. Memory grows with slice/bucket memberships: one uint64 ID and uint32
// size per membership, plus an ID, size and reference count per unique slice
// (and Go map overhead), instead of one eight-byte sum per bucket.
type run9OwnedBytes struct {
	basis  atomic.Uint64
	epoch  atomic.Uint64
	marked [run9OwnedBuckets / 64]atomic.Uint64

	// recount serializes live counts and updates to Fork protection.
	// A channel lets a capture stop waiting when its deadline expires.
	recount         chan struct{}
	buckets         []map[uint64]uint32
	references      map[uint64]run9OwnedSliceReference
	visible         atomic.Uint64
	offset          atomic.Int64
	protected       map[uint64]struct{}
	ownershipRanges []Run9OwnedEpochRange
	forkFloor       uint64
	current         atomic.Bool

	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

func run9OwnedBucket(key []byte) uint16 {
	return uint16(key[1])<<8 | uint16(key[2])
}

// changed marks the ranges a transaction wrote, after it has committed.
func (o *run9OwnedBytes) changed(buckets []uint16) {
	for _, b := range buckets {
		o.marked[b/64].Or(1 << (b % 64))
	}
}

func (o *run9OwnedBytes) markAll() {
	for i := range o.marked {
		o.marked[i].Store(^uint64(0))
	}
}

// takeMarked clears and returns the marked ranges. Clearing comes before the
// recount reads: a change committed after that read marks its range again.
func (o *run9OwnedBytes) takeMarked() []uint16 {
	var buckets []uint16
	for i := range o.marked {
		for word := o.marked[i].Swap(0); word != 0; word &= word - 1 {
			buckets = append(buckets, uint16(i*64+bits.TrailingZeros64(word)))
		}
	}
	return buckets
}

func (o *run9OwnedBytes) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "run9_owned_basis",
			Help: "Identity of the raw count baseline supplied by run9rt.",
		}, func() float64 { return float64(o.basis.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "run9_owned_visible_bytes",
			Help: "Unique own slice bytes still read and not protected for Forks.",
		}, func() float64 { return float64(o.visible.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "run9_owned_kept_for_forks_bytes",
			Help: "Signed baseline preserving closed counts and bytes newly protected for Forks.",
		}, func() float64 { return float64(o.offset.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "run9_owned_current",
			Help: "1 when every change committed before the last recount is counted.",
		}, func() float64 {
			if o.current.Load() {
				return 1
			}
			return 0
		}),
	}
}

// Run9TrackOwnedBytes initializes the live count before writers are exposed.
// protected and forkFloor use the same child-checkpoint evidence as GC. The
// signed baseline preserves the previous close's raw count when that evidence
// changes; Portal's later physical measurement corrects the absolute baseline.
func (m *kvMeta) Run9TrackOwnedBytes(ctx context.Context, cfg Run9OwnedBytesConfig) error {
	epoch, allocationStart := cfg.Epoch, cfg.AllocationStart
	client, ok := m.client.(*badgerClient)
	if !ok || epoch == 0 {
		return nil
	}
	o := &m.run9Owned
	o.basis.Store(cfg.Basis)
	if cfg.Basis == ^uint64(0) {
		// Exhausted baseline identities permanently disable the optional count.
		return nil
	}
	o.buckets = make([]map[uint64]uint32, run9OwnedBuckets)
	o.references = make(map[uint64]run9OwnedSliceReference)
	o.ownershipRanges = cfg.OwnershipRanges
	if len(o.ownershipRanges) == 0 {
		o.ownershipRanges = []Run9OwnedEpochRange{{StartEpoch: epoch, EndEpoch: epoch}}
	}
	historical := false
	for _, r := range o.ownershipRanges {
		if r.StartEpoch > r.EndEpoch || r.EndEpoch >= 1<<32 {
			return fmt.Errorf("invalid owned epoch range")
		}
		historical = historical || r.StartEpoch != epoch || r.EndEpoch != epoch
	}
	if !o.owns(epoch << 32) {
		return fmt.Errorf("writable epoch is outside ownership ranges")
	}
	o.protected = make(map[uint64]struct{}, len(cfg.ProtectedIDs))
	for _, id := range cfg.ProtectedIDs {
		if o.owns(id) {
			o.protected[id] = struct{}{}
		}
	}
	// Legacy unknown bounds protect existing allocations, never future writes.
	o.forkFloor = min(cfg.ForkFloor, allocationStart)
	o.epoch.Store(epoch)
	o.recount = make(chan struct{}, 1)
	if historical || allocationStart > epoch<<32 {
		o.markAll()
	}
	if err := m.Run9RecountOwnedBytes(ctx); err != nil {
		// A baseline cannot be reconstructed after exposing writers. Omit this
		// mount's optional count; the next mount can initialize it again.
		o.epoch.Store(0)
		o.current.Store(false)
		return err
	}
	if cfg.LastClosedRaw != nil {
		o.offset.Store(*cfg.LastClosedRaw - int64(o.visible.Load()))
	}
	o.stop, o.stopped = make(chan struct{}), make(chan struct{})
	client.run9Owned.Store(o)
	go m.run9OwnedWorker()
	return nil
}

func (m *kvMeta) run9OwnedWorker() {
	o := &m.run9Owned
	defer close(o.stopped)
	for {
		select {
		case <-o.stop:
			return
		case <-time.After(Run9OwnedRecountInterval):
			_ = m.Run9RecountOwnedBytes(context.Background())
		}
	}
}

func (m *kvMeta) run9OwnedCount(ctx context.Context, buckets []uint16) (map[uint16]map[uint64]uint32, error) {
	counted := make(map[uint16]map[uint64]uint32, len(buckets))
	if len(buckets) > run9OwnedFullPass {
		return counted, m.run9CountOwned(ctx, []byte{'A'}, counted)
	}
	for _, b := range buckets {
		if err := m.run9CountOwned(ctx, []byte{'A', byte(b >> 8), byte(b)}, counted); err != nil {
			return nil, err
		}
	}
	return counted, nil
}

// Run9RecountOwnedBytes refreshes marked ranges. Concurrent writes mark them
// again; initialization and finalize call it before and after exposed writers.
func (m *kvMeta) Run9RecountOwnedBytes(ctx context.Context) error {
	o := &m.run9Owned
	if o.epoch.Load() == 0 {
		return nil
	}
	select {
	case o.recount <- struct{}{}:
		defer func() { <-o.recount }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	buckets := o.takeMarked()
	counted, err := m.run9OwnedCount(ctx, buckets)
	if err != nil {
		o.changed(buckets)
		o.current.Store(false)
		return err
	}
	visible := o.visible.Load()
	for _, b := range buckets {
		for id := range o.buckets[b] {
			ref := o.references[id]
			ref.count--
			if ref.count == 0 {
				if !o.keptForFork(id) {
					visible -= uint64(ref.size)
				}
				delete(o.references, id)
			} else {
				o.references[id] = ref
			}
		}
		for id, size := range counted[b] {
			ref := o.references[id]
			if ref.count == 0 {
				if !o.keptForFork(id) {
					visible += uint64(size)
				}
				ref.size = size
			}
			ref.count++
			o.references[id] = ref
		}
		o.buckets[b] = counted[b]
	}
	o.visible.Store(visible)
	o.current.Store(true)
	return nil
}

// run9CountOwned collects unique own slices read by linked regular files.
// The chunk keys sort before the inode attributes. It uses the same overlay
// as checkpoint GC, but deduplicates references across chunks and files.
func (m *kvMeta) run9CountOwned(ctx context.Context, prefix []byte, counted map[uint16]map[uint64]uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var inode Ino
	var chunks [][]byte
	var scanErr error
	err := m.client.scan(prefix, func(k, v []byte) bool {
		if ctx.Err() != nil {
			return false
		}
		if len(k) < 10 {
			scanErr = fmt.Errorf("malformed inode key")
			return false
		}
		if current := m.decodeInode(k[1:9]); current != inode {
			inode, chunks = current, chunks[:0]
		}
		switch k[9] {
		case 'C':
			if len(k) != 14 || len(v)%sliceBytes != 0 {
				scanErr = fmt.Errorf("malformed chunk record")
				return false
			}
			for _, s := range readSliceBuf(v) {
				if s.len == 0 || uint64(s.pos)+uint64(s.len) > ChunkSize || (s.id != 0 && (s.size == 0 || s.size > ChunkSize || uint64(s.off)+uint64(s.len) > uint64(s.size))) {
					scanErr = fmt.Errorf("invalid chunk slice")
					return false
				}
			}
			chunks = append(chunks, v)
		case 'I':
			if len(chunks) == 0 {
				return true
			}
			if len(v) != 64 && len(v) != 72 && len(v) != 80 {
				scanErr = fmt.Errorf("malformed inode attributes")
				return false
			}
			var attr Attr
			attr.Unmarshal(v)
			if attr.Typ != TypeFile {
				scanErr = fmt.Errorf("chunk belongs to non-file inode")
				return false
			}
			if attr.Nlink == 0 {
				return true
			}
			bucket := run9OwnedBucket(k)
			add := func(id uint64, size uint32) {
				if !m.run9Owned.owns(id) {
					return
				}
				if counted[bucket] == nil {
					counted[bucket] = make(map[uint64]uint32)
				}
				counted[bucket][id] = size
			}
			for _, chunk := range chunks {
				if len(chunk) == sliceBytes {
					var s slice
					s.read(chunk)
					if s.len > 0 {
						add(s.id, s.size)
					}
					continue
				}
				for _, s := range buildSlice(readSliceBuf(chunk)) {
					add(s.Id, s.Size)
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if scanErr != nil {
		return scanErr
	}
	return ctx.Err()
}

func (o *run9OwnedBytes) owns(id uint64) bool {
	if id == 0 {
		return false
	}
	epoch := id >> 32
	for _, r := range o.ownershipRanges {
		if epoch >= r.StartEpoch && epoch <= r.EndEpoch {
			return true
		}
	}
	return false
}

// keptForFork must be called while holding recount or before starting writers.
func (o *run9OwnedBytes) keptForFork(id uint64) bool {
	_, protected := o.protected[id]
	return protected || id < o.forkFloor
}

// Run9RecordFork reads the immutable captured metadata before its candidate is
// exposed to the caller. Allocation order cannot prove which slices a capture
// received: background compaction can commit an earlier allocation afterwards.
// Only captured own IDs become protected. Reading happens outside the recount
// gate so live writes and counts may proceed while the snapshot is scanned.
func (m *kvMeta) Run9RecordFork(ctx context.Context, directory string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o := &m.run9Owned
	if o.epoch.Load() == 0 {
		return nil
	}
	snapshot, err := OpenRun9Checkpoint(directory)
	if err != nil {
		return err
	}
	defer snapshot.Shutdown()
	format, err := snapshot.Load(true)
	if err != nil {
		return err
	}
	if format.Name != m.getFormat().Name {
		return fmt.Errorf("captured metadata belongs to another format")
	}
	snapshot.run9Owned.ownershipRanges = o.ownershipRanges
	counted := make(map[uint16]map[uint64]uint32)
	if err := snapshot.run9CountOwned(ctx, []byte{'A'}, counted); err != nil {
		return err
	}
	if err := snapshot.Shutdown(); err != nil {
		return err
	}
	select {
	case o.recount <- struct{}{}:
		defer func() { <-o.recount }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	visible, offset := o.visible.Load(), o.offset.Load()
	for _, bucket := range counted {
		for id, size := range bucket {
			if o.keptForFork(id) {
				continue
			}
			o.protected[id] = struct{}{}
			offset += int64(size)
			if ref := o.references[id]; ref.count != 0 {
				visible -= uint64(ref.size)
			}
		}
	}
	o.visible.Store(visible)
	o.offset.Store(offset)
	return nil
}

// stopWorker returns once no recount is running any more.
func (o *run9OwnedBytes) stopWorker() {
	if o.stop == nil {
		return
	}
	o.stopOnce.Do(func() { close(o.stop) })
	<-o.stopped
}

// Run9OwnedBytes is what a closing mount reports.
type Run9OwnedBytes struct {
	// VisibleBytes are unique own slices read and not protected for a Fork.
	VisibleBytes uint64 `json:"visible_bytes"`
	// KeptForForksBytes is the signed baseline. VisibleBytes plus this offset
	// is the raw count; it preserves the previous close across evidence changes.
	KeptForForksBytes int64 `json:"kept_for_forks_bytes"`
}

// Run9SettleOwnedBytes stops the worker and counts what is still marked. The
// worker keeps that little; right after a change of most files it is one pass
// over the inode keys. It reports nothing when the count fails.
func (m *kvMeta) Run9SettleOwnedBytes() *Run9OwnedBytes {
	o := &m.run9Owned
	if o.epoch.Load() == 0 {
		return nil
	}
	o.stopWorker()
	if err := m.Run9RecountOwnedBytes(context.Background()); err != nil {
		return nil
	}
	return &Run9OwnedBytes{VisibleBytes: o.visible.Load(), KeptForForksBytes: o.offset.Load()}
}
