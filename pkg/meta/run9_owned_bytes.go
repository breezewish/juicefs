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

	// recount serializes live counts and updates to Fork protection. A channel
	// lets a recount stop waiting when its context ends.
	recount         chan struct{}
	buckets         []map[uint64]uint32
	references      map[uint64]run9OwnedSliceReference
	visible         atomic.Uint64
	offset          atomic.Int64
	protected       map[uint64]struct{}
	ownershipRanges []Run9OwnedEpochRange
	forkFloor       uint64
	current         atomic.Bool

	// forkSnapshot is held while a pinned Fork snapshot may read the database:
	// by a capture until it pins or fails, then by that Fork's record until it
	// is applied. Closing the database waits for it. forkPending withholds
	// samples until the record is applied.
	forkSnapshot chan struct{}
	forkPending  atomic.Bool

	// records is cancelled when the mount stops counting, ending Fork records.
	records       context.Context
	cancelRecords context.CancelFunc

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
			Help: "1 when every change committed before the last recount and every Fork are counted.",
		}, func() float64 {
			if o.sampleCurrent() {
				return 1
			}
			return 0
		}),
	}
}

// sampleCurrent reports whether the live figure may be published: the last
// recount is current and no Fork record is pending.
func (o *run9OwnedBytes) sampleCurrent() bool {
	return o.current.Load() && !o.forkPending.Load()
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
		o.disable()
		return err
	}
	if cfg.LastClosedRaw != nil {
		o.offset.Store(*cfg.LastClosedRaw - int64(o.visible.Load()))
	}
	o.stop, o.stopped = make(chan struct{}), make(chan struct{})
	o.records, o.cancelRecords = context.WithCancel(context.Background())
	o.forkSnapshot = make(chan struct{}, 1)
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
		return counted, m.run9CountOwned(ctx, m.client.scan, []byte{'A'}, counted)
	}
	for _, b := range buckets {
		if err := m.run9CountOwned(ctx, m.client.scan, []byte{'A', byte(b >> 8), byte(b)}, counted); err != nil {
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
	if o.epoch.Load() == 0 {
		return nil // A failed Fork record turned the count off.
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
func (m *kvMeta) run9CountOwned(ctx context.Context, scan func(prefix []byte, handler func(key, value []byte) bool) error, prefix []byte, counted map[uint16]map[uint64]uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var inode Ino
	var chunks [][]byte
	var scanErr error
	err := scan(prefix, func(k, v []byte) bool {
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

// Run9ForkRecord holds what a running Fork received until the capture's
// caller knows whether the Fork proceeds. Call exactly one of Start or Discard;
// both accept nil, which a mount without a live count returns.
type Run9ForkRecord struct {
	m        *kvMeta
	snapshot *badgerSnapshot
}

// run9PinForkRecord must run while the capture holds dbMu exclusively and the
// forkSnapshot slot, so that the snapshot is exactly the captured metadata.
func (m *kvMeta) run9PinForkRecord(c *badgerClient) *Run9ForkRecord {
	m.run9Owned.forkPending.Store(true)
	return &Run9ForkRecord{m: m, snapshot: c.pinSnapshot()}
}

// Start protects the Fork's own slices in the background. The scan never
// delays the Fork; samples stay withheld until it is applied.
func (r *Run9ForkRecord) Start() {
	if r != nil {
		go r.m.run9RecordFork(r.snapshot)
	}
}

// Discard drops the record of a Fork that did not proceed.
func (r *Run9ForkRecord) Discard() {
	if r != nil {
		r.snapshot.release()
		r.m.run9Owned.endForkRecord()
	}
}

// run9RecordFork protects the own slices in a pinned Fork snapshot. Allocation
// order cannot prove which slices a capture received: background compaction can
// commit an earlier allocation afterwards. A failed or cancelled record turns
// this mount's optional count off; the Fork itself is unaffected.
func (m *kvMeta) run9RecordFork(snapshot *badgerSnapshot) {
	o := &m.run9Owned
	defer o.endForkRecord()
	counted := make(map[uint16]map[uint64]uint32)
	err := m.run9CountOwned(o.records, snapshot.scan, []byte{'A'}, counted)
	snapshot.release()
	o.recount <- struct{}{}
	defer func() { <-o.recount }()
	if err != nil {
		logger.Warnf("owned bytes unavailable until remount: record running Fork: %s", err)
		o.disable()
		return
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
}

func (o *run9OwnedBytes) endForkRecord() {
	o.forkPending.Store(false)
	<-o.forkSnapshot
}

// disable turns this mount's optional count off until remount.
func (o *run9OwnedBytes) disable() {
	o.epoch.Store(0)
	o.current.Store(false)
}

// acquireForkSnapshot takes the slot once no Fork snapshot reads the database.
// It reports false for a mount without a live count, which has no slot.
func (o *run9OwnedBytes) acquireForkSnapshot(ctx context.Context) (bool, error) {
	if o.forkSnapshot == nil {
		return false, nil
	}
	select {
	case o.forkSnapshot <- struct{}{}:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// waitForkRecords returns once no Fork record is running.
func (o *run9OwnedBytes) waitForkRecords(ctx context.Context) error {
	held, err := o.acquireForkSnapshot(ctx)
	if held {
		<-o.forkSnapshot
	}
	return err
}

// Run9WaitForkRecord waits for the previous Fork's record before a capture
// seals writes. Compaction keeps committing while a capture waits, so waiting
// after CaptureUploads could capture metadata that references uploads it never
// waits for. Captures are serialized, so no other record starts meanwhile.
func (m *kvMeta) Run9WaitForkRecord(ctx context.Context) error {
	return m.run9Owned.waitForkRecords(ctx)
}

// stopWorker returns once no recount is running any more.
func (o *run9OwnedBytes) stopWorker() {
	if o.stop == nil {
		return
	}
	o.stopOnce.Do(func() { close(o.stop) })
	<-o.stopped
}

// stopCounting stops the worker and cancels running Fork records, which then
// turn the count off. It returns once no record reads the database.
func (o *run9OwnedBytes) stopCounting() {
	if o.stop == nil {
		return
	}
	o.stopWorker()
	o.cancelRecords()
	_ = o.waitForkRecords(context.Background())
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
// over the inode keys. A Fork record still scanning is cancelled rather than
// extending finalize. It reports nothing when the count fails.
func (m *kvMeta) Run9SettleOwnedBytes() *Run9OwnedBytes {
	o := &m.run9Owned
	if o.epoch.Load() == 0 {
		return nil
	}
	o.stopCounting()
	if o.epoch.Load() == 0 {
		return nil
	}
	if err := m.Run9RecountOwnedBytes(context.Background()); err != nil {
		return nil
	}
	return &Run9OwnedBytes{VisibleBytes: o.visible.Load(), KeptForForksBytes: o.offset.Load()}
}
