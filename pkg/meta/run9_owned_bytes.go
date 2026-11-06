package meta

import (
	"context"
	"fmt"
	"maps"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
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

// Run9OwnedInitBudget bounds the background initialization: waiting for Fork
// protection, reading it, and counting the stopped metadata.
var Run9OwnedInitBudget = 5 * time.Second

// Run9OwnedBytesConfig describes the source's ownership and the previous close.
type Run9OwnedBytesConfig struct {
	Epoch           uint64
	AllocationStart uint64
	LastClosedRaw   *int64
	Basis           uint64
	OwnershipRanges []Run9OwnedEpochRange
	// Protection supplies what existing Forks retain. Initialization calls it
	// in the background, after writers are admitted; nil protects nothing.
	Protection func(context.Context) (Run9ForkProtection, error)
}

// Run9ForkProtection is checkpoint GC's evidence of what existing Forks
// retain: the exact slices of their fork checkpoints, and a conservative
// allocation floor for Forks without a usable one.
type Run9ForkProtection struct {
	IDs   []uint64
	Floor uint64
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

	// forkSnapshot is held while a pinned snapshot may read the database and
	// until what it records is applied: by initialization, by a capture until
	// it pins or fails, then by that Fork's record. Closing the database waits
	// for it. forkPending withholds samples until a Fork record is applied;
	// before initialization is applied, current is still false.
	forkSnapshot chan struct{}
	forkPending  atomic.Bool

	// records is cancelled when the mount stops counting, ending
	// initialization and Fork records.
	records       context.Context
	cancelRecords context.CancelFunc

	// initialized is closed once initialization has succeeded or failed.
	initialized chan struct{}
	stop        chan struct{}
	stopped     chan struct{}
	stopOnce    sync.Once
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

// Run9TrackOwnedBytes starts the live count before writers are admitted, but
// does no I/O on the mount's path. It pins the stopped metadata and installs
// the change marks in the same commit gate, so every later commit marks the
// ranges it writes. Initialization then runs in the background: it waits for
// Fork protection, counts the snapshot as the baseline V0 and applies it,
// after which the next recount adds exactly the changes made since the
// snapshot. Until then samples are not current. A failed initialization
// turns this mount's optional count off; it never affects the mount.
func (m *kvMeta) Run9TrackOwnedBytes(cfg Run9OwnedBytesConfig) error {
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
	o.buckets = make([]map[uint64]uint32, run9OwnedBuckets)
	o.references = make(map[uint64]run9OwnedSliceReference)
	o.protected = make(map[uint64]struct{})
	o.epoch.Store(epoch)
	o.recount = make(chan struct{}, 1)
	// Initialization holds the slot: a capture can neither close the database
	// under its snapshot nor record a Fork before the baseline exists.
	o.forkSnapshot = make(chan struct{}, 1)
	o.forkSnapshot <- struct{}{}
	o.initialized, o.stop, o.stopped = make(chan struct{}), make(chan struct{}), make(chan struct{})
	o.records, o.cancelRecords = context.WithCancel(context.Background())
	var stopped *badgerSnapshot
	client.dbMu.Lock()
	client.run9Owned.Store(o)
	// Before this epoch allocates, only historical ranges can hold own slices.
	if historical || allocationStart > epoch<<32 {
		stopped = client.pinSnapshot()
	}
	client.dbMu.Unlock()
	go m.run9OwnedWorker(cfg, stopped)
	return nil
}

func (m *kvMeta) run9OwnedWorker(cfg Run9OwnedBytesConfig, stopped *badgerSnapshot) {
	o := &m.run9Owned
	defer close(o.stopped)
	started := time.Now()
	err := m.run9InitOwnedBytes(cfg, stopped)
	close(o.initialized)
	if err != nil {
		logger.Warnf("owned bytes initialization unavailable: dur_ms=%d budget_ms=%d error=%s", time.Since(started).Milliseconds(), Run9OwnedInitBudget.Milliseconds(), err)
		return
	}
	logger.Infof("owned bytes initialization finished: dur_ms=%d budget_ms=%d", time.Since(started).Milliseconds(), Run9OwnedInitBudget.Milliseconds())
	for {
		_ = m.Run9RecountOwnedBytes(context.Background())
		select {
		case <-o.stop:
			return
		case <-time.After(Run9OwnedRecountInterval):
		}
	}
}

// run9InitOwnedBytes applies the baseline: the stopped metadata's slice sets,
// V0 under the Fork protection, and the signed offset that keeps the previous
// close's raw count. Each range's set is then exact as of the snapshot, and
// every range changed since is marked, which is what a recount requires. A
// baseline cannot be reconstructed after writers changed the metadata, so a
// failure, the budget, or a stop leaves the count off until remount.
func (m *kvMeta) run9InitOwnedBytes(cfg Run9OwnedBytesConfig, stopped *badgerSnapshot) error {
	o := &m.run9Owned
	defer func() { <-o.forkSnapshot }()
	ctx, cancel := context.WithTimeout(o.records, Run9OwnedInitBudget)
	defer cancel()
	// The snapshot count does not depend on the protection; run9rt may still
	// be reading the lineage while the snapshot is scanned.
	counted := make(map[uint16]map[uint64]uint32)
	var protection Run9ForkProtection
	group, ctx := errgroup.WithContext(ctx)
	if stopped != nil {
		group.Go(func() error {
			defer stopped.release()
			return m.run9CountOwned(ctx, stopped.scan, []byte{'A'}, counted)
		})
	}
	if cfg.Protection != nil {
		group.Go(func() (err error) {
			protection, err = cfg.Protection(ctx)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		o.disable()
		return err
	}
	o.recount <- struct{}{}
	for _, id := range protection.IDs {
		if o.owns(id) {
			o.protected[id] = struct{}{}
		}
	}
	// Legacy unknown bounds protect existing allocations, never future writes.
	o.forkFloor = min(protection.Floor, cfg.AllocationStart)
	o.replace(slices.Collect(maps.Keys(counted)), counted)
	if cfg.LastClosedRaw != nil {
		o.offset.Store(*cfg.LastClosedRaw - int64(o.visible.Load()))
	}
	<-o.recount
	return nil
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
// again. The worker calls it after initialization; finalize after writers.
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
	o.replace(buckets, counted)
	o.current.Store(true)
	return nil
}

// replace swaps the slice sets of buckets for their new counts and adjusts the
// visible bytes. The caller holds recount.
func (o *run9OwnedBytes) replace(buckets []uint16, counted map[uint16]map[uint64]uint32) {
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

// keptForFork must be called while holding recount.
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

// Run9WaitForkRecord waits for initialization and the previous Fork's record
// before a capture seals writes. Compaction keeps committing while a capture
// waits, so waiting after CaptureUploads could capture metadata that references
// uploads it never waits for. Captures are serialized, so no other record
// starts meanwhile.
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

// stopCounting cancels an unfinished initialization and running Fork
// records, which then turn the count off, and stops the worker. It returns
// once neither reads the database.
func (o *run9OwnedBytes) stopCounting() {
	if o.stop == nil {
		return
	}
	o.cancelRecords()
	o.stopWorker()
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
// over the inode keys. A short mount's unfinished initialization is awaited
// until ctx ends, so its close keeps Portal's correction; its own budget bounds
// that wait. Then initialization, or a Fork record still scanning, is
// cancelled rather than extending finalize. It reports nothing when the count
// fails or was cancelled.
func (m *kvMeta) Run9SettleOwnedBytes(ctx context.Context) *Run9OwnedBytes {
	o := &m.run9Owned
	if o.epoch.Load() == 0 {
		return nil
	}
	select {
	case <-o.initialized:
	case <-ctx.Done():
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
