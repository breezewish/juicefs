package meta

import (
	"math"
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
// has one sum. A committed transaction marks the ranges it wrote; a background
// worker recounts marked ranges and the total is the sum of all ranges.
const run9OwnedBuckets = 1 << 16

// More marked ranges than this are recounted by one pass over all inode keys.
const run9OwnedFullPass = run9OwnedBuckets / 8

// Run9OwnedRecountInterval is how long the worker rests after each recount.
// The count is for people watching a disk, who do not need it by the second.
var Run9OwnedRecountInterval = 30 * time.Second

// run9OwnedLateChanges are changes that were committed before a Fork raised
// the bound to high but were not counted yet. They belong to what is kept for
// that Fork. sums is what each range had counted for [low, high) by then.
type run9OwnedLateChanges struct {
	generation uint64 // The one the Fork ended.
	low, high  uint64
	buckets    []uint16
	sums       []uint64
}

type run9OwnedBytes struct {
	// Zero: not a Run9 writable epoch, nothing is counted.
	epoch atomic.Uint64
	// Only slices at or above the bound are counted. Slices below it existed at
	// a Fork of this filesystem and stay kept for that Fork whatever happens here.
	forkBound atomic.Uint64

	marked [run9OwnedBuckets / 64]atomic.Uint64

	// mu orders applying a recount with a change of the bound.
	mu         sync.Mutex
	generation uint64 // Advances with the bound.
	sums       []uint64
	late       []*run9OwnedLateChanges
	visible    atomic.Uint64
	// keptForForks is what was counted each time a Fork raised the bound: own
	// data below the bound, kept whether or not this filesystem still reads it.
	keptForForks atomic.Uint64
	// current: every change committed before the last recount is counted.
	current atomic.Bool

	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
	// forked wakes the worker to count the late changes of a Fork at once,
	// before this filesystem drops data that the Fork received.
	forked chan struct{}
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
			Name: "run9_owned_visible_bytes",
			Help: "Bytes of own slices at or above the fork bound that a read still reaches.",
		}, func() float64 { return float64(o.visible.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "run9_owned_kept_for_forks_bytes",
			Help: "Bytes of own slices below the fork bound, as counted when each Fork was taken.",
		}, func() float64 { return float64(o.keptForForks.Load()) }),
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

// Run9TrackOwnedBytes starts counting for a mounted writable epoch whose next
// slice ID is allocationStart. Slices below forkBound existed at a Fork; they
// were counted as keptForForks bytes when those Forks were taken.
func (m *kvMeta) Run9TrackOwnedBytes(epoch uint64, forkBound uint64, keptForForks uint64, allocationStart uint64) {
	client, ok := m.client.(*badgerClient)
	if !ok || epoch == 0 {
		return
	}
	o := &m.run9Owned
	o.sums = make([]uint64, run9OwnedBuckets)
	o.forkBound.Store(forkBound)
	o.keptForForks.Store(keptForForks)
	o.epoch.Store(epoch)
	// Nothing to find when no own slice at or above the bound exists yet.
	if allocationStart > max(forkBound, epoch<<32) {
		o.markAll()
	} else {
		o.current.Store(true)
	}
	o.stop, o.stopped, o.forked = make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	client.run9Owned.Store(o)
	go m.run9OwnedWorker()
}

// The worker recounts, rests for the interval, and recounts again. A Fork in
// between only wakes it for that Fork's late changes.
func (m *kvMeta) run9OwnedWorker() {
	o := &m.run9Owned
	defer close(o.stopped)
	next := time.Now().Add(Run9OwnedRecountInterval)
	for {
		select {
		case <-o.stop:
			return
		case <-o.forked:
			m.run9OwnedCountLateChanges()
		case <-time.After(time.Until(next)):
			m.run9OwnedRecount()
			next = time.Now().Add(Run9OwnedRecountInterval)
		}
	}
}

// run9OwnedCount counts the marked ranges, or everything when most are marked.
func (m *kvMeta) run9OwnedCount(buckets []uint16, low uint64, high uint64) (map[uint16]uint64, error) {
	counted := make(map[uint16]uint64, len(buckets))
	if len(buckets) > run9OwnedFullPass {
		return counted, m.run9CountOwned([]byte{'A'}, low, high, counted)
	}
	for _, b := range buckets {
		if err := m.run9CountOwned([]byte{'A', byte(b >> 8), byte(b)}, low, high, counted); err != nil {
			return nil, err
		}
	}
	return counted, nil
}

// run9OwnedRecount brings the sums of the marked ranges up to date.
func (m *kvMeta) run9OwnedRecount() {
	m.run9OwnedCountLateChanges()
	o := &m.run9Owned
	o.mu.Lock()
	generation := o.generation
	o.mu.Unlock()
	bound := o.forkBound.Load()
	buckets := o.takeMarked()
	if len(buckets) == 0 {
		o.current.Store(true)
		return
	}
	counted, err := m.run9OwnedCount(buckets, bound, math.MaxUint64)
	if err != nil {
		// The database is closed for a capture or for good. Count again later.
		o.changed(buckets)
		o.current.Store(false)
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if generation != o.generation {
		// A Fork moved the bound meanwhile and reset the sums. These ranges
		// held changes from before it: they join that Fork's late changes.
		for _, late := range o.late {
			if late.generation == generation {
				late.buckets = append(late.buckets, buckets...)
			}
		}
		o.wakeForLateChanges()
		// Changes after the Fork may be among them too: count these ranges again.
		o.changed(buckets)
		o.current.Store(false)
		return
	}
	visible := o.visible.Load()
	for _, b := range buckets {
		visible += counted[b] - o.sums[b]
		o.sums[b] = counted[b]
	}
	o.visible.Store(visible)
	o.current.Store(true)
}

func (o *run9OwnedBytes) wakeForLateChanges() {
	select {
	case o.forked <- struct{}{}:
	default:
	}
}

// run9OwnedCountLateChanges adds to what is kept for Forks the changes that
// were committed before a Fork but counted only now. What this filesystem has
// dropped of them since is missed; the worker is woken at once to keep that
// moment short.
func (m *kvMeta) run9OwnedCountLateChanges() {
	o := &m.run9Owned
	for {
		o.mu.Lock()
		if len(o.late) == 0 {
			o.mu.Unlock()
			return
		}
		late := o.late[0]
		buckets := late.buckets
		late.buckets = nil
		o.mu.Unlock()
		counted, err := m.run9OwnedCount(buckets, late.low, late.high)
		o.mu.Lock()
		if err != nil {
			late.buckets = append(late.buckets, buckets...)
			o.mu.Unlock()
			return
		}
		kept := o.keptForForks.Load()
		for _, b := range buckets {
			kept += counted[b] - late.sums[b]
			late.sums[b] = counted[b]
		}
		o.keptForForks.Store(kept)
		if len(late.buckets) == 0 {
			o.late = o.late[1:]
		}
		o.mu.Unlock()
	}
}

// run9CountOwned adds, per key range, the bytes of the own slices in
// [low, high) that a read reaches, for the inode keys under prefix. It applies the
// rule of ScanRun9CheckpointSlices: only chunks of linked regular files count,
// through the overlay a read uses. The chunk keys of an inode sort before its
// attribute key. A slice read through two files counts for each of them.
func (m *kvMeta) run9CountOwned(prefix []byte, low uint64, high uint64, counted map[uint16]uint64) error {
	epoch := m.run9Owned.epoch.Load()
	owned := func(id uint64) bool { return id>>32 == epoch && id >= low && id < high }
	var inode Ino
	var chunks [][]byte
	var seen []uint64
	return m.client.scan(prefix, func(k, v []byte) bool {
		if len(k) < 10 {
			return true
		}
		if current := m.decodeInode(k[1:9]); current != inode {
			inode, chunks = current, chunks[:0]
		}
		switch k[9] {
		case 'C':
			if len(k) == 14 && len(v)%sliceBytes == 0 {
				chunks = append(chunks, v)
			}
		case 'I':
			if len(chunks) == 0 || (len(v) != 64 && len(v) != 72 && len(v) != 80) {
				return true
			}
			var attr Attr
			attr.Unmarshal(v)
			if attr.Typ != TypeFile || attr.Nlink == 0 {
				return true
			}
			var bytes uint64
			for _, chunk := range chunks {
				if len(chunk) == sliceBytes { // The usual chunk: one record, read as it is.
					var s slice
					s.read(chunk)
					if s.len > 0 && owned(s.id) {
						bytes += uint64(s.size)
					}
					continue
				}
				// An overwrite can leave one slice readable in several pieces.
				seen = seen[:0]
			pieces:
				for _, s := range buildSlice(readSliceBuf(chunk)) {
					if !owned(s.Id) {
						continue
					}
					for _, id := range seen {
						if id == s.Id {
							continue pieces
						}
					}
					seen = append(seen, s.Id)
					bytes += uint64(s.Size)
				}
			}
			counted[run9OwnedBucket(k)] += bytes
		}
		return true
	})
}

// Run9RecordFork records a Fork captured while mounted, whose metadata ends
// at allocationEnd, and returns its bound: the first slice ID that the Fork
// cannot have received. The persisted allocation end includes IDs this mount
// has reserved but not used yet; those are still to be written here.
//
// Everything counted so far is below the new bound: it stays kept for the
// Fork, and the count starts again from zero. Writes are sealed during a
// capture, so no slice at or above the bound is referenced yet. The capture
// itself reads no metadata: it must not wait for a count.
func (m *baseMeta) Run9RecordFork(allocationEnd uint64) uint64 {
	m.freeMu.Lock()
	bound := allocationEnd
	if m.freeSlices.next < m.freeSlices.maxid {
		bound = m.freeSlices.next
	}
	m.freeMu.Unlock()
	o := &m.run9Owned
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.epoch.Load() == 0 || bound <= o.forkBound.Load() {
		return bound
	}
	// Changes committed before the capture may not be counted yet. Their
	// ranges are counted against the old bound right after it.
	o.late = append(o.late, &run9OwnedLateChanges{generation: o.generation, low: o.forkBound.Load(), high: bound, buckets: o.takeMarked(), sums: o.sums})
	o.generation++
	o.forkBound.Store(bound)
	o.keptForForks.Add(o.visible.Swap(0))
	o.sums = make([]uint64, run9OwnedBuckets)
	o.wakeForLateChanges()
	return bound
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
	// VisibleBytes are the own slices at or above ForkBound that are read.
	VisibleBytes uint64 `json:"visible_bytes"`
	ForkBound    uint64 `json:"fork_bound"`
	// KeptForForksBytes are the own slices below ForkBound, as counted when
	// each Fork was taken.
	KeptForForksBytes uint64 `json:"kept_for_forks_bytes"`
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
	m.run9OwnedRecount()
	if !o.current.Load() {
		return nil
	}
	return &Run9OwnedBytes{VisibleBytes: o.visible.Load(), ForkBound: o.forkBound.Load(), KeptForForksBytes: o.keptForForks.Load()}
}
