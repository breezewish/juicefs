package chunk

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// PrefetchAdmit is asked before a prefetch fetches a block of size bytes from
// object storage. It may wait, for example for a host-wide fetch slot. It
// returns done, called once the fetch ends with whether this prefetch
// downloaded the block itself, or an error that skips the block.
type PrefetchAdmit func(ctx context.Context, size int) (done func(downloaded bool), err error)

// PrefetchBlock loads block indx of slice id into the local caches unless a
// local cache already holds it, and reports the bytes it downloaded from
// object storage: its size if this prefetch started the block's download,
// whether or not that download then failed, else 0. A block that a workload
// read loads meanwhile, or whose download a workload read started, costs no
// download of its own. Only a block not already cached is admitted by admit.
// It reads the whole block, so the block is published to the clean cache like
// any other complete remote read; its cache hits and misses are not counted.
// A block it downloads is tracked until SettlePrefetch, so a workload read of
// it marks it read.
func (store *cachedStore) PrefetchBlock(ctx context.Context, id uint64, length uint32, indx int, admit PrefetchAdmit) (int, error) {
	r := sliceForRead(id, int(length), store)
	key := r.key(indx)
	if store.holdsBlock(key) {
		return 0, nil
	}
	size := r.blockSize(indx)
	// Tracked before admission, which may wait: a workload read meanwhile
	// marks it read.
	store.prefetched.add(key, size)
	done, err := admit(ctx, size)
	if err != nil {
		store.prefetched.forget(key)
		return 0, err
	}
	downloaded := false
	defer func() { done(downloaded) }()
	if store.holdsBlock(key) {
		store.prefetched.forget(key)
		return 0, nil
	}
	page := NewOffPage(size)
	defer page.Release()
	_, err = r.ReadAt(context.WithValue(WithPrefetch(ctx), prefetchDownloadKey{}, &downloaded), page, indx*store.conf.BlockSize)
	if !downloaded || err != nil {
		// Either a workload read's download served it, or nothing was cached.
		store.prefetched.forget(key)
	}
	if !downloaded {
		return 0, err
	}
	store.prefetchBlocks.Inc()
	store.prefetchBytes.Add(float64(size))
	return size, err
}

// prefetchDownloadKey holds the *bool a prefetch read sets when it, rather
// than a read it joined, downloads its block.
type prefetchDownloadKey struct{}

// SettlePrefetch ends the tracking of prefetched blocks and counts those no
// workload read since they were fetched as unread. Later fetches are not
// tracked.
func (store *cachedStore) SettlePrefetch() (blocks int, bytes int64) {
	blocks, bytes = store.prefetched.settle()
	store.prefetchUnreadBlocks.Add(float64(blocks))
	store.prefetchUnreadBytes.Add(float64(bytes))
	store.prefetchSettled.Set(1)
	return blocks, bytes
}

// holdsBlock checks the local caches without reading the block: a prefetch of
// a block already here must not cost a read of it.
func (store *cachedStore) holdsBlock(key string) bool {
	if _, ok := store.bcache.exist(key); ok {
		return true
	}
	held := func(dir string) bool {
		_, err := os.Stat(filepath.Join(dir, cacheDir, filepath.FromSlash(key)))
		return err == nil
	}
	return store.sharedCache != nil && held(store.sharedCache.dir) || store.cleanCache != nil && held(store.cleanCache.dir)
}

// prefetchTracker holds the prefetched blocks no workload has read yet, by
// key, until it is settled. Workload reads check it on every block, so a
// mount that tracks nothing pays one atomic load.
type prefetchTracker struct {
	tracking atomic.Bool
	mu       sync.Mutex
	settled  bool
	unread   map[string]int
}

func (t *prefetchTracker) add(key string, size int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.settled {
		return
	}
	if t.unread == nil {
		t.unread = make(map[string]int)
	}
	t.unread[key] = size
	t.tracking.Store(true)
}

// read marks the block key read by a workload.
func (t *prefetchTracker) read(key string) {
	if t.tracking.Load() {
		t.forget(key)
	}
}

// forget stops tracking key.
func (t *prefetchTracker) forget(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.unread, key)
}

func (t *prefetchTracker) settle() (blocks int, bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, size := range t.unread {
		bytes += int64(size)
	}
	blocks = len(t.unread)
	t.settled, t.unread = true, nil
	t.tracking.Store(false)
	return blocks, bytes
}

func (store *cachedStore) initPrefetchMetrics() {
	store.prefetchBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "run9_prefetch_blocks", Help: "blocks a start prefetch fetched from object storage",
	})
	store.prefetchBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "run9_prefetch_bytes", Help: "bytes a start prefetch fetched from object storage",
	})
	store.prefetchUnreadBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "run9_prefetch_unread_blocks", Help: "prefetched blocks no workload read before the prefetch was settled",
	})
	store.prefetchUnreadBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "run9_prefetch_unread_bytes", Help: "bytes of prefetched blocks no workload read before the prefetch was settled",
	})
	store.prefetchSettled = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "run9_prefetch_settled", Help: "1 once the unread prefetched blocks are counted",
	})
}

func (store *cachedStore) regPrefetchMetrics(reg prometheus.Registerer) {
	reg.MustRegister(store.prefetchBlocks)
	reg.MustRegister(store.prefetchBytes)
	reg.MustRegister(store.prefetchUnreadBlocks)
	reg.MustRegister(store.prefetchUnreadBytes)
	reg.MustRegister(store.prefetchSettled)
}
