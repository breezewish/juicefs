package chunk

import (
	"context"
	"os"
	"path/filepath"
)

// PrefetchBlock loads block indx of slice id into the local caches unless a
// local cache already holds it, and reports whether it fetched the block from
// object storage. It reads the whole block, so the block is published to the
// clean cache like any other complete remote read; its cache hits and misses
// are not counted.
func (store *cachedStore) PrefetchBlock(ctx context.Context, id uint64, length uint32, indx int) (bool, error) {
	r := sliceForRead(id, int(length), store)
	if store.holdsBlock(r.key(indx)) {
		return false, nil
	}
	page := NewOffPage(r.blockSize(indx))
	defer page.Release()
	if _, err := r.ReadAt(WithPrefetch(ctx), page, indx*store.conf.BlockSize); err != nil {
		return false, err
	}
	return true, nil
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
