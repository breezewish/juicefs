package chunk

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// cleanDiskCache stores only remote-backed immutable blocks. It has no private
// key index, staging scanner, uploader, or eviction worker: every process sees
// publications immediately, and a host-level maintenance command owns eviction.
type cleanDiskCache struct {
	dir       string
	mode      os.FileMode
	freeRatio float32
	metrics   *cacheManagerMetrics
}

func (cache *cleanDiskCache) load(key string) (ReadCloser, error) {
	path := filepath.Join(cache.dir, cacheDir, filepath.FromSlash(key))
	r, err := openCacheFile(path, parseObjOrigSize(key), CsExtend)
	if err != nil {
		return nil, err
	}
	// Unlike legacy/prewarm files, dynamically published clean blocks always
	// include checksums. A truncated checksum trailer must not disable checking.
	if r.csLevel == CsNone {
		_ = r.Close()
		return nil, fmt.Errorf("clean cache block %s has no checksum", key)
	}
	// Coarse recency is shared through the inode, without per-hit bookkeeping or
	// reliance on the host's atime mount option. Content remains immutable.
	if fi, statErr := r.Stat(); statErr == nil && time.Since(fi.ModTime()) > time.Hour {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
	}
	return r, nil
}

func (cache *cleanDiskCache) cache(key string, p *Page, dropCache bool) {
	start := time.Now()
	if err := cache.publish(key, p.Data, dropCache); err != nil {
		cache.metrics.cacheDrops.Add(1)
		logger.Warnf("Skip clean cache block %s: %s", key, err)
		return
	}
	cache.metrics.cacheWriteHist.Observe(time.Since(start).Seconds())
}

func (cache *cleanDiskCache) publish(key string, data []byte, dropCache bool) error {
	if len(data) != parseObjOrigSize(key) {
		return fmt.Errorf("clean cache block %s has size %d", key, len(data))
	}
	if err := os.MkdirAll(cache.dir, 0700); err != nil {
		return err
	}
	total, free, files, ffree := diskUsageFn(cache.dir)
	if total > 0 && float64(free)/float64(total) < float64(cache.freeRatio) || files > 0 && float64(ffree)/float64(files) < float64(cache.freeRatio) {
		return errStageFull
	}
	path := filepath.Join(cache.dir, cacheDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".fill-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(cache.mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		_, err = f.Write(checksum(data))
	}
	if dropCache {
		dropOSCache(f)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// Link publishes without replacing another writer's complete block. The
	// temporary link is removed on return. No fsync: this is a disposable copy.
	if err = os.Link(f.Name(), path); os.IsExist(err) {
		r, readErr := cache.load(key)
		if readErr != nil {
			return readErr
		}
		defer r.Close()
		existing := make([]byte, len(data))
		if _, readErr = r.ReadAt(existing, 0); readErr != nil {
			return readErr
		}
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("immutable clean cache block conflict: %s", key)
		}
		return nil
	}
	if err == nil {
		cache.metrics.cacheWrites.Add(1)
		cache.metrics.cacheWriteBytes.Add(float64(len(data)))
	}
	return err
}

// cacheBlock accepts only complete blocks read from object storage or confirmed
// by a successful PUT. Private writeback data must never enter this path early.
func (store *cachedStore) cacheBlock(key string, p *Page, force, dropCache bool) {
	if store.cleanCache != nil {
		store.cleanCache.cache(key, p, dropCache)
	} else {
		store.bcache.cache(key, p, force, dropCache)
	}
}

func (store *cachedStore) uploadedBlock(key string, size int) {
	if store.cleanCache != nil {
		// The PUT is already complete. Even if clean admission failed, the private
		// recovery link is no longer needed; rawstaging removal stays with caller.
		if manager, ok := store.bcache.(*cacheManager); ok {
			cache := manager.getStore(key)
			if cache == nil {
				return
			}
			cache.Lock()
			delete(cache.pages, key)
			if item := cache.keys.remove(cache.getCacheKey(key), true); item != nil && item.size > 0 {
				cache.used -= int64(item.size + 4096)
			}
			cache.Unlock()
			// Do not let an incomplete scan or the pending (negative) index entry
			// suppress unlinking this known-uploaded recovery copy.
			if err := cache.removeFile(cache.cachePath(key)); err != nil && !os.IsNotExist(err) {
				logger.Warnf("Remove uploaded private cache block %s: %s", key, err)
			}
		} else {
			store.bcache.remove(key, false)
		}
	} else {
		store.bcache.uploaded(key, size)
	}
}
