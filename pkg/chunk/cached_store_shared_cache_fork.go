package chunk

import (
	"path/filepath"
)

// sharedDiskCache is an immutable secondary cache populated outside JuiceFS.
// It deliberately exposes only reads: misses continue to dynamic clean cache
// and object storage. No cache writes, staging, eviction, or repair touch it.
type sharedDiskCache struct {
	dir      string
	checksum string
}

func newSharedDiskCache(dir, checksum string) *sharedDiskCache {
	if dir == "" {
		return nil
	}
	return &sharedDiskCache{dir: dir, checksum: checksum}
}

func (cache *sharedDiskCache) load(key string) (ReadCloser, error) {
	return openCacheFile(
		filepath.Join(cache.dir, cacheDir, filepath.FromSlash(key)),
		parseObjOrigSize(key),
		cache.checksum,
	)
}
