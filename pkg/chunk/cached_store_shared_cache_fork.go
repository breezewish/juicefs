package chunk

import (
	"path/filepath"
)

// sharedDiskCache is an immutable secondary cache populated outside JuiceFS.
// It deliberately exposes only reads: misses fall through to object storage,
// while all cache writes, staging, eviction, and repair stay in the mount's
// private cache directory.
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
