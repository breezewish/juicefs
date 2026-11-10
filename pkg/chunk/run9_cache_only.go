package chunk

import (
	"context"
	"errors"
)

type cacheOnlyKey struct{}

// ErrNotCached reports a cache-only read of a block that no local cache holds.
var ErrNotCached = errors.New("block is not in a local cache")

// WithCacheOnly marks reads that may use the private, recorded prewarm and
// clean caches but must never fetch from object storage. Such a read of an
// uncached block fails with ErrNotCached instead.
func WithCacheOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheOnlyKey{}, true)
}

func isCacheOnly(ctx context.Context) bool {
	return ctx.Value(cacheOnlyKey{}) != nil
}

type prefetchKey struct{}

// WithPrefetch marks reads that load blocks ahead of a workload. They may
// fetch from object storage, which counts its requests as usual, but their
// cache hits and misses are not the workload's and are not counted.
func WithPrefetch(ctx context.Context) context.Context {
	return context.WithValue(ctx, prefetchKey{}, true)
}

// isWorkloadRead reports whether a read's cache hits and misses are counted:
// those of cache-only and prefetch reads belong to no workload.
func isWorkloadRead(ctx context.Context) bool {
	return !isCacheOnly(ctx) && ctx.Value(prefetchKey{}) == nil
}
