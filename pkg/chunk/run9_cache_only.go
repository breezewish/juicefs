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
