//go:build !linux

package vfs

import (
	"context"
	"fmt"
	"runtime"
)

// WarmPageCache is implemented only on Linux.
func (v *VFS) WarmPageCache(ctx context.Context, files []PageCacheFile, maxBytes uint64, concurrency int) (PageCacheWarmResult, error) {
	return PageCacheWarmResult{}, fmt.Errorf("page cache warming is not supported on %s", runtime.GOOS)
}
