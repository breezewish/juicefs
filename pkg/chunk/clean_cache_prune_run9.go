package chunk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gofrs/flock"
)

// CleanCacheUsage reports one host-wide clean cache pruning pass. It never
// counts or removes private staging or the separately installed prewarm cache.
type CleanCacheUsage struct {
	OK            bool   `json:"ok"`
	Busy          bool   `json:"busy,omitempty"`
	Bytes         uint64 `json:"bytes"`
	Blocks        uint64 `json:"blocks"`
	RemovedBytes  uint64 `json:"removed_bytes"`
	RemovedBlocks uint64 `json:"removed_blocks"`
}

// PruneCleanCache evicts cold remote-backed blocks across all volume UUIDs in
// root. Limits are host watermarks, not admission reservations: concurrent fills
// can temporarily exceed them. The only cross-process lock is between cleaners.
func PruneCleanCache(ctx context.Context, root string, maxBytes, maxItems uint64) (result CleanCacheUsage, err error) {
	result.OK = true
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return result, fmt.Errorf("clean cache root is not a directory: %s", root)
	}
	lock := flock.New(filepath.Join(root, ".prune.lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return result, err
	}
	if !locked {
		result.Busy = true
		return result, nil
	}
	defer func() {
		if e := lock.Close(); err == nil {
			err = e
		}
	}()
	type entry struct {
		path     string
		size     uint64
		accessed time.Time
	}
	var blocks []entry
	var directories []string
	now := time.Now()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if path != root {
				directories = append(directories, path)
			}
			return nil
		}
		if path == filepath.Join(root, ".prune.lock") {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unexpected clean cache entry: %s", path)
		}
		fi, e := d.Info()
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if strings.HasPrefix(d.Name(), ".fill-") && strings.HasSuffix(d.Name(), ".tmp") {
			// A live publication losing its old temporary name simply fails cache
			// admission. Its source data is already safe in object storage.
			if now.Sub(fi.ModTime()) > time.Hour {
				if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
					return e
				}
			}
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 6 || parts[1] != cacheDir {
			return fmt.Errorf("unexpected clean cache layout: %s", path)
		}
		key := strings.Join(parts[2:], "/")
		if _, normal := ParseObjectBlockKey(key, false); !normal {
			if _, hashed := ParseObjectBlockKey(key, true); !hashed {
				return fmt.Errorf("invalid clean cache key: %s", key)
			}
		}
		blocks = append(blocks, entry{path, uint64(fi.Size()), fi.ModTime()})
		result.Bytes += uint64(fi.Size())
		result.Blocks++
		return nil
	})
	if err != nil {
		return result, err
	}
	defer func() {
		// Reverse walk order visits children before parents. Remove only empty
		// directories; concurrent publishers can make them non-empty again.
		for i := len(directories) - 1; i >= 0; i-- {
			if ctx.Err() != nil {
				break
			}
			if e := os.Remove(directories[i]); e != nil && !os.IsNotExist(e) && !errors.Is(e, syscall.ENOTEMPTY) && !errors.Is(e, syscall.EEXIST) {
				if err == nil {
					err = e
				}
			}
		}
	}()
	// Reserve 10% of both physical bytes and inodes for non-cache work, including
	// writeback. Pinned prewarm blocks consume this space but are never removed.
	total, free, files, ffree := diskUsageFn(root)
	byteTarget, itemTarget := maxBytes, maxItems
	if free < total/10 {
		byteTarget = min(byteTarget, result.Bytes-min(result.Bytes, total/10-free))
	}
	if ffree < files/10 {
		itemTarget = min(itemTarget, result.Blocks-min(result.Blocks, files/10-ffree))
	}
	if result.Bytes <= byteTarget && result.Blocks <= itemTarget {
		return result, nil
	}
	// Leave headroom so the next pass does not repeatedly evict only one block.
	byteTarget = byteTarget * 9 / 10
	itemTarget = itemTarget * 9 / 10
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].accessed.Before(blocks[j].accessed) })
	for _, block := range blocks {
		if result.Bytes <= byteTarget && result.Blocks <= itemTarget {
			break
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if e := os.Remove(block.path); e != nil && !os.IsNotExist(e) {
			return result, e
		}
		result.Bytes -= block.size
		result.Blocks--
		result.RemovedBytes += block.size
		result.RemovedBlocks++
	}
	return result, nil
}
