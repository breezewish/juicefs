package chunk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
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
	// Capture a blocked syscall before VMA's 30s process deadline. The watcher
	// reads no mutable pruning state; progress below stays on the pruning thread.
	stopDiagnostic := watchCleanCachePrune(os.Stderr, 20*time.Second)
	defer stopDiagnostic()
	// Stage records are bounded and contain no paths or object keys. Emit before
	// synchronous filesystem work so a caller killing this process retains the
	// last reached stage, even when a syscall never returns.
	started := time.Now()
	phase := "lock"
	report := func(state string) {
		_ = json.NewEncoder(os.Stderr).Encode(struct {
			Timestamp  time.Time       `json:"ts"`
			Event      string          `json:"event"`
			Phase      string          `json:"phase"`
			State      string          `json:"state"`
			DurationMS int64           `json:"duration_ms"`
			Usage      CleanCacheUsage `json:"usage"`
		}{time.Now().UTC(), "clean_cache_prune", phase, state, time.Since(started).Milliseconds(), result})
	}
	report("started")
	defer func() {
		result.OK = err == nil
		if err != nil {
			report("failed")
		} else {
			report("finished")
		}
	}()
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
	phase = "scan"
	report("started")
	progressAt := time.Now().Add(5 * time.Second)
	progressReports := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if progressReports < 4 && time.Now().After(progressAt) {
			report("progress")
			progressReports++
			progressAt = time.Now().Add(5 * time.Second)
		}
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
		phase = "prune_empty_dirs"
		report("started")
		// Reverse walk order visits children before parents. Remove only empty
		// directories; concurrent publishers can make them non-empty again.
		for i := len(directories) - 1; i >= 0; i-- {
			if ctx.Err() != nil {
				if err == nil {
					err = ctx.Err()
				}
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
	phase = "disk_usage"
	report("started")
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
	phase = "sort"
	report("started")
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].accessed.Before(blocks[j].accessed) })
	phase = "evict"
	report("started")
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

// watchCleanCachePrune emits one bounded stack snapshot even if filesystem work
// stops returning. It never logs cache paths, object keys, argv or environment.
// Stopping joins an already-fired callback, so no diagnostic outlives its pass.
func watchCleanCachePrune(w io.Writer, after time.Duration) func() {
	done := make(chan struct{})
	timer := time.AfterFunc(after, func() {
		defer close(done)
		// Emit before stopping goroutines for a stack snapshot. If collection
		// stalls, the caller still knows the watchdog ran; absolute timestamps
		// separate child startup time from time spent inside pruning.
		record := struct {
			Timestamp time.Time `json:"ts"`
			Event     string    `json:"event"`
			Truncated bool      `json:"truncated,omitempty"`
			Stack     string    `json:"stack,omitempty"`
		}{Timestamp: time.Now().UTC(), Event: "clean_cache_prune_stack_started"}
		encoder := json.NewEncoder(w)
		_ = encoder.Encode(record)
		stack := make([]byte, 64*1024)
		n := runtime.Stack(stack, true)
		record.Timestamp = time.Now().UTC()
		record.Event = "clean_cache_prune_slow_stack"
		record.Truncated = n == len(stack)
		record.Stack = string(stack[:n])
		_ = encoder.Encode(record)
	})
	return func() {
		if !timer.Stop() {
			<-done
		}
	}
}
