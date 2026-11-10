//go:build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// A run9 Box mount started with run9StartReadsEnv=FILE prefetches the blocks
// its previous start read, as FILE lists them, and replaces FILE with the
// reads of its own start once the start window ends. See
// vfs.TraceStartReads; run9_start_reads_config.go holds the limits.

// errStartReadsPrefetchTimeout stops a prefetch as a cancellation, not a
// deadline: the store retries a guest read that joined a cancelled fetch,
// but fails one that joined a fetch past its deadline.
var errStartReadsPrefetchTimeout = errors.New("start reads prefetch timed out")

// recordRun9StartReads writes this mount's start list once: when the start
// window ends, or when the mount ends first. nil unless run9StartReadsEnv is set.
var recordRun9StartReads func()

// startRun9StartReads traces this mount's start and, given hostDir, prefetches
// the blocks listed in path in the background within the host-wide limits
// that hostDir's lock files enforce. Call it before the mount serves requests.
// The prefetch stops with the page cache warms; the list is written while the
// metadata is open, by the window's timer or by finishRun9StartReads.
//
// Once the window has ended and the prefetch has stopped, the prefetched
// blocks no read of the mount used by then are counted as unread
// (chunk.SettlePrefetch): those fetches did not serve this start.
func startRun9StartReads(v *vfs.VFS, path string, hostDir string) {
	// The window and the prefetch each end once; the later one settles, in
	// its own goroutine, so a mount that ends settles before it exits.
	var pending atomic.Int32
	pending.Store(2)
	ended := func() {
		if pending.Add(-1) == 0 {
			blocks, bytes := v.Store.SettlePrefetch()
			logger.Infof("start reads: %d prefetched blocks (%d bytes) were not read during the start", blocks, bytes)
		}
	}
	v.TraceStartReads(startReadsConfig.Window)
	if hostDir == "" {
		ended()
		logger.Warnf("start reads: not prefetching: %s is not set, so no host-wide limit applies", run9PrefetchHostDirEnv)
	} else {
		prefetchStartReads(v, path, hostDir, ended)
	}
	var once sync.Once
	recordRun9StartReads = func() {
		once.Do(func() {
			defer ended()
			files, err := v.StartReads(meta.Background(), vfs.StartReadLimits{
				MaxFiles: startReadsConfig.MaxFiles, MaxRangeBytes: startReadsConfig.MaxRangeBytes, MaxListBytes: startReadsConfig.MaxListBytes,
			})
			if err == nil && len(files) == 0 {
				// A mount that ended before its start read anything, such as
				// one whose Box failed to start, says nothing about the next.
				err = errors.New("this mount read nothing during its start")
			}
			if err == nil {
				err = writeStartReads(path, files)
			}
			if err != nil {
				logger.Warnf("start reads: keep the previous list: %s", err)
				return
			}
			logger.Infof("start reads: recorded %d files", len(files))
		})
	}
	time.AfterFunc(startReadsConfig.Window, func() {
		if !admitForkMountRequest() {
			return // the mount is ending and records them itself
		}
		defer forkMountLifecycle.RUnlock()
		recordRun9StartReads()
	})
}

// finishRun9StartReads records the start list of a mount that ends before its
// start window does. The caller holds forkMountLifecycle with the metadata
// still open, so the window's timer cannot record concurrently.
func finishRun9StartReads() {
	if recordRun9StartReads != nil {
		recordRun9StartReads()
	}
}

// readStartReads returns the list in path, or nil if there is none yet.
func readStartReads(path string) ([]vfs.StartRead, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	maxListBytes := startReadsConfig.MaxListBytes
	data, err := io.ReadAll(io.LimitReader(f, int64(maxListBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxListBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, maxListBytes)
	}
	var list []vfs.StartRead
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	for _, file := range list {
		if file.Inode == 0 {
			return nil, fmt.Errorf("%s lists inode 0", path)
		}
		var end uint64
		for _, r := range file.Ranges {
			if r[1] == 0 || r[0] < end || r[0]+r[1] < r[0] {
				return nil, fmt.Errorf("%s has invalid range %v of inode %d", path, r, file.Inode)
			}
			end = r[0] + r[1]
		}
	}
	if len(list) > startReadsConfig.MaxFiles {
		return nil, fmt.Errorf("%s lists %d files, past the limit", path, len(list))
	}
	return list, nil
}

// prefetchStartReads reads the list in path and prefetches its blocks, both in
// the background: path is on shared metadata, which may be slow to read on a
// host that just started. It calls stopped once the prefetch has stopped or
// will not run; a stop by stopForkPageCacheWarms waits for it.
func prefetchStartReads(v *vfs.VFS, path string, hostDir string, stopped func()) {
	ctx, cancel := context.WithCancelCause(context.Background())
	done, err := beginForkPageCacheWarm(func() { cancel(nil) })
	if err != nil {
		cancel(nil)
		stopped()
		logger.Warnf("start reads: not prefetching: %s", err)
		return
	}
	timeout := time.AfterFunc(startReadsConfig.Timeout, func() { cancel(errStartReadsPrefetchTimeout) })
	go func() {
		defer cancel(nil)
		defer timeout.Stop()
		defer done()
		// Before done: stopForkPageCacheWarms then returns only after it.
		defer stopped()
		start := time.Now()
		list, err := readStartReads(path)
		if err != nil {
			logger.Warnf("start reads: not prefetching: %s", err)
			return
		} else if list == nil {
			return
		}
		host, err := openHostPrefetchLimiter(hostDir, startReadsConfig.HostConcurrency, startReadsConfig.HostBytesPerSecond)
		if err != nil {
			logger.Warnf("start reads: not prefetching: open host limits: %s", err)
			return
		}
		defer host.close()
		result := v.PrefetchStartReads(ctx, list, vfs.StartReadsPrefetchLimits{
			Concurrency: startReadsConfig.MountConcurrency,
			MaxBytes:    startReadsConfig.MountMaxBytes,
			Admit:       host.admit,
		})
		logger.Infof("start reads: prefetched %d blocks (%d bytes) for %d files in %s (skipped %d files, %d blocks failed, stopped %q)",
			result.Blocks, result.Bytes, result.Files, time.Since(start).Round(time.Millisecond), result.Skipped, result.Failed, result.Stopped)
	}()
}

// writeStartReads replaces path atomically, so a reader sees the previous
// list or this one, never part of one.
func writeStartReads(path string, files []vfs.StartRead) error {
	data, err := json.Marshal(files)
	if err != nil {
		return err
	}
	if len(data) > startReadsConfig.MaxListBytes {
		return fmt.Errorf("list of %d files encodes to %d bytes", len(files), len(data))
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(data)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	return err
}
