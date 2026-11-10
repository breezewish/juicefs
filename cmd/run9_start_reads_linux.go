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
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// A run9 Box mount started with JFS_RUN9_START_READS=FILE prefetches the
// blocks its previous start read, as FILE lists them, and replaces FILE with
// the reads of its own start once startReadsWindow ends. See
// vfs.TraceStartReads. An environment variable rather than a flag lets run9rt
// and juicefs.run9 roll out in either order: an older juicefs ignores it, and
// an older run9rt never sets it.
const run9StartReadsEnv = "JFS_RUN9_START_READS"

const (
	// startReadsWindow covers a Box start: boot, the first commands and the
	// programs they start. Reads after it are the workload's, not the start's.
	startReadsWindow = 30 * time.Second
	// startReadsPrefetchConcurrency bounds the blocks fetched at once, and so
	// the daemon's memory: each fetch holds two buffers of one block (4 MiB by
	// default), the caller's and the download's.
	startReadsPrefetchConcurrency = 8
	startReadsPrefetchTimeout     = 2 * time.Minute
	startReadsMaxFiles            = 16384
	startReadsMaxBytes            = 2 << 30
	// startReadsMaxListBytes bounds the list file a mount writes and reads.
	startReadsMaxListBytes = 16 << 20
)

// errStartReadsPrefetchTimeout stops a prefetch as a cancellation, not a
// deadline: the store retries a guest read that joined a cancelled fetch,
// but fails one that joined a fetch past its deadline.
var errStartReadsPrefetchTimeout = errors.New("start reads prefetch timed out")

// recordRun9StartReads writes this mount's start list once: when the start
// window ends, or when the mount ends first. nil unless run9StartReadsEnv is set.
var recordRun9StartReads func()

// startRun9StartReads traces this mount's start and prefetches the blocks
// listed in path in the background. Call it before the mount serves requests.
// The prefetch stops with the page cache warms; the list is written while the
// metadata is open, by the window's timer or by finishRun9StartReads.
func startRun9StartReads(v *vfs.VFS, path string) {
	v.TraceStartReads(startReadsWindow)
	prefetchStartReads(v, path)
	var once sync.Once
	recordRun9StartReads = func() {
		once.Do(func() {
			files, err := v.StartReads(meta.Background(), vfs.StartReadLimits{
				MaxFiles: startReadsMaxFiles, MaxBytes: startReadsMaxBytes, MaxListBytes: startReadsMaxListBytes,
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
	time.AfterFunc(startReadsWindow, func() {
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
	data, err := io.ReadAll(io.LimitReader(f, startReadsMaxListBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > startReadsMaxListBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, startReadsMaxListBytes)
	}
	var list []vfs.StartRead
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	var bytes uint64
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
			bytes += r[1]
		}
	}
	if len(list) > startReadsMaxFiles || bytes > startReadsMaxBytes {
		return nil, fmt.Errorf("%s lists %d files and %d bytes, past the limits", path, len(list), bytes)
	}
	return list, nil
}

// prefetchStartReads reads the list in path and prefetches its blocks, both in
// the background: path is on shared metadata, which may be slow to read on a
// host that just started.
func prefetchStartReads(v *vfs.VFS, path string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	done, err := beginForkPageCacheWarm(func() { cancel(nil) })
	if err != nil {
		cancel(nil)
		logger.Warnf("start reads: not prefetching: %s", err)
		return
	}
	timeout := time.AfterFunc(startReadsPrefetchTimeout, func() { cancel(errStartReadsPrefetchTimeout) })
	go func() {
		defer cancel(nil)
		defer timeout.Stop()
		defer done()
		start := time.Now()
		list, err := readStartReads(path)
		if err != nil {
			logger.Warnf("start reads: not prefetching: %s", err)
			return
		} else if list == nil {
			return
		}
		result := v.PrefetchStartReads(ctx, list, startReadsPrefetchConcurrency)
		logger.Infof("start reads: prefetched %d blocks for %d files in %s (skipped %d files, %d blocks failed, stopped %q)",
			result.Blocks, result.Files, time.Since(start).Round(time.Millisecond), result.Skipped, result.Failed, result.Stopped)
	}()
}

// writeStartReads replaces path atomically, so a reader sees the previous
// list or this one, never part of one.
func writeStartReads(path string, files []vfs.StartRead) error {
	data, err := json.Marshal(files)
	if err != nil {
		return err
	}
	if len(data) > startReadsMaxListBytes {
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
