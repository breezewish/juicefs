//go:build linux && run9_checkpoint_research && !nobadger

package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// Run9CheckpointOptions selects an experimental owner-local snapshot mechanism.
// This API is compiled only for research binaries; callers must block new
// buffered writes and finish their immutable data slices before entering it.
// Either drain their uploads first or retain an upload fence until publication.
// The captured callback releases the barrier after protecting slices from GC.
type Run9CheckpointOptions struct {
	Directory     string `json:"directory"`
	Strategy      string `json:"strategy"`
	ExportDelayMS int    `json:"export_delay_ms,omitempty"`
	FailPhase     string `json:"fail_phase,omitempty"`
}

// Run9CheckpointResult records actual capture and materialization work.
type Run9CheckpointResult struct {
	Counter   uint64  `json:"counter"`
	CaptureMS float64 `json:"capture_ms"`
	ExportMS  float64 `json:"export_ms"`
	Entries   int64   `json:"entries"`
	Bytes     int64   `json:"bytes"`
	Files     int     `json:"files"`
}

// Run9Checkpoint preserves one Badger view without closing the FUSE session.
// The callback must protect all allocated slices below counter before letting
// the source resume; otherwise its eventual retired-slice GC can delete child
// data. The caller holds the mount lifecycle lock until this method returns.
func (m *kvMeta) Run9Checkpoint(ctx context.Context, opts Run9CheckpointOptions, captured func(uint64) error) (out Run9CheckpointResult, resultErr error) {
	if err := ctx.Err(); err != nil {
		return out, err
	}
	c, ok := m.client.(*badgerClient)
	if !ok || c.readOnly {
		return out, fmt.Errorf("checkpoint requires writable Badger")
	}
	if !filepath.IsAbs(opts.Directory) {
		return out, fmt.Errorf("checkpoint directory must be absolute")
	}
	if _, err := os.Lstat(opts.Directory); !os.IsNotExist(err) {
		return out, fmt.Errorf("checkpoint destination must not exist: %v", err)
	}
	if opts.Strategy != "physical" && opts.Strategy != "logical" && opts.Strategy != "logical-async" && opts.Strategy != "logical-blocking" && opts.Strategy != "checkpoint" && opts.Strategy != "checkpoint-async" {
		return out, fmt.Errorf("unknown checkpoint strategy %q", opts.Strategy)
	}
	start := time.Now()
	if opts.Strategy == "physical" || opts.Strategy == "checkpoint" || opts.Strategy == "checkpoint-async" {
		out, resultErr = c.checkpointPhysical(ctx, opts)
		if resultErr != nil {
			return out, resultErr
		}
		out.CaptureMS = float64(time.Since(start).Microseconds()) / 1000
		if err := captured(out.Counter); err != nil {
			return out, err
		}
	} else {
		out, resultErr = c.checkpointLogical(ctx, opts, captured)
		if resultErr != nil {
			return out, resultErr
		}
	}
	// Child sessions do not own any running processes. Clean only the copy;
	// touching the parent's session would release its locks and open inodes.
	conf := DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	child, err := newKVMeta("badger", opts.Directory, conf)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, child.Shutdown()) }()
	sessions, err := child.ListSessions()
	if err != nil {
		return out, err
	}
	for _, session := range sessions {
		if err := child.(*kvMeta).doCleanStaleSession(session.Sid); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (c *badgerClient) checkpointLogical(ctx context.Context, opts Run9CheckpointOptions, captured func(uint64) error) (out Run9CheckpointResult, resultErr error) {
	start := time.Now()
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	if c.dbError != nil {
		return out, c.dbError
	}
	txn := c.client.NewTransaction(false)
	defer txn.Discard()
	counter, err := checkpointCounter(txn)
	if err != nil {
		return out, err
	}
	out.Counter = counter
	out.CaptureMS = float64(time.Since(start).Microseconds()) / 1000
	if opts.Strategy == "logical" || opts.Strategy == "logical-async" {
		if err := captured(counter); err != nil {
			return out, err
		}
	}
	if opts.FailPhase == "after_capture" {
		return out, fmt.Errorf("injected failure after capture")
	}
	if opts.ExportDelayMS > 0 {
		timer := time.NewTimer(time.Duration(opts.ExportDelayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-timer.C:
		}
	}
	exportStart := time.Now()
	dstOptions := c.client.Opts()
	dstOptions.Dir, dstOptions.ValueDir = opts.Directory, opts.Directory
	dst, err := badger.Open(dstOptions)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, dst.Close()) }()
	batch := dst.NewWriteBatch()
	defer batch.Cancel()
	iterator := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		item := iterator.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return out, err
		}
		entry := badger.NewEntry(key, value).WithMeta(item.UserMeta())
		entry.ExpiresAt = item.ExpiresAt()
		if err := batch.SetEntry(entry); err != nil {
			return out, err
		}
		out.Entries++
		out.Bytes += int64(len(key) + len(value))
	}
	if err := batch.Flush(); err != nil {
		return out, err
	}
	out.ExportMS = float64(time.Since(exportStart).Microseconds()) / 1000
	if opts.Strategy == "logical-blocking" {
		out.CaptureMS = float64(time.Since(start).Microseconds()) / 1000
		if err := captured(counter); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (c *badgerClient) checkpointPhysical(ctx context.Context, opts Run9CheckpointOptions) (out Run9CheckpointResult, resultErr error) {
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	if c.dbError != nil {
		return out, c.dbError
	}
	txn := c.client.NewTransaction(false)
	counter, err := checkpointCounter(txn)
	txn.Discard()
	if err != nil {
		return out, err
	}
	out.Counter = counter
	dbOptions := c.client.Opts()
	if opts.Strategy != "physical" {
		err := c.client.WithFileCheckpoint(ctx, func() error {
			if opts.FailPhase == "after_flush" {
				return fmt.Errorf("injected failure after flush")
			}
			return cloneCheckpointMetadata(ctx, dbOptions.Dir, opts, &out)
		})
		return out, err
	}
	if err := c.client.Close(); err != nil {
		c.dbError = fmt.Errorf("physical checkpoint close failed: %w", err)
		return out, c.dbError
	}
	// Reopen even when cloning or cancellation fails. This restores the same
	// metadata session's database; it does not reset allocation counters.
	defer func() {
		db, err := badger.Open(dbOptions)
		if err != nil {
			c.dbError = fmt.Errorf("physical checkpoint reopen failed: %w", err)
			resultErr = errors.Join(resultErr, c.dbError)
			return
		}
		c.client = db
	}()
	if opts.FailPhase == "after_close" {
		return out, fmt.Errorf("injected failure after close")
	}
	err = cloneCheckpointMetadata(ctx, dbOptions.Dir, opts, &out)
	return out, err
}

func cloneCheckpointMetadata(ctx context.Context, source string, opts Run9CheckpointOptions, out *Run9CheckpointResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(opts.Directory, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	start := time.Now()
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-file metadata entry %s", entry.Name())
		}
	}
	// Match run9rt's existing offline clone concurrency for a fair comparison.
	group, copyCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	sizes := make([]int64, len(entries))
	for index, entry := range entries {
		group.Go(func() error {
			if opts.FailPhase == "during_copy" && index == 1 {
				return fmt.Errorf("injected failure during copy")
			}
			size, err := checkpointCloneFile(copyCtx, filepath.Join(source, entry.Name()), filepath.Join(opts.Directory, entry.Name()))
			sizes[index] = size
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	out.Files = len(entries)
	for _, size := range sizes {
		out.Bytes += size
	}
	out.ExportMS = float64(time.Since(start).Microseconds()) / 1000
	return nil
}

func checkpointCounter(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get([]byte("CnextChunk"))
	if err != nil {
		return 0, err
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if len(value) != 8 || parseCounter(value) < 0 {
		return 0, fmt.Errorf("invalid allocation counter")
	}
	return uint64(parseCounter(value)), nil
}

func checkpointCloneFile(ctx context.Context, src, dst string) (size int64, resultErr error) {
	input, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return 0, err
	}
	output, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
	if err != nil {
		return 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, output.Close()) }()
	remaining := info.Size()
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := unix.CopyFileRange(int(input.Fd()), nil, int(output.Fd()), nil, int(min(remaining, 1<<30)), 0)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		remaining -= int64(n)
	}
	return info.Size(), nil
}
