//go:build linux && !nobadger

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

// ErrRun9MetadataUnavailable means the source database cannot serve more I/O.
// The mount owner must terminate instead of exposing a half-closed database.
var ErrRun9MetadataUnavailable = errors.New("source metadata unavailable")

// Run9MetadataCapture describes a closed-file metadata copy. AllocationEnd is
// the exclusive reservation limit, including IDs still cached by the allocator.
type Run9MetadataCapture struct {
	AllocationEnd uint64 `json:"allocation_end"`
	CloseMS       int64  `json:"close_ms"`
	CloneMS       int64  `json:"clone_ms"`
	OpenMS        int64  `json:"open_ms"`
}

// Run9CaptureMetadata closes only Badger, clones its complete stable file set,
// and reopens the same options. The caller must seal VFS writes first and keep
// the mount alive. No session, allocator, or chunk-store state is recreated.
func (m *kvMeta) Run9CaptureMetadata(ctx context.Context, directory string) (out Run9MetadataCapture, resultErr error) {
	c, ok := m.client.(*badgerClient)
	if !ok || c.readOnly {
		return out, fmt.Errorf("metadata capture requires writable Badger")
	}
	if !filepath.IsAbs(directory) {
		return out, fmt.Errorf("metadata capture requires an absolute destination")
	}
	if _, err := os.Lstat(directory); !os.IsNotExist(err) {
		return out, fmt.Errorf("metadata capture destination must not exist: %s (%v)", directory, err)
	}
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if c.dbError != nil {
		return out, c.dbError
	}
	if err := c.client.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("CnextChunk"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(value) != 8 || parseCounter(value) < 0 {
			return fmt.Errorf("invalid slice allocation counter")
		}
		out.AllocationEnd = uint64(parseCounter(value))
		return nil
	}); err != nil {
		return out, err
	}
	options := c.client.Opts()
	if options.Dir != options.ValueDir {
		return out, fmt.Errorf("metadata capture requires a single Badger directory")
	}
	start := time.Now()
	err := c.client.Close()
	out.CloseMS = time.Since(start).Milliseconds()
	if err != nil {
		c.dbError = fmt.Errorf("%w: metadata capture close failed: %w", ErrRun9MetadataUnavailable, err)
		return out, c.dbError
	}
	// Reopening is source recovery, so it must run even after cancellation or
	// a failed copy. Keep the access lock until the replacement is installed.
	defer func() {
		start := time.Now()
		db, err := badger.Open(options)
		out.OpenMS = time.Since(start).Milliseconds()
		if err != nil {
			c.dbError = fmt.Errorf("%w: metadata capture reopen failed: %w", ErrRun9MetadataUnavailable, err)
			resultErr = errors.Join(resultErr, c.dbError)
			return
		}
		c.client = db
	}()
	start = time.Now()
	err = cloneRun9Metadata(ctx, options.Dir, directory)
	out.CloneMS = time.Since(start).Milliseconds()
	return out, err
}

func cloneRun9Metadata(ctx context.Context, source, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return err
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for _, entry := range entries {
		group.Go(func() (resultErr error) {
			if !entry.Type().IsRegular() {
				return fmt.Errorf("non-file metadata entry: %s", entry.Name())
			}
			src, err := os.Open(filepath.Join(source, entry.Name()))
			if err != nil {
				return err
			}
			defer src.Close()
			info, err := src.Stat()
			if err != nil {
				return err
			}
			dst, err := os.OpenFile(filepath.Join(destination, entry.Name()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			defer func() { resultErr = errors.Join(resultErr, dst.Close()) }()
			for remaining := info.Size(); remaining > 0; {
				if err := ctx.Err(); err != nil {
					return err
				}
				n, err := unix.CopyFileRange(int(src.Fd()), nil, int(dst.Fd()), nil, int(min(remaining, 1<<30)), 0)
				if err != nil {
					return err
				}
				if n == 0 {
					return io.ErrUnexpectedEOF
				}
				remaining -= int64(n)
			}
			return dst.Sync()
		})
	}
	return group.Wait()
}
