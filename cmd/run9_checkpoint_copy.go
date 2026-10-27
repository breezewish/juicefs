package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// copyRun9GCCheckpoint gives a maintenance reader ownership of its mapped files.
// Published checkpoints are immutable but GC can remove them concurrently. Badger
// mmap on shared FUSE can turn a lost page read into SIGBUS, bypassing Go error
// handling. Ordinary reads into a private local directory report I/O failures
// before Badger opens anything. This does not add locks to foreground operations.
// TMPDIR must be host-local. The caller removes the copy after closing Badger.
func copyRun9GCCheckpoint(ctx context.Context, source string) (directory string, resultErr error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	info, err := os.Lstat(source)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("checkpoint source is not a directory: %s", source)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return "", err
	}
	directory, err = os.MkdirTemp("", "run9-gc-checkpoint-")
	if err != nil {
		return "", err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, os.RemoveAll(directory))
		}
	}()
	for _, entry := range entries {
		path := filepath.Join(source, entry.Name())
		if !entry.Type().IsRegular() {
			return directory, fmt.Errorf("checkpoint entry is not a regular file: %s", path)
		}
		if err := copyRun9GCCheckpointFile(ctx, path, filepath.Join(directory, entry.Name())); err != nil {
			return directory, err
		}
	}
	return directory, nil
}

func copyRun9GCCheckpointFile(ctx context.Context, source, destination string) (resultErr error) {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("checkpoint entry is not a regular file: %s", source)
	}
	dst, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, dst.Close()) }()
	// Bounded reads observe cancellation without background copy goroutines.
	for remaining := info.Size(); remaining > 0; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := io.CopyN(dst, src, min(remaining, 1<<20))
		if err != nil {
			return fmt.Errorf("copy checkpoint %s at offset %d of %d: %w", source, info.Size()-remaining+n, info.Size(), err)
		}
		remaining -= n
	}
	return nil
}
