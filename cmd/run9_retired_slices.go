package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/urfave/cli/v2"
)

// Published with a CLOSED metadata clone while run9rt holds the source lock.
// End and storage identity come from this clone, never the later writable source.
type run9RetiredSliceSnapshot struct {
	Version    int    `json:"version"`
	Format     string `json:"format"`
	OwnedEpoch uint64 `json:"owned_epoch"`
	Start      uint64 `json:"start"`
}

type run9RetiredSliceGCResult struct {
	OK                  bool   `json:"ok"`
	Busy                bool   `json:"busy,omitempty"`
	CompletedSnapshots  int    `json:"completed_snapshots"`
	PendingSnapshots    int    `json:"pending_snapshots"`
	FailedSnapshots     int    `json:"failed_snapshots"`
	DeferredSnapshots   int    `json:"deferred_snapshots"`
	ScannedPages        int    `json:"scanned_pages"`
	DeletedObjects      uint64 `json:"deleted_objects"`
	DeletedLogicalBytes uint64 `json:"deleted_logical_bytes"`
	Error               string `json:"error,omitempty"`
}

func cmdRun9GCRetiredSlices() *cli.Command {
	return &cli.Command{Name: "gc-retired-slices", Hidden: true, Usage: "reclaim lifecycle-private slices from finalized metadata snapshots",
		Flags: []cli.Flag{&cli.StringFlag{Name: "queue-dir", Required: true}},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("gc-retired-slices takes no positional arguments")
			}
			ctx, cancel := context.WithTimeout(c.Context, 2*time.Minute)
			defer cancel()
			out, err := run9GCRetiredSlices(ctx, c.String("queue-dir"))
			if err != nil {
				out.OK = false
				if out.Error == "" {
					out.Error = err.Error()
				}
			}
			return json.NewEncoder(c.App.Writer).Encode(out)
		},
	}
}

func run9GCRetiredSlices(ctx context.Context, dir string) (run9RetiredSliceGCResult, error) {
	out := run9RetiredSliceGCResult{OK: true}
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	if !info.IsDir() {
		return out, fmt.Errorf("retired slice queue is not a directory")
	}
	lock := flock.New(filepath.Join(dir, ".gc.lock"))
	defer lock.Close()
	locked, err := lock.TryLock()
	if err != nil {
		return out, err
	}
	if !locked {
		out.Busy = true
		return out, nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return out, err
	}
	defer f.Close()
	for out.DeletedObjects < 65536 {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		entries, err := f.ReadDir(32)
		if err != nil && !errors.Is(err, io.EOF) {
			return out, err
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			path := filepath.Join(dir, entry.Name())
			if strings.HasPrefix(entry.Name(), ".done-") {
				if err := os.RemoveAll(path); err != nil {
					return out, err
				}
				continue
			}
			if !strings.HasPrefix(entry.Name(), "gc-") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return out, err
			}
			if info.ModTime().After(time.Now()) {
				out.DeferredSnapshots++
				continue
			}
			var done bool
			if info.IsDir() {
				done, err = consumeRun9RetiredSnapshot(ctx, path, 65536-out.DeletedObjects, &out)
			} else {
				err = fmt.Errorf("GC snapshot is not a directory")
			}
			if err != nil {
				out.OK = false
				out.FailedSnapshots++
				// Only task directories carry retry state. In particular, never
				// follow an invalid symlink to change another path's timestamps.
				if info.IsDir() {
					retryAt := time.Now().Add(5 * time.Minute)
					if retryErr := os.Chtimes(path, retryAt, retryAt); retryErr != nil {
						err = fmt.Errorf("%w; defer retry: %v", err, retryErr)
					}
				}
				if out.Error == "" {
					out.Error = fmt.Sprintf("snapshot %s: %v", entry.Name(), err)
				}
				logger.Errorf("retired slice GC %s: %s", entry.Name(), err)
			} else if done {
				// A crash during recursive cleanup must not leave a half-removed
				// Badger in the runnable namespace.
				finished := filepath.Join(dir, ".done-"+entry.Name())
				if err := os.Rename(path, finished); err != nil {
					return out, err
				}
				out.CompletedSnapshots++
				if err := os.RemoveAll(finished); err != nil {
					return out, err
				}
			} else {
				out.PendingSnapshots++
			}
			if out.DeletedObjects >= 65536 {
				break
			}
		}
	}
	return out, nil
}

func consumeRun9RetiredSnapshot(ctx context.Context, path string, objectBudget uint64, out *run9RetiredSliceGCResult) (done bool, err error) {
	proofPath := filepath.Join(path, "proof.json")
	info, err := os.Lstat(proofPath)
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return false, fmt.Errorf("invalid snapshot proof file")
	}
	raw, err := os.ReadFile(proofPath)
	if err != nil {
		return false, err
	}
	if !strings.HasSuffix(filepath.Base(path), fmt.Sprintf("-%x", sha256.Sum256(raw))) {
		return false, fmt.Errorf("snapshot proof checksum mismatch")
	}
	var proof run9RetiredSliceSnapshot
	if err := json.Unmarshal(raw, &proof); err != nil {
		return false, err
	}
	if proof.Version != 1 || proof.Format == "" || proof.OwnedEpoch == 0 || proof.OwnedEpoch >= 1<<32-1 || proof.Start < proof.OwnedEpoch<<32 || proof.Start > (proof.OwnedEpoch+1)<<32 {
		return false, fmt.Errorf("invalid snapshot allocation proof")
	}
	metaDir := filepath.Join(path, "meta")
	info, err = os.Lstat(metaDir)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("snapshot metadata is not a directory")
	}
	m, err := meta.OpenRun9RetiredSnapshot(metaDir)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, m.Shutdown()) }()
	format, err := m.Load(true)
	if err != nil {
		return false, err
	}
	if format.Name != proof.Format || format.UUID == "" || format.BlockSize <= 0 {
		return false, fmt.Errorf("snapshot format mismatch")
	}
	end, err := m.Run9SliceAllocationCounter()
	if err != nil {
		return false, err
	}
	if end < proof.Start || end > (proof.OwnedEpoch+1)<<32 {
		return false, fmt.Errorf("snapshot allocation counter outside owned epoch")
	}
	layout := run9ObjectLayout{BlockSizeBytes: format.BlockSize * 1024, HashPrefix: format.HashPrefix}
	blob, err := run9SliceRangeObjectStorage(format.Name, layout, run9ObjectStorageDescriptorFromFormat(*format))
	if err != nil {
		return false, err
	}
	defer object.Shutdown(blob)
	cursorPath := filepath.Join(path, "cursor")
	if info, err := os.Lstat(cursorPath); err == nil {
		if !info.Mode().IsRegular() || info.Size() != 26 {
			return false, fmt.Errorf("invalid snapshot cursor file")
		}
	} else if !os.IsNotExist(err) {
		return false, err
	}
	cursor, err := os.ReadFile(cursorPath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	// Aim for at most 1,024 DELETEs per page at the maximum slice size. A single
	// slice with a tiny block layout may exceed that target (at most 65,536).
	// These limits bound each turn's work, not total lifecycle garbage coverage.
	blocksPerSlice := (uint64(meta.ChunkSize) + uint64(layout.BlockSizeBytes) - 1) / uint64(layout.BlockSizeBytes)
	pageSize := int(max(uint64(1), 1024/blocksPerSlice))
	// Yield between completed pages, not halfway through every slow page. The
	// invocation context still bounds IO; page checkpoints make retries safe.
	deadline := time.Now().Add(15 * time.Second)
	var deleted uint64
	for deleted < objectBudget && time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		slices, next, err := m.Run9RetiredSlices(ctx, proof.Start, end, string(cursor), pageSize)
		if err != nil {
			return false, err
		}
		out.ScannedPages++
		var objects []run9GCExactObject
		for _, s := range slices {
			for offset, index := uint64(0), uint64(0); offset < uint64(s.Size); index++ {
				size := min(uint64(layout.BlockSizeBytes), uint64(s.Size)-offset)
				objects = append(objects, run9GCExactObject{Key: chunk.FormatObjectBlockKey(s.Id, index, size, layout.HashPrefix), Size: size})
				offset += size
			}
		}
		n, bytes, err := deleteRun9ExactObjects(ctx, blob, objects, 4)
		out.DeletedObjects += n
		out.DeletedLogicalBytes += bytes
		deleted += n
		if err != nil {
			return false, err
		}
		if next == "" {
			return true, nil
		}
		// Checkpoint only a completely deleted page. A crash before rename
		// repeats idempotent DELETEs; it cannot skip uncompleted objects.
		f, err := os.CreateTemp(path, ".cursor-")
		if err != nil {
			return false, err
		}
		_, writeErr := f.WriteString(next)
		closeErr := f.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			_ = os.Remove(f.Name())
			return false, err
		}
		if err := os.Rename(f.Name(), cursorPath); err != nil {
			_ = os.Remove(f.Name())
			return false, err
		}
		cursor = []byte(next)
	}
	return false, nil
}
