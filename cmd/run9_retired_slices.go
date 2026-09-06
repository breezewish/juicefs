package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/urfave/cli/v2"
)

const run9RetiredSliceLimit = 4096

type run9RetiredSliceMetadata interface {
	Run9SliceAllocationCounter() (uint64, error)
	Run9RetiredSlices(context.Context, uint64, uint64, int) ([]meta.Slice, bool, error)
}

// A batch is an immutable deletion authorization, not a live-slice manifest.
// Only a successfully finalized mount may publish it. It lives outside meta/ so
// fork never copies it; consumption never opens a snap or touches writeback.
type run9RetiredSliceBatch struct {
	Version    int                         `json:"version"`
	Format     string                      `json:"format"`
	Storage    run9ObjectStorageDescriptor `json:"storage"`
	Layout     run9ObjectLayout            `json:"layout"`
	OwnedEpoch uint64                      `json:"owned_epoch"`
	Start      uint64                      `json:"start"`
	End        uint64                      `json:"end"`
	Slices     []run9LiveSlice             `json:"slices"`
}

type run9RetiredSliceGC struct {
	metadata run9RetiredSliceMetadata
	dir      string
	batch    run9RetiredSliceBatch
}

func beginRun9RetiredSliceGC(m meta.Meta, format *meta.Format) (*run9RetiredSliceGC, error) {
	dir := os.Getenv("JFS_RUN9_RETIRED_SLICES_DIR")
	if dir == "" {
		return nil, nil
	}
	epoch, err := strconv.ParseUint(os.Getenv("JFS_RUN9_OWNED_EPOCH"), 10, 64)
	if err != nil || epoch == 0 || epoch >= 1<<32-1 || !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("invalid run9 retired slice GC configuration")
	}
	metadata, ok := m.(run9RetiredSliceMetadata)
	if !ok || m.Name() != "badger" {
		return nil, fmt.Errorf("run9 retired slice GC requires Badger")
	}
	start, err := metadata.Run9SliceAllocationCounter()
	if err != nil {
		return nil, err
	}
	if start < epoch<<32 || start > (epoch+1)<<32 {
		return nil, fmt.Errorf("slice allocation counter %d is outside owned epoch %d", start, epoch)
	}
	return &run9RetiredSliceGC{metadata: metadata, dir: dir, batch: run9RetiredSliceBatch{
		Version: 1, Format: format.Name, Storage: run9ObjectStorageDescriptorFromFormat(*format),
		Layout:     run9ObjectLayout{BlockSizeBytes: format.BlockSize * 1024, HashPrefix: format.HashPrefix},
		OwnedEpoch: epoch, Start: start,
	}}, nil
}

// collect runs after CloseSession and before Shutdown, with no new user I/O.
// A bounded scan may leave garbage behind, never widen its deletion proof.
func (gc *run9RetiredSliceGC) collect(ctx context.Context) (bool, error) {
	end, err := gc.metadata.Run9SliceAllocationCounter()
	if err != nil {
		return false, err
	}
	gc.batch.End = min(end, (gc.batch.OwnedEpoch+1)<<32)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	slices, truncated, err := gc.metadata.Run9RetiredSlices(ctx, gc.batch.Start, gc.batch.End, run9RetiredSliceLimit)
	if err != nil {
		return false, err
	}
	var objects uint64
	for _, s := range slices {
		objects += (uint64(s.Size) + uint64(gc.batch.Layout.BlockSizeBytes) - 1) / uint64(gc.batch.Layout.BlockSizeBytes)
		if objects > 65536 {
			truncated = true
			break
		}
		gc.batch.Slices = append(gc.batch.Slices, run9LiveSlice{ID: s.Id, Size: s.Size})
	}
	return truncated, nil
}

func (batch run9RetiredSliceBatch) validate() error {
	if batch.Version != 1 || batch.Format == "" || strings.ContainsAny(batch.Format, "/\\") || batch.Format == "." || batch.Format == ".." || batch.Storage.UUID == "" {
		return fmt.Errorf("invalid retired slice batch identity")
	}
	if err := batch.Layout.validate(); err != nil {
		return err
	}
	if err := batch.Storage.validate(); err != nil {
		return err
	}
	if batch.OwnedEpoch == 0 || batch.OwnedEpoch >= 1<<32-1 || batch.Start < batch.OwnedEpoch<<32 || batch.Start >= batch.End || batch.End > (batch.OwnedEpoch+1)<<32 || len(batch.Slices) == 0 || len(batch.Slices) > run9RetiredSliceLimit {
		return fmt.Errorf("invalid retired slice batch bounds")
	}
	var previous, objects uint64
	for _, s := range batch.Slices {
		if s.ID < batch.Start || s.ID >= batch.End || s.ID <= previous || s.Size == 0 || s.Size > meta.ChunkSize {
			return fmt.Errorf("invalid retired slice %d size %d", s.ID, s.Size)
		}
		previous = s.ID
		objects += (uint64(s.Size) + uint64(batch.Layout.BlockSizeBytes) - 1) / uint64(batch.Layout.BlockSizeBytes)
		if objects > 65536 {
			return fmt.Errorf("retired slice batch exceeds object budget")
		}
	}
	return nil
}

// publish is called only after successful metadata shutdown AND success ack.
// A crash before publication leaks garbage conservatively; temporary files are
// never deletion authorization. The content hash also detects disk corruption.
func (gc *run9RetiredSliceGC) publish() error {
	if len(gc.batch.Slices) == 0 {
		return nil
	}
	if err := gc.batch.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(gc.dir, 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(gc.batch)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(gc.dir, ".tmp-retired-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	sum := sha256.Sum256(raw)
	return os.Rename(f.Name(), filepath.Join(gc.dir, hex.EncodeToString(sum[:])+".json"))
}

type run9RetiredSliceGCResult struct {
	OK               bool   `json:"ok"`
	Busy             bool   `json:"busy,omitempty"`
	CompletedBatches int    `json:"completed_batches"`
	FailedBatches    int    `json:"failed_batches"`
	DeletedObjects   uint64 `json:"deleted_objects"`
	// Exact keys encode uncompressed sizes; this is not a provider billing metric.
	DeletedLogicalBytes uint64 `json:"deleted_logical_bytes"`
	Error               string `json:"error,omitempty"`
}

func cmdRun9GCRetiredSlices() *cli.Command {
	return &cli.Command{Name: "gc-retired-slices", Hidden: true, Usage: "consume finalized private-slice deletion batches",
		Flags: []cli.Flag{&cli.StringFlag{Name: "queue-dir", Required: true}},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("gc-retired-slices takes no positional arguments")
			}
			ctx, cancel := context.WithTimeout(c.Context, 2*time.Minute)
			defer cancel()
			out, err := run9GCRetiredSlices(ctx, c.String("queue-dir"))
			if err != nil {
				return err
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
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return out, fmt.Errorf("retired slice queue is not a directory")
	}
	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer f.Close()
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
	// Stream directory entries, not every snap or every remote object. A corrupt
	// batch stays visible for diagnosis but does not block healthy batches behind it.
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
			if !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			n, bytes, err := consumeRun9RetiredSliceBatch(ctx, path)
			if err != nil {
				out.OK = false
				out.FailedBatches++
				if out.Error == "" {
					out.Error = fmt.Sprintf("batch %s: %v", entry.Name(), err)
				}
				logger.Errorf("retired slice GC batch %s: %s", entry.Name(), err)
				continue
			}
			out.CompletedBatches++
			out.DeletedObjects += n
			out.DeletedLogicalBytes += bytes
			if out.DeletedObjects >= 65536 {
				break
			}
		}
	}
	return out, nil
}

func consumeRun9RetiredSliceBatch(ctx context.Context, path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return 0, 0, fmt.Errorf("invalid batch file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	sum := sha256.Sum256(raw)
	if filepath.Base(path) != hex.EncodeToString(sum[:])+".json" {
		return 0, 0, fmt.Errorf("batch checksum mismatch")
	}
	var batch run9RetiredSliceBatch
	if err := json.Unmarshal(raw, &batch); err != nil {
		return 0, 0, err
	}
	if err := batch.validate(); err != nil {
		return 0, 0, err
	}
	blob, err := run9SliceRangeObjectStorage(batch.Format, batch.Layout, batch.Storage)
	if err != nil {
		return 0, 0, err
	}
	defer object.Shutdown(blob)
	var objects []run9GCExactObject
	for _, s := range batch.Slices {
		for offset, index := uint64(0), uint64(0); offset < uint64(s.Size); index++ {
			size := min(uint64(batch.Layout.BlockSizeBytes), uint64(s.Size)-offset)
			objects = append(objects, run9GCExactObject{Key: chunk.FormatObjectBlockKey(s.ID, index, size, batch.Layout.HashPrefix), Size: size})
			offset += size
		}
	}
	n, bytes, err := deleteRun9ExactObjects(ctx, blob, objects, 4)
	if err != nil {
		return n, bytes, err
	}
	// Delete is idempotent, including overlap with whole-subtree GC. Never open
	// active Badger to remove its K records from this independent worker.
	if err := os.Remove(path); err != nil {
		return n, bytes, err
	}
	return n, bytes, nil
}
