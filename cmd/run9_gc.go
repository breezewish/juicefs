package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/urfave/cli/v2"
)

type run9ObjectLayout struct {
	BlockSizeBytes int  `json:"block_size_bytes"`
	HashPrefix     bool `json:"hash_prefix"`
}

type run9LiveSlice struct {
	ID   uint64 `json:"id"`
	Size uint32 `json:"size"`
}

type run9ListLiveSlicesOutput struct {
	OK                bool             `json:"ok"`
	JuiceFSFormatName string           `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout `json:"object_layout"`
	Slices            []run9LiveSlice  `json:"slices"`
}

type run9ProtectedRange struct {
	Start        uint64 `json:"start"`
	EndInclusive uint64 `json:"end_inclusive"`
}

type run9GCLineageObjectsRequest struct {
	FormatMetaURL     string               `json:"format_meta_url"`
	JuiceFSFormatName string               `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout     `json:"object_layout"`
	LiveSlices        []run9LiveSlice      `json:"live_slices"`
	ProtectedRanges   []run9ProtectedRange `json:"protected_ranges"`
	MaxDeleteObjects  uint64               `json:"max_delete_objects"`
	Threads           int                  `json:"threads"`
}

type run9GCLineageObjectsOutput struct {
	OK             bool   `json:"ok"`
	DeletedObjects uint64 `json:"deleted_objects"`
	DeletedBytes   uint64 `json:"deleted_bytes"`
	HasMore        bool   `json:"has_more"`
}

type run9ParsedObjectBlock = chunk.ObjectBlockKey

func cmdRun9ListLiveSlices() *cli.Command {
	return &cli.Command{
		Name:      "list-live-slices",
		Hidden:    true,
		Usage:     "run9 internal: list live slice manifest data",
		ArgsUsage: "META-URL",
		Action: func(ctx *cli.Context) error {
			setup(ctx, 1)
			out, err := run9ListLiveSlices(ctx.Context, ctx.Args().Get(0))
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func cmdRun9GCLineageObjects() *cli.Command {
	return &cli.Command{
		Name:   "gc-lineage-objects",
		Hidden: true,
		Usage:  "run9 internal: delete unprotected lineage objects",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "request",
				Required: true,
				Usage:    "path to JSON request",
			},
		},
		Action: func(ctx *cli.Context) error {
			setup(ctx, 0)
			raw, err := os.ReadFile(ctx.String("request"))
			if err != nil {
				return fmt.Errorf("read request: %w", err)
			}
			var req run9GCLineageObjectsRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return fmt.Errorf("parse request: %w", err)
			}
			out, err := run9GCLineageObjects(ctx.Context, req)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func run9ListLiveSlices(ctx context.Context, metaURL string) (run9ListLiveSlicesOutput, error) {
	metaURL = strings.TrimSpace(metaURL)
	if metaURL == "" {
		return run9ListLiveSlicesOutput{}, fmt.Errorf("missing meta url")
	}

	metaConf := meta.DefaultConf()
	metaConf.NoBGJob = true
	m := meta.NewClient(metaURL, metaConf)
	format, err := m.Load(true)
	if err != nil {
		return run9ListLiveSlicesOutput{}, fmt.Errorf("load setting: %w", err)
	}
	if err := m.NewSession(false); err != nil {
		return run9ListLiveSlicesOutput{}, fmt.Errorf("new session: %w", err)
	}
	defer m.CloseSession() //nolint:errcheck

	slicesByInode := map[meta.Ino][]meta.Slice{}
	if st := m.ListSlices(meta.WrapContext(ctx), slicesByInode, false, false, nil); st != 0 {
		return run9ListLiveSlicesOutput{}, fmt.Errorf("list slices: %s", st)
	}

	slicesByID := map[uint64]uint32{}
	for _, inodeSlices := range slicesByInode {
		for _, s := range inodeSlices {
			if s.Size == 0 {
				continue
			}
			if existing := slicesByID[s.Id]; existing < s.Size {
				slicesByID[s.Id] = s.Size
			}
		}
	}

	slices := make([]run9LiveSlice, 0, len(slicesByID))
	for id, size := range slicesByID {
		slices = append(slices, run9LiveSlice{ID: id, Size: size})
	}
	sort.Slice(slices, func(i, j int) bool {
		return slices[i].ID < slices[j].ID
	})

	return run9ListLiveSlicesOutput{
		OK:                true,
		JuiceFSFormatName: format.Name,
		ObjectLayout: run9ObjectLayout{
			BlockSizeBytes: format.BlockSize * 1024,
			HashPrefix:     format.HashPrefix,
		},
		Slices: slices,
	}, nil
}

func run9GCLineageObjects(ctx context.Context, req run9GCLineageObjectsRequest) (run9GCLineageObjectsOutput, error) {
	if strings.TrimSpace(req.FormatMetaURL) == "" {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("missing format_meta_url")
	}
	if strings.TrimSpace(req.JuiceFSFormatName) == "" {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("missing juicefs_format_name")
	}
	if err := req.ObjectLayout.validate(); err != nil {
		return run9GCLineageObjectsOutput{}, err
	}
	if req.MaxDeleteObjects == 0 {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("max_delete_objects must be greater than 0")
	}
	threads := req.Threads
	if threads <= 0 {
		threads = 1
	}

	metaConf := meta.DefaultConf()
	metaConf.NoBGJob = true
	m := meta.NewClient(strings.TrimSpace(req.FormatMetaURL), metaConf)
	format, err := m.Load(true)
	if err != nil {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("load setting: %w", err)
	}
	if format.Name != req.JuiceFSFormatName {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("format name mismatch: loaded %q request %q", format.Name, req.JuiceFSFormatName)
	}
	if format.BlockSize*1024 != req.ObjectLayout.BlockSizeBytes || format.HashPrefix != req.ObjectLayout.HashPrefix {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("object layout mismatch with loaded format")
	}

	blob, err := createStorage(*format)
	if err != nil {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)

	protectedSlices := map[uint64]uint32{}
	for _, s := range req.LiveSlices {
		if s.Size == 0 {
			continue
		}
		if existing := protectedSlices[s.ID]; existing < s.Size {
			protectedSlices[s.ID] = s.Size
		}
	}
	ranges := normalizedRun9Ranges(req.ProtectedRanges)

	listCtx, cancelList := context.WithCancel(ctx)
	defer cancelList()

	objs, err := object.ListAll(listCtx, blob, "chunks/", "", true, true)
	if err != nil {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("list lineage objects: %w", err)
	}

	deleteJobs := make(chan object.Object)
	var deletedObjects atomic.Uint64
	var deletedBytes atomic.Uint64
	var scheduledDeletes atomic.Uint64
	var firstDeleteErr error
	var firstDeleteErrMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range deleteJobs {
				firstDeleteErrMu.Lock()
				hasDeleteErr := firstDeleteErr != nil
				firstDeleteErrMu.Unlock()
				if hasDeleteErr {
					continue
				}
				if err := blob.Delete(ctx, obj.Key()); err != nil {
					firstDeleteErrMu.Lock()
					if firstDeleteErr == nil {
						firstDeleteErr = err
					}
					firstDeleteErrMu.Unlock()
					cancelList()
					continue
				}
				deletedObjects.Add(1)
				if obj.Size() > 0 {
					deletedBytes.Add(uint64(obj.Size()))
				}
			}
		}()
	}

	hasMore := false
	for obj := range objs {
		if obj == nil {
			close(deleteJobs)
			wg.Wait()
			return run9GCLineageObjectsOutput{}, fmt.Errorf("list lineage objects failed")
		}
		if obj.IsDir() {
			continue
		}
		firstDeleteErrMu.Lock()
		hasDeleteErr := firstDeleteErr != nil
		firstDeleteErrMu.Unlock()
		if hasDeleteErr {
			break
		}
		parsed, ok := parseRun9ObjectBlockKey(obj.Key(), req.ObjectLayout)
		if !ok {
			continue
		}
		if run9ObjectBlockProtected(parsed, req.ObjectLayout.BlockSizeBytes, protectedSlices, ranges) {
			continue
		}
		if scheduledDeletes.Add(1) > req.MaxDeleteObjects {
			hasMore = true
			cancelList()
			break
		}
		deleteJobs <- obj
	}
	close(deleteJobs)
	wg.Wait()

	if firstDeleteErr != nil {
		return run9GCLineageObjectsOutput{}, fmt.Errorf("delete lineage object: %w", firstDeleteErr)
	}

	if deletedObjects.Load() >= req.MaxDeleteObjects {
		hasMore = true
	}
	return run9GCLineageObjectsOutput{
		OK:             true,
		DeletedObjects: deletedObjects.Load(),
		DeletedBytes:   deletedBytes.Load(),
		HasMore:        hasMore,
	}, nil
}

func (l run9ObjectLayout) validate() error {
	if l.BlockSizeBytes <= 0 {
		return fmt.Errorf("object_layout.block_size_bytes must be greater than 0")
	}
	return nil
}

func parseRun9ObjectBlockKey(key string, layout run9ObjectLayout) (run9ParsedObjectBlock, bool) {
	if err := layout.validate(); err != nil {
		return run9ParsedObjectBlock{}, false
	}
	block, ok := chunk.ParseObjectBlockKey(key, layout.HashPrefix)
	if !ok {
		return run9ParsedObjectBlock{}, false
	}
	if block.BlockSize == 0 || block.BlockSize > uint64(layout.BlockSizeBytes) {
		return run9ParsedObjectBlock{}, false
	}
	return block, true
}

func run9ObjectBlockProtected(
	block run9ParsedObjectBlock,
	blockSizeBytes int,
	liveSlices map[uint64]uint32,
	ranges []run9ProtectedRange,
) bool {
	for _, r := range ranges {
		if block.SliceID >= r.Start && block.SliceID <= r.EndInclusive {
			return true
		}
	}

	sliceSize := uint64(liveSlices[block.SliceID])
	if sliceSize == 0 {
		return false
	}
	formatBlockSize := uint64(blockSizeBytes)
	lastBlockIndex := (sliceSize - 1) / formatBlockSize
	if block.BlockIndex > lastBlockIndex {
		return false
	}
	expectedSize := formatBlockSize
	if block.BlockIndex == lastBlockIndex {
		expectedSize = sliceSize - block.BlockIndex*formatBlockSize
	}
	return block.BlockSize == expectedSize
}

func normalizedRun9Ranges(input []run9ProtectedRange) []run9ProtectedRange {
	ranges := make([]run9ProtectedRange, 0, len(input))
	for _, r := range input {
		if r.Start > r.EndInclusive {
			continue
		}
		ranges = append(ranges, r)
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Start != ranges[j].Start {
			return ranges[i].Start < ranges[j].Start
		}
		return ranges[i].EndInclusive < ranges[j].EndInclusive
	})
	merged := ranges[:0]
	for _, r := range ranges {
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if last.EndInclusive != math.MaxUint64 && r.Start <= last.EndInclusive+1 {
			if r.EndInclusive > last.EndInclusive {
				last.EndInclusive = r.EndInclusive
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

func run9SliceEpochRange(epoch uint64) run9ProtectedRange {
	start := epoch << 32
	if epoch >= math.MaxUint32 {
		return run9ProtectedRange{Start: start, EndInclusive: math.MaxUint64}
	}
	return run9ProtectedRange{Start: start, EndInclusive: ((epoch + 1) << 32) - 1}
}

func run9FutureEpochRange(lastAllocatedEpoch uint64) (run9ProtectedRange, bool) {
	if lastAllocatedEpoch >= math.MaxUint32 {
		return run9ProtectedRange{}, false
	}
	start := (lastAllocatedEpoch + 1) << 32
	return run9ProtectedRange{Start: start, EndInclusive: math.MaxUint64}, true
}
