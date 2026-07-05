package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/urfave/cli/v2"
)

type run9ObjectLayout struct {
	BlockSizeBytes int  `json:"block_size_bytes"`
	HashPrefix     bool `json:"hash_prefix"`
}

type run9ObjectStorageDescriptor struct {
	Storage      string `json:"storage"`
	Bucket       string `json:"bucket"`
	UUID         string `json:"uuid,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
	StorageClass string `json:"storage_class,omitempty"`
	Shards       int    `json:"shards,omitempty"`
	EncryptKey   string `json:"encrypt_key,omitempty"`
	EncryptAlgo  string `json:"encrypt_algo,omitempty"`
	KeyEncrypted bool   `json:"key_encrypted,omitempty"`
}

type run9LiveSlice struct {
	ID   uint64 `json:"id"`
	Size uint32 `json:"size"`
}

type run9LiveSlicePathSummary struct {
	Inode          uint64   `json:"inode,omitempty"`
	Paths          []string `json:"paths,omitempty"`
	Count          int      `json:"count"`
	TotalSizeBytes uint64   `json:"total_size_bytes"`
	Unattributed   bool     `json:"unattributed,omitempty"`
}

type run9LiveSliceRef struct {
	Inode                  uint64   `json:"inode"`
	Paths                  []string `json:"paths,omitempty"`
	ChunkIndex             uint32   `json:"chunk_index"`
	ChunkOffsetBytes       uint64   `json:"chunk_offset_bytes"`
	FileOffsetBytes        uint64   `json:"file_offset_bytes"`
	SliceID                uint64   `json:"slice_id"`
	SliceSizeBytes         uint32   `json:"slice_size_bytes"`
	SliceObjectOffsetBytes uint32   `json:"slice_object_offset_bytes"`
	SliceLenBytes          uint32   `json:"slice_len_bytes"`
}

type run9ListLiveSlicesOutput struct {
	OK                bool                       `json:"ok"`
	JuiceFSFormatName string                     `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout           `json:"object_layout"`
	Slices            []run9LiveSlice            `json:"slices"`
	PathSummaries     []run9LiveSlicePathSummary `json:"path_summaries,omitempty"`
	SliceRefs         []run9LiveSliceRef         `json:"slice_refs,omitempty"`
}

type run9DescribeFormatOutput struct {
	OK                bool                        `json:"ok"`
	JuiceFSFormatName string                      `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout            `json:"object_layout"`
	ObjectStorage     run9ObjectStorageDescriptor `json:"object_storage"`
}

type run9GCExactObject struct {
	Key  string `json:"key"`
	Size uint64 `json:"size"`
}

type run9GCSliceRange struct {
	SnapID       string `json:"snap_id,omitempty"`
	Start        uint64 `json:"start"`
	EndInclusive uint64 `json:"end_inclusive"`
}

type run9GCSliceRangesRequest struct {
	JuiceFSFormatName string                      `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout            `json:"object_layout"`
	ObjectStorage     run9ObjectStorageDescriptor `json:"object_storage"`
	Ranges            []run9GCSliceRange          `json:"ranges"`
	MaxDeleteObjects  uint64                      `json:"max_delete_objects"`
	Threads           int                         `json:"threads"`
}

type run9GCSliceRangesOutput struct {
	OK                bool   `json:"ok"`
	ScanListRequests  uint64 `json:"scan_list_requests"`
	ScanListedObjects uint64 `json:"scan_listed_objects"`
	DeletedObjects    uint64 `json:"deleted_objects"`
	DeletedBytes      uint64 `json:"deleted_bytes"`
	HasMore           bool   `json:"has_more"`
}

type run9CountSliceRangesRequest struct {
	JuiceFSFormatName string                      `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout            `json:"object_layout"`
	ObjectStorage     run9ObjectStorageDescriptor `json:"object_storage"`
	Ranges            []run9GCSliceRange          `json:"ranges"`
}

type run9CountSliceRangesOutput struct {
	OK                bool                         `json:"ok"`
	ScanListRequests  uint64                       `json:"scan_list_requests"`
	ScanListedObjects uint64                       `json:"scan_listed_objects"`
	Objects           uint64                       `json:"objects"`
	Bytes             uint64                       `json:"bytes"`
	Ranges            []run9CountSliceRangeAccount `json:"ranges"`
}

type run9CountSliceRangeAccount struct {
	SnapID       string `json:"snap_id,omitempty"`
	Start        uint64 `json:"start"`
	EndInclusive uint64 `json:"end_inclusive"`
	Objects      uint64 `json:"objects"`
	Bytes        uint64 `json:"bytes"`
}

const (
	run9GCExactObjectsMaxBulkDeleteBatchSize = 1000
	run9GCExactObjectsDeleteAttempts         = 12
	run9GCExactObjectsInitialRetryDelay      = 500 * time.Millisecond
	run9GCExactObjectsMaxRetryDelay          = 10 * time.Second
	run9GCSliceRangesListPageSize            = 10000
)

type run9GCSliceRangeScanStats struct {
	listRequests  atomic.Uint64
	listedObjects atomic.Uint64
	done          chan struct{}
}

func newRun9GCSliceRangeScanStats() *run9GCSliceRangeScanStats {
	return &run9GCSliceRangeScanStats{done: make(chan struct{})}
}

func (s *run9GCSliceRangeScanStats) record(listedObjects int) {
	s.listRequests.Add(1)
	if listedObjects > 0 {
		s.listedObjects.Add(uint64(listedObjects))
	}
}

func (s *run9GCSliceRangeScanStats) snapshot() (uint64, uint64) {
	return s.listRequests.Load(), s.listedObjects.Load()
}

func run9GCExactObjectsBulkDeleteBatchSize(objectCount int, threads int) int {
	if objectCount <= 0 {
		return 0
	}
	if threads <= 1 {
		return run9GCExactObjectsMaxBulkDeleteBatchSize
	}
	batchSize := (objectCount + threads - 1) / threads
	if batchSize < 1 {
		return 1
	}
	if batchSize > run9GCExactObjectsMaxBulkDeleteBatchSize {
		return run9GCExactObjectsMaxBulkDeleteBatchSize
	}
	return batchSize
}

func cmdRun9ListLiveSlices() *cli.Command {
	return &cli.Command{
		Name:      "list-live-slices",
		Hidden:    true,
		Usage:     "run9 internal: list live slice manifest data",
		ArgsUsage: "META-URL",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "scan-pending",
				Usage: "include pending deleted and delayed slices",
			},
			&cli.BoolFlag{
				Name:  "include-paths",
				Usage: "include per-inode path summaries for diagnostics",
			},
			&cli.BoolFlag{
				Name:  "include-refs",
				Usage: "include per-file live slice references with chunk offsets for diagnostics",
			},
		},
		Action: func(ctx *cli.Context) error {
			setup(ctx, 1)
			out, err := run9ListSlices(ctx.Context, ctx.Args().Get(0), ctx.Bool("scan-pending"), ctx.Bool("include-paths"), ctx.Bool("include-refs"))
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func cmdRun9DescribeFormat() *cli.Command {
	return &cli.Command{
		Name:      "describe-format",
		Hidden:    true,
		Usage:     "run9 internal: describe JuiceFS format storage",
		ArgsUsage: "META-URL",
		Action: func(ctx *cli.Context) error {
			setup(ctx, 1)
			out, err := run9DescribeFormat(ctx.Context, ctx.Args().Get(0))
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func cmdRun9PrepareWritableEpoch() *cli.Command {
	return &cli.Command{
		Name:      "prepare-writable-epoch",
		Hidden:    true,
		Usage:     "run9 internal: seed a fresh writable epoch into badger metadata",
		ArgsUsage: "META-URL OWNED-EPOCH",
		Action: func(ctx *cli.Context) error {
			setup(ctx, 2)
			metaURL := ctx.Args().Get(0)
			ownedEpochText := strings.TrimSpace(ctx.Args().Get(1))
			ownedEpoch, err := strconv.ParseUint(ownedEpochText, 10, 64)
			if err != nil {
				return fmt.Errorf("parse owned epoch %q: %w", ownedEpochText, err)
			}
			if ownedEpoch == 0 {
				return fmt.Errorf("owned epoch must be greater than 0")
			}
			out, err := run9PrepareWritableEpoch(ctx.Context, metaURL, ownedEpoch)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func cmdRun9GCSliceRanges() *cli.Command {
	return &cli.Command{
		Name:   "gc-slice-ranges",
		Hidden: true,
		Usage:  "run9 internal: delete objects whose slice ids fall within one or more ranges",
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
			var req run9GCSliceRangesRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return fmt.Errorf("parse request: %w", err)
			}
			out, err := run9GCSliceRanges(ctx.Context, req)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func cmdRun9CountSliceRanges() *cli.Command {
	return &cli.Command{
		Name:   "count-slice-ranges",
		Hidden: true,
		Usage:  "run9 internal: count objects whose slice ids fall within one or more ranges",
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
			var req run9CountSliceRangesRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return fmt.Errorf("parse request: %w", err)
			}
			out, err := run9CountSliceRanges(ctx.Context, req)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(out)
		},
	}
}

func run9ListSlices(ctx context.Context, metaURL string, scanPending bool, includePaths bool, includeRefs bool) (run9ListLiveSlicesOutput, error) {
	metaURL = strings.TrimSpace(metaURL)
	if metaURL == "" {
		return run9ListLiveSlicesOutput{}, fmt.Errorf("missing meta url")
	}

	metaConf := meta.DefaultConf()
	metaConf.NoBGJob = true
	m := meta.NewClient(metaURL, metaConf)
	format, err := m.Load(true)
	if err != nil {
		_ = m.Shutdown()
		return run9ListLiveSlicesOutput{}, fmt.Errorf("load setting: %w", err)
	}
	if err := m.NewSession(false); err != nil {
		_ = m.Shutdown()
		return run9ListLiveSlicesOutput{}, fmt.Errorf("new session: %w", err)
	}
	defer func() {
		_ = m.CloseSession()
		_ = m.Shutdown()
	}()

	slicesByInode := map[meta.Ino][]meta.Slice{}
	if st := m.ListSlices(meta.WrapContext(ctx), slicesByInode, scanPending, false, nil); st != 0 {
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

	var pathSummaries []run9LiveSlicePathSummary
	if includePaths {
		pathSummaries = run9LiveSlicePathSummaries(meta.WrapContext(ctx), m, slicesByInode)
	}
	var sliceRefs []run9LiveSliceRef
	if includeRefs {
		var err error
		sliceRefs, err = run9LiveSliceRefs(meta.WrapContext(ctx), m, slicesByInode)
		if err != nil {
			return run9ListLiveSlicesOutput{}, err
		}
	}

	return run9ListLiveSlicesOutput{
		OK:                true,
		JuiceFSFormatName: format.Name,
		ObjectLayout: run9ObjectLayout{
			BlockSizeBytes: format.BlockSize * 1024,
			HashPrefix:     format.HashPrefix,
		},
		Slices:        slices,
		PathSummaries: pathSummaries,
		SliceRefs:     sliceRefs,
	}, nil
}

func run9LiveSlicePathSummaries(ctx meta.Context, m meta.Meta, slicesByInode map[meta.Ino][]meta.Slice) []run9LiveSlicePathSummary {
	inodes := make([]meta.Ino, 0, len(slicesByInode))
	for inode := range slicesByInode {
		inodes = append(inodes, inode)
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	summaries := make([]run9LiveSlicePathSummary, 0, len(inodes))
	for _, inode := range inodes {
		sliceSizes := map[uint64]uint32{}
		for _, s := range slicesByInode[inode] {
			if s.Size == 0 {
				continue
			}
			if existing := sliceSizes[s.Id]; existing < s.Size {
				sliceSizes[s.Id] = s.Size
			}
		}
		if len(sliceSizes) == 0 {
			continue
		}

		summary := run9LiveSlicePathSummary{
			Inode: uint64(inode),
			Count: len(sliceSizes),
		}
		for _, size := range sliceSizes {
			summary.TotalSizeBytes += uint64(size)
		}

		if inode == 0 {
			summary.Unattributed = true
		} else {
			summary.Paths = m.GetPaths(ctx, inode)
			sort.Strings(summary.Paths)
			if len(summary.Paths) == 0 {
				summary.Unattributed = true
			}
		}
		summaries = append(summaries, summary)
	}

	sort.Slice(summaries, func(i, j int) bool {
		leftPath := strings.Join(summaries[i].Paths, "\x00")
		rightPath := strings.Join(summaries[j].Paths, "\x00")
		if leftPath == rightPath {
			return summaries[i].Inode < summaries[j].Inode
		}
		return leftPath < rightPath
	})
	return summaries
}

func run9LiveSliceRefs(ctx meta.Context, m meta.Meta, slicesByInode map[meta.Ino][]meta.Slice) ([]run9LiveSliceRef, error) {
	inodes := make([]meta.Ino, 0, len(slicesByInode))
	for inode := range slicesByInode {
		if inode != 0 {
			inodes = append(inodes, inode)
		}
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	var refs []run9LiveSliceRef
	for _, inode := range inodes {
		var attr meta.Attr
		if st := m.GetAttr(ctx, inode, &attr); st != 0 {
			return nil, fmt.Errorf("get attr for inode %d: %s", inode, st)
		}
		if attr.Typ != meta.TypeFile {
			return nil, fmt.Errorf("inode %d has live slices but is not a file", inode)
		}

		paths := m.GetPaths(ctx, inode)
		sort.Strings(paths)

		chunkCount := uint64(0)
		if attr.Length > 0 {
			chunkCount = (attr.Length + meta.ChunkSize - 1) / meta.ChunkSize
		}
		for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
			var chunkSlices []meta.Slice
			if st := m.Read(ctx, inode, uint32(chunkIndex), &chunkSlices); st != 0 {
				return nil, fmt.Errorf("read inode %d chunk %d: %s", inode, chunkIndex, st)
			}
			var chunkOffset uint64
			for _, s := range chunkSlices {
				sliceChunkOffset := chunkOffset
				chunkOffset += uint64(s.Len)
				if s.Id == 0 || s.Size == 0 {
					continue
				}
				refs = append(refs, run9LiveSliceRef{
					Inode:                  uint64(inode),
					Paths:                  paths,
					ChunkIndex:             uint32(chunkIndex),
					ChunkOffsetBytes:       sliceChunkOffset,
					FileOffsetBytes:        chunkIndex*meta.ChunkSize + sliceChunkOffset,
					SliceID:                s.Id,
					SliceSizeBytes:         s.Size,
					SliceObjectOffsetBytes: s.Off,
					SliceLenBytes:          s.Len,
				})
			}
		}
	}

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Inode != refs[j].Inode {
			return refs[i].Inode < refs[j].Inode
		}
		if refs[i].ChunkIndex != refs[j].ChunkIndex {
			return refs[i].ChunkIndex < refs[j].ChunkIndex
		}
		if refs[i].ChunkOffsetBytes != refs[j].ChunkOffsetBytes {
			return refs[i].ChunkOffsetBytes < refs[j].ChunkOffsetBytes
		}
		return refs[i].SliceID < refs[j].SliceID
	})
	return refs, nil
}

func run9DescribeFormat(ctx context.Context, metaURL string) (run9DescribeFormatOutput, error) {
	metaURL = strings.TrimSpace(metaURL)
	if metaURL == "" {
		return run9DescribeFormatOutput{}, fmt.Errorf("missing meta url")
	}

	metaConf := meta.DefaultConf()
	metaConf.NoBGJob = true
	m := meta.NewClient(metaURL, metaConf)
	format, err := m.Load(true)
	if err != nil {
		_ = m.Shutdown()
		return run9DescribeFormatOutput{}, fmt.Errorf("load setting: %w", err)
	}
	if err := m.Shutdown(); err != nil {
		return run9DescribeFormatOutput{}, fmt.Errorf("shutdown meta: %w", err)
	}
	return run9DescribeFormatOutput{
		OK:                true,
		JuiceFSFormatName: format.Name,
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: format.BlockSize * 1024, HashPrefix: format.HashPrefix},
		ObjectStorage:     run9ObjectStorageDescriptorFromFormat(*format),
	}, nil
}

func run9PrepareWritableEpoch(ctx context.Context, metaURL string, ownedEpoch uint64) (run9DescribeFormatOutput, error) {
	if ownedEpoch == 0 {
		return run9DescribeFormatOutput{}, fmt.Errorf("owned epoch must be greater than 0")
	}
	nextChunkStart := ownedEpoch << 32
	return run9DescribeFormat(ctx, run9MetaURLWithNextChunk(metaURL, nextChunkStart))
}

func run9MetaURLWithNextChunk(metaURL string, nextChunkStart uint64) string {
	nextChunkText := fmt.Sprintf("%d", nextChunkStart)
	if strings.Contains(metaURL, "?") {
		return metaURL + "&nextchunk=" + url.QueryEscape(nextChunkText)
	}
	return metaURL + "?nextchunk=" + url.QueryEscape(nextChunkText)
}

func run9GCSliceRanges(ctx context.Context, req run9GCSliceRangesRequest) (run9GCSliceRangesOutput, error) {
	if strings.TrimSpace(req.JuiceFSFormatName) == "" {
		return run9GCSliceRangesOutput{}, fmt.Errorf("missing juicefs_format_name")
	}
	if req.MaxDeleteObjects == 0 {
		return run9GCSliceRangesOutput{}, fmt.Errorf("max_delete_objects must be greater than 0")
	}
	threads := req.Threads
	if threads <= 0 {
		threads = 1
	}
	for _, r := range req.Ranges {
		if r.EndInclusive < r.Start {
			return run9GCSliceRangesOutput{}, fmt.Errorf("invalid range [%d,%d]", r.Start, r.EndInclusive)
		}
	}
	if len(req.Ranges) == 0 {
		return run9GCSliceRangesOutput{OK: true}, nil
	}

	blob, err := run9SliceRangeObjectStorage(req.JuiceFSFormatName, req.ObjectLayout, req.ObjectStorage)
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)

	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	objs, scanStats, err := listRun9GCSliceRangeObjects(listCtx, blob, "chunks/", "", true)
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("list range objects: %w", err)
	}

	deletedObjects, deletedBytes, hasMore, err := deleteRun9SliceRangeMatches(ctx, cancel, blob, objs, req.ObjectLayout.HashPrefix, req.Ranges, req.MaxDeleteObjects, threads)
	<-scanStats.done
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("delete range objects: %w", err)
	}
	scanListRequests, scanListedObjects := scanStats.snapshot()
	return run9GCSliceRangesOutput{
		OK:                true,
		ScanListRequests:  scanListRequests,
		ScanListedObjects: scanListedObjects,
		DeletedObjects:    deletedObjects,
		DeletedBytes:      deletedBytes,
		HasMore:           hasMore,
	}, nil
}

func run9CountSliceRanges(ctx context.Context, req run9CountSliceRangesRequest) (run9CountSliceRangesOutput, error) {
	if strings.TrimSpace(req.JuiceFSFormatName) == "" {
		return run9CountSliceRangesOutput{}, fmt.Errorf("missing juicefs_format_name")
	}
	for _, r := range req.Ranges {
		if r.EndInclusive < r.Start {
			return run9CountSliceRangesOutput{}, fmt.Errorf("invalid range [%d,%d]", r.Start, r.EndInclusive)
		}
	}
	if len(req.Ranges) == 0 {
		return run9CountSliceRangesOutput{OK: true}, nil
	}

	blob, err := run9SliceRangeObjectStorage(req.JuiceFSFormatName, req.ObjectLayout, req.ObjectStorage)
	if err != nil {
		return run9CountSliceRangesOutput{}, fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)

	listCtx, cancel := context.WithCancel(ctx)
	objs, scanStats, err := listRun9GCSliceRangeObjects(listCtx, blob, "chunks/", "", true)
	if err != nil {
		cancel()
		return run9CountSliceRangesOutput{}, fmt.Errorf("list range objects: %w", err)
	}

	objects, bytes, ranges, err := countRun9SliceRangeMatches(ctx, objs, req.ObjectLayout.HashPrefix, req.Ranges)
	cancel()
	<-scanStats.done
	if err != nil {
		return run9CountSliceRangesOutput{}, fmt.Errorf("count range objects: %w", err)
	}
	scanListRequests, scanListedObjects := scanStats.snapshot()
	return run9CountSliceRangesOutput{
		OK:                true,
		ScanListRequests:  scanListRequests,
		ScanListedObjects: scanListedObjects,
		Objects:           objects,
		Bytes:             bytes,
		Ranges:            ranges,
	}, nil
}

func run9SliceRangeObjectStorage(formatName string, layout run9ObjectLayout, descriptor run9ObjectStorageDescriptor) (object.ObjectStorage, error) {
	if err := layout.validate(); err != nil {
		return nil, err
	}
	if err := descriptor.validate(); err != nil {
		return nil, err
	}
	if layout.BlockSizeBytes%1024 != 0 {
		return nil, fmt.Errorf("object_layout.block_size_bytes must be KiB-aligned")
	}
	format := descriptor.toFormat(formatName)
	format.BlockSize = layout.BlockSizeBytes / 1024
	format.HashPrefix = layout.HashPrefix
	if format.BlockSize*1024 != layout.BlockSizeBytes || format.HashPrefix != layout.HashPrefix {
		return nil, fmt.Errorf("object layout mismatch with descriptor")
	}
	return createStorage(format)
}

type run9CountSliceRangeAccumulator struct {
	run9CountSliceRangeAccount
	requestIndex int
}

func countRun9SliceRangeMatches(ctx context.Context, objs <-chan object.Object, hashPrefix bool, ranges []run9GCSliceRange) (uint64, uint64, []run9CountSliceRangeAccount, error) {
	accumulators := make([]run9CountSliceRangeAccumulator, 0, len(ranges))
	for i, r := range ranges {
		if r.EndInclusive < r.Start {
			return 0, 0, nil, fmt.Errorf("invalid range [%d,%d]", r.Start, r.EndInclusive)
		}
		accumulators = append(accumulators, run9CountSliceRangeAccumulator{
			run9CountSliceRangeAccount: run9CountSliceRangeAccount{
				SnapID:       r.SnapID,
				Start:        r.Start,
				EndInclusive: r.EndInclusive,
			},
			requestIndex: i,
		})
	}
	sort.Slice(accumulators, func(i, j int) bool {
		if accumulators[i].Start != accumulators[j].Start {
			return accumulators[i].Start < accumulators[j].Start
		}
		return accumulators[i].EndInclusive < accumulators[j].EndInclusive
	})
	for i := 1; i < len(accumulators); i++ {
		if accumulators[i].Start <= accumulators[i-1].EndInclusive {
			return 0, 0, nil, fmt.Errorf("overlapping ranges [%d,%d] and [%d,%d]", accumulators[i-1].Start, accumulators[i-1].EndInclusive, accumulators[i].Start, accumulators[i].EndInclusive)
		}
	}

	var objects uint64
	var bytes uint64
	for {
		select {
		case <-ctx.Done():
			return 0, 0, nil, ctx.Err()
		case obj, ok := <-objs:
			if !ok {
				out := make([]run9CountSliceRangeAccount, len(accumulators))
				for _, acc := range accumulators {
					out[acc.requestIndex] = acc.run9CountSliceRangeAccount
				}
				return objects, bytes, out, nil
			}
			if obj == nil {
				return 0, 0, nil, fmt.Errorf("list range objects returned out-of-order results")
			}
			if obj.IsDir() {
				continue
			}
			block, ok := chunk.ParseObjectBlockKey(obj.Key(), hashPrefix)
			if !ok {
				continue
			}
			accumulatorIndex := run9CountSliceRangeAccumulatorIndex(accumulators, block.SliceID)
			if accumulatorIndex < 0 {
				continue
			}
			objects++
			size := uint64(obj.Size())
			bytes += size
			accumulators[accumulatorIndex].Objects++
			accumulators[accumulatorIndex].Bytes += size
		}
	}
}

func run9CountSliceRangeAccumulatorIndex(ranges []run9CountSliceRangeAccumulator, sliceID uint64) int {
	i := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].EndInclusive >= sliceID
	})
	if i >= len(ranges) || ranges[i].Start > sliceID {
		return -1
	}
	return i
}

func listRun9GCSliceRangeObjects(ctx context.Context, store object.ObjectStorage, prefix, marker string, followLink bool) (<-chan object.Object, *run9GCSliceRangeScanStats, error) {
	scanStats := newRun9GCSliceRangeScanStats()
	objs, hasMore, nextToken, err := store.List(ctx, prefix, marker, "", "", run9GCSliceRangesListPageSize, followLink)
	if err != nil {
		return listRun9GCSliceRangeObjectsFallback(ctx, scanStats, store, prefix, marker, followLink)
	}
	scanStats.record(len(objs))

	out := make(chan object.Object, run9GCSliceRangesListPageSize)
	go func() {
		defer close(out)
		defer close(scanStats.done)

		lastKey := marker
		for {
			for _, obj := range objs {
				lastKey = obj.Key()
				select {
				case <-ctx.Done():
					return
				case out <- obj:
				}
			}
			if !hasMore {
				return
			}

			marker = lastKey
			for {
				var nextToken2 string
				objs, hasMore, nextToken2, err = store.List(ctx, prefix, marker, nextToken, "", run9GCSliceRangesListPageSize, followLink)
				scanStats.record(len(objs))
				if err == nil {
					nextToken = nextToken2
					break
				}
				if ctx.Err() != nil {
					return
				}
				timer := time.NewTimer(100 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}()
	return out, scanStats, nil
}

func listRun9GCSliceRangeObjectsFallback(ctx context.Context, scanStats *run9GCSliceRangeScanStats, store object.ObjectStorage, prefix, marker string, followLink bool) (<-chan object.Object, *run9GCSliceRangeScanStats, error) {
	objs, err := object.ListAll(ctx, store, prefix, marker, followLink, false)
	if err != nil {
		close(scanStats.done)
		return nil, nil, err
	}
	scanStats.record(0)

	out := make(chan object.Object, run9GCSliceRangesListPageSize)
	go func() {
		defer close(out)
		defer close(scanStats.done)
		for obj := range objs {
			if obj != nil {
				scanStats.listedObjects.Add(1)
			}
			select {
			case <-ctx.Done():
				return
			case out <- obj:
			}
		}
	}()
	return out, scanStats, nil
}

func deleteRun9SliceRangeMatches(
	ctx context.Context,
	stopScan context.CancelFunc,
	store object.ObjectStorage,
	objs <-chan object.Object,
	hashPrefix bool,
	ranges []run9GCSliceRange,
	maxDeleteObjects uint64,
	threads int,
) (uint64, uint64, bool, error) {
	if maxDeleteObjects == 0 {
		return 0, 0, false, fmt.Errorf("max_delete_objects must be greater than 0")
	}
	if threads <= 0 {
		threads = 1
	}

	scanStopped := false
	stopListing := func() {
		if scanStopped {
			return
		}
		scanStopped = true
		stopScan()
	}
	defer stopListing()

	batchSize := run9GCDeleteBatchSizeForSliceRanges(store, maxDeleteObjects, threads)
	deleteCtx, cancelDelete := context.WithCancel(ctx)
	defer cancelDelete()

	deleteJobs := make(chan []run9GCExactObject, threads*2)
	supportsBulkDelete := object.SupportsBulkDelete(store)
	var deletedObjects atomic.Uint64
	var deletedBytes atomic.Uint64
	var firstDeleteErr error
	var firstDeleteErrMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range deleteJobs {
				firstDeleteErrMu.Lock()
				hasDeleteErr := firstDeleteErr != nil
				firstDeleteErrMu.Unlock()
				if hasDeleteErr {
					continue
				}

				batchDeletedObjects, batchDeletedBytes, err := deleteRun9SliceRangeDeleteBatch(deleteCtx, store, batch, supportsBulkDelete)
				if err != nil {
					firstDeleteErrMu.Lock()
					if firstDeleteErr == nil {
						firstDeleteErr = err
						cancelDelete()
						stopListing()
					}
					firstDeleteErrMu.Unlock()
					continue
				}
				deletedObjects.Add(batchDeletedObjects)
				deletedBytes.Add(batchDeletedBytes)
			}
		}()
	}

	sendBatch := func(batch []run9GCExactObject) error {
		if len(batch) == 0 {
			return nil
		}
		firstDeleteErrMu.Lock()
		hasDeleteErr := firstDeleteErr != nil
		err := firstDeleteErr
		firstDeleteErrMu.Unlock()
		if hasDeleteErr {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case deleteJobs <- batch:
			return nil
		}
	}

	matches := make([]run9GCExactObject, 0, batchSize)
	var matchedForDelete uint64
	var hasMore bool
	for obj := range objs {
		if obj == nil {
			close(deleteJobs)
			wg.Wait()
			return 0, 0, false, fmt.Errorf("list range objects returned out-of-order results")
		}
		if obj.IsDir() {
			continue
		}
		block, ok := chunk.ParseObjectBlockKey(obj.Key(), hashPrefix)
		if !ok || !sliceIDInRanges(block.SliceID, ranges) {
			continue
		}
		if matchedForDelete >= maxDeleteObjects {
			hasMore = true
			stopListing()
			break
		}

		matches = append(matches, run9GCExactObject{
			Key:  obj.Key(),
			Size: uint64(obj.Size()),
		})
		matchedForDelete++
		if len(matches) >= batchSize || matchedForDelete == maxDeleteObjects {
			batch := matches
			matches = make([]run9GCExactObject, 0, batchSize)
			if err := sendBatch(batch); err != nil {
				close(deleteJobs)
				wg.Wait()
				if err == nil {
					err = firstDeleteErr
				}
				return 0, 0, false, err
			}
		}
	}
	if err := sendBatch(matches); err != nil {
		close(deleteJobs)
		wg.Wait()
		if err == nil {
			err = firstDeleteErr
		}
		return 0, 0, false, err
	}
	close(deleteJobs)
	wg.Wait()

	firstDeleteErrMu.Lock()
	err := firstDeleteErr
	firstDeleteErrMu.Unlock()
	if err != nil {
		return 0, 0, false, err
	}
	if ctx.Err() != nil {
		return 0, 0, false, ctx.Err()
	}
	return deletedObjects.Load(), deletedBytes.Load(), hasMore, nil
}

func run9GCDeleteBatchSizeForSliceRanges(store object.ObjectStorage, maxDeleteObjects uint64, threads int) int {
	if maxDeleteObjects == 0 {
		return 1
	}
	if threads <= 0 {
		threads = 1
	}
	maxBatchObjects := maxDeleteObjects
	if maxBatchObjects > uint64(^uint(0)>>1) {
		maxBatchObjects = uint64(^uint(0) >> 1)
	}
	if object.SupportsBulkDelete(store) {
		return run9GCExactObjectsBulkDeleteBatchSize(int(maxBatchObjects), threads)
	}
	return 1
}

func deleteRun9SliceRangeDeleteBatch(ctx context.Context, store object.ObjectStorage, objects []run9GCExactObject, supportsBulkDelete bool) (uint64, uint64, error) {
	if len(objects) == 0 {
		return 0, 0, nil
	}
	if supportsBulkDelete {
		bulk, ok := store.(run9GCExactObjectsBulkDeleteStorage)
		if !ok {
			return 0, 0, fmt.Errorf("bulk delete unsupported by %T", store)
		}
		keys := make([]string, 0, len(objects))
		var deletedBytes uint64
		for _, obj := range objects {
			keys = append(keys, obj.Key)
			deletedBytes += obj.Size
		}
		if err := deleteRun9ExactObjectBatch(ctx, bulk, keys); err != nil {
			return 0, 0, err
		}
		return uint64(len(objects)), deletedBytes, nil
	}

	var deletedBytes uint64
	for _, obj := range objects {
		if err := deleteRun9ExactObjectWithRetry(ctx, store, obj.Key); err != nil {
			return 0, 0, err
		}
		deletedBytes += obj.Size
	}
	return uint64(len(objects)), deletedBytes, nil
}

func sliceIDInRanges(sliceID uint64, ranges []run9GCSliceRange) bool {
	for _, r := range ranges {
		if sliceID < r.Start {
			continue
		}
		if sliceID <= r.EndInclusive {
			return true
		}
	}
	return false
}

type run9GCExactObjectsBulkDeleteStorage interface {
	DeleteObjects(ctx context.Context, keys []string, getters ...object.AttrGetter) error
}

func deleteRun9ExactObjects(ctx context.Context, store object.ObjectStorage, objects []run9GCExactObject, threads int) (uint64, uint64, error) {
	if threads <= 0 {
		threads = 1
	}
	if object.SupportsBulkDelete(store) {
		bulk, ok := store.(run9GCExactObjectsBulkDeleteStorage)
		if !ok {
			return 0, 0, fmt.Errorf("bulk delete unsupported by %T", store)
		}
		return deleteRun9ExactObjectsBulk(ctx, bulk, objects, threads)
	}
	return deleteRun9ExactObjectsSequential(ctx, store, objects, threads)
}

func deleteRun9ExactObjectsBulk(ctx context.Context, store run9GCExactObjectsBulkDeleteStorage, objects []run9GCExactObject, threads int) (uint64, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	batchSize := run9GCExactObjectsBulkDeleteBatchSize(len(objects), threads)
	deleteJobs := make(chan []run9GCExactObject)
	var deletedObjects atomic.Uint64
	var deletedBytes atomic.Uint64
	var firstDeleteErr error
	var firstDeleteErrMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range deleteJobs {
				firstDeleteErrMu.Lock()
				hasDeleteErr := firstDeleteErr != nil
				firstDeleteErrMu.Unlock()
				if hasDeleteErr {
					continue
				}

				keys := make([]string, 0, len(batch))
				var batchBytes uint64
				for _, obj := range batch {
					keys = append(keys, obj.Key)
					batchBytes += obj.Size
				}
				if err := deleteRun9ExactObjectBatch(ctx, store, keys); err != nil {
					firstDeleteErrMu.Lock()
					if firstDeleteErr == nil {
						firstDeleteErr = err
						cancel()
					}
					firstDeleteErrMu.Unlock()
					continue
				}
				deletedObjects.Add(uint64(len(keys)))
				deletedBytes.Add(batchBytes)
			}
		}()
	}
	for start := 0; start < len(objects); start += batchSize {
		firstDeleteErrMu.Lock()
		hasDeleteErr := firstDeleteErr != nil
		firstDeleteErrMu.Unlock()
		if hasDeleteErr {
			break
		}
		end := start + batchSize
		if end > len(objects) {
			end = len(objects)
		}
		select {
		case <-ctx.Done():
			close(deleteJobs)
			wg.Wait()
			if firstDeleteErr != nil {
				return 0, 0, firstDeleteErr
			}
			return 0, 0, ctx.Err()
		case deleteJobs <- objects[start:end]:
		}
	}
	close(deleteJobs)
	wg.Wait()
	if firstDeleteErr != nil {
		return 0, 0, firstDeleteErr
	}
	return deletedObjects.Load(), deletedBytes.Load(), nil
}

func deleteRun9ExactObjectBatch(ctx context.Context, store run9GCExactObjectsBulkDeleteStorage, keys []string) error {
	delay := run9GCExactObjectsInitialRetryDelay
	var err error
	for attempt := 1; attempt <= run9GCExactObjectsDeleteAttempts; attempt++ {
		err = store.DeleteObjects(ctx, keys)
		if err == nil {
			return nil
		}
		if !isRun9GCExactObjectsRetryableDeleteError(err) {
			break
		}
		if attempt == run9GCExactObjectsDeleteAttempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = nextRun9GCExactObjectsRetryDelay(delay)
	}
	return err
}

func nextRun9GCExactObjectsRetryDelay(delay time.Duration) time.Duration {
	delay *= 2
	if delay > run9GCExactObjectsMaxRetryDelay {
		return run9GCExactObjectsMaxRetryDelay
	}
	return delay
}

func isRun9GCExactObjectsRetryableDeleteError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "slowdown") ||
		strings.Contains(msg, "throttl") ||
		strings.Contains(msg, "internalerror") ||
		strings.Contains(msg, "internal error") ||
		strings.Contains(msg, "requesttimeout") ||
		strings.Contains(msg, "serviceunavailable") ||
		strings.Contains(msg, "service unavailable") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "exceeded maximum number of attempts")
}

func deleteRun9ExactObjectsSequential(ctx context.Context, store object.ObjectStorage, objects []run9GCExactObject, threads int) (uint64, uint64, error) {
	deleteJobs := make(chan run9GCExactObject)
	var deletedObjects atomic.Uint64
	var deletedBytes atomic.Uint64
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
				if err := deleteRun9ExactObjectWithRetry(ctx, store, obj.Key); err != nil {
					firstDeleteErrMu.Lock()
					if firstDeleteErr == nil {
						firstDeleteErr = err
					}
					firstDeleteErrMu.Unlock()
					continue
				}
				deletedObjects.Add(1)
				deletedBytes.Add(obj.Size)
			}
		}()
	}
	for _, obj := range objects {
		firstDeleteErrMu.Lock()
		hasDeleteErr := firstDeleteErr != nil
		firstDeleteErrMu.Unlock()
		if hasDeleteErr {
			break
		}
		select {
		case <-ctx.Done():
			close(deleteJobs)
			wg.Wait()
			return 0, 0, ctx.Err()
		case deleteJobs <- obj:
		}
	}
	close(deleteJobs)
	wg.Wait()
	if firstDeleteErr != nil {
		return 0, 0, firstDeleteErr
	}
	return deletedObjects.Load(), deletedBytes.Load(), nil
}

func deleteRun9ExactObjectWithRetry(ctx context.Context, store object.ObjectStorage, key string) error {
	delay := run9GCExactObjectsInitialRetryDelay
	var err error
	for attempt := 1; attempt <= run9GCExactObjectsDeleteAttempts; attempt++ {
		err = store.Delete(ctx, key)
		if err == nil {
			return nil
		}
		if !isRun9GCExactObjectsRetryableDeleteError(err) {
			break
		}
		if attempt == run9GCExactObjectsDeleteAttempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = nextRun9GCExactObjectsRetryDelay(delay)
	}
	return err
}

func (l run9ObjectLayout) validate() error {
	if l.BlockSizeBytes <= 0 {
		return fmt.Errorf("object_layout.block_size_bytes must be greater than 0")
	}
	return nil
}

func (d run9ObjectStorageDescriptor) validate() error {
	if strings.TrimSpace(d.Storage) == "" {
		return fmt.Errorf("object_storage.storage must not be empty")
	}
	if strings.TrimSpace(d.Bucket) == "" {
		return fmt.Errorf("object_storage.bucket must not be empty")
	}
	if d.SecretKey == "removed" || d.SessionToken == "removed" || d.EncryptKey == "removed" {
		return fmt.Errorf("object_storage contains removed secret")
	}
	return nil
}

func run9ObjectStorageDescriptorFromFormat(format meta.Format) run9ObjectStorageDescriptor {
	return run9ObjectStorageDescriptor{
		Storage:      format.Storage,
		Bucket:       format.Bucket,
		UUID:         format.UUID,
		AccessKey:    format.AccessKey,
		SecretKey:    format.SecretKey,
		SessionToken: format.SessionToken,
		StorageClass: format.StorageClass,
		Shards:       format.Shards,
		EncryptKey:   format.EncryptKey,
		EncryptAlgo:  format.EncryptAlgo,
		KeyEncrypted: format.KeyEncrypted,
	}
}

func (d run9ObjectStorageDescriptor) toFormat(name string) meta.Format {
	return meta.Format{
		Name:         name,
		UUID:         d.UUID,
		Storage:      d.Storage,
		Bucket:       d.Bucket,
		AccessKey:    d.AccessKey,
		SecretKey:    d.SecretKey,
		SessionToken: d.SessionToken,
		StorageClass: d.StorageClass,
		Shards:       d.Shards,
		EncryptKey:   d.EncryptKey,
		EncryptAlgo:  d.EncryptAlgo,
		KeyEncrypted: d.KeyEncrypted,
	}
}
