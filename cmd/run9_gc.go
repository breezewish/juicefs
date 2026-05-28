package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
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

type run9ListLiveSlicesOutput struct {
	OK                bool             `json:"ok"`
	JuiceFSFormatName string           `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout `json:"object_layout"`
	Slices            []run9LiveSlice  `json:"slices"`
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

type run9GCExactObjectsRequest struct {
	JuiceFSFormatName string                      `json:"juicefs_format_name"`
	ObjectLayout      run9ObjectLayout            `json:"object_layout"`
	ObjectStorage     run9ObjectStorageDescriptor `json:"object_storage"`
	Objects           []run9GCExactObject         `json:"objects"`
	Threads           int                         `json:"threads"`
}

type run9GCExactObjectsOutput struct {
	OK             bool   `json:"ok"`
	DeletedObjects uint64 `json:"deleted_objects"`
	DeletedBytes   uint64 `json:"deleted_bytes"`
}

type run9GCSliceRange struct {
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
	OK             bool   `json:"ok"`
	DeletedObjects uint64 `json:"deleted_objects"`
	DeletedBytes   uint64 `json:"deleted_bytes"`
	HasMore        bool   `json:"has_more"`
}

const (
	run9GCExactObjectsMaxBulkDeleteBatchSize = 1000
	run9GCExactObjectsDeleteAttempts         = 12
	run9GCExactObjectsInitialRetryDelay      = 500 * time.Millisecond
	run9GCExactObjectsMaxRetryDelay          = 10 * time.Second
)

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
		},
		Action: func(ctx *cli.Context) error {
			setup(ctx, 1)
			out, err := run9ListSlices(ctx.Context, ctx.Args().Get(0), ctx.Bool("scan-pending"))
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

func cmdRun9GCExactObjects() *cli.Command {
	return &cli.Command{
		Name:   "gc-exact-objects",
		Hidden: true,
		Usage:  "run9 internal: delete exact object keys",
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
			var req run9GCExactObjectsRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return fmt.Errorf("parse request: %w", err)
			}
			out, err := run9GCExactObjects(ctx.Context, req)
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

func run9ListLiveSlices(ctx context.Context, metaURL string) (run9ListLiveSlicesOutput, error) {
	return run9ListSlices(ctx, metaURL, false)
}

func run9ListSlices(ctx context.Context, metaURL string, scanPending bool) (run9ListLiveSlicesOutput, error) {
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

func run9GCExactObjects(ctx context.Context, req run9GCExactObjectsRequest) (run9GCExactObjectsOutput, error) {
	if strings.TrimSpace(req.JuiceFSFormatName) == "" {
		return run9GCExactObjectsOutput{}, fmt.Errorf("missing juicefs_format_name")
	}
	if err := req.ObjectLayout.validate(); err != nil {
		return run9GCExactObjectsOutput{}, err
	}
	if err := req.ObjectStorage.validate(); err != nil {
		return run9GCExactObjectsOutput{}, err
	}
	threads := req.Threads
	if threads <= 0 {
		threads = 1
	}
	for _, obj := range req.Objects {
		if err := validateRun9ExactObjectKey(obj.Key); err != nil {
			return run9GCExactObjectsOutput{}, err
		}
	}
	if len(req.Objects) == 0 {
		return run9GCExactObjectsOutput{OK: true}, nil
	}

	if req.ObjectLayout.BlockSizeBytes%1024 != 0 {
		return run9GCExactObjectsOutput{}, fmt.Errorf("object_layout.block_size_bytes must be KiB-aligned")
	}
	format := req.ObjectStorage.toFormat(req.JuiceFSFormatName)
	format.BlockSize = req.ObjectLayout.BlockSizeBytes / 1024
	format.HashPrefix = req.ObjectLayout.HashPrefix
	if format.BlockSize*1024 != req.ObjectLayout.BlockSizeBytes || format.HashPrefix != req.ObjectLayout.HashPrefix {
		return run9GCExactObjectsOutput{}, fmt.Errorf("object layout mismatch with descriptor")
	}
	blob, err := createStorage(format)
	if err != nil {
		return run9GCExactObjectsOutput{}, fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(ctx, blob, req.Objects, threads)
	if err != nil {
		return run9GCExactObjectsOutput{}, fmt.Errorf("delete exact object: %w", err)
	}
	return run9GCExactObjectsOutput{
		OK:             true,
		DeletedObjects: deletedObjects,
		DeletedBytes:   deletedBytes,
	}, nil
}

func run9GCSliceRanges(ctx context.Context, req run9GCSliceRangesRequest) (run9GCSliceRangesOutput, error) {
	if strings.TrimSpace(req.JuiceFSFormatName) == "" {
		return run9GCSliceRangesOutput{}, fmt.Errorf("missing juicefs_format_name")
	}
	if err := req.ObjectLayout.validate(); err != nil {
		return run9GCSliceRangesOutput{}, err
	}
	if err := req.ObjectStorage.validate(); err != nil {
		return run9GCSliceRangesOutput{}, err
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

	if req.ObjectLayout.BlockSizeBytes%1024 != 0 {
		return run9GCSliceRangesOutput{}, fmt.Errorf("object_layout.block_size_bytes must be KiB-aligned")
	}
	format := req.ObjectStorage.toFormat(req.JuiceFSFormatName)
	format.BlockSize = req.ObjectLayout.BlockSizeBytes / 1024
	format.HashPrefix = req.ObjectLayout.HashPrefix
	if format.BlockSize*1024 != req.ObjectLayout.BlockSizeBytes || format.HashPrefix != req.ObjectLayout.HashPrefix {
		return run9GCSliceRangesOutput{}, fmt.Errorf("object layout mismatch with descriptor")
	}
	blob, err := createStorage(format)
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)

	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	objs, err := object.ListAll(listCtx, blob, "chunks/", "", true, false)
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("list range objects: %w", err)
	}

	matches := make([]run9GCExactObject, 0, minInt(int(req.MaxDeleteObjects), 1024))
	var hasMore bool
	for obj := range objs {
		if obj == nil {
			return run9GCSliceRangesOutput{}, fmt.Errorf("list range objects returned out-of-order results")
		}
		if obj.IsDir() {
			continue
		}
		block, ok := chunk.ParseObjectBlockKey(obj.Key(), req.ObjectLayout.HashPrefix)
		if !ok || !sliceIDInRanges(block.SliceID, req.Ranges) {
			continue
		}
		matches = append(matches, run9GCExactObject{
			Key:  obj.Key(),
			Size: uint64(obj.Size()),
		})
		if uint64(len(matches)) >= req.MaxDeleteObjects {
			hasMore = true
			cancel()
			break
		}
	}
	if len(matches) == 0 {
		return run9GCSliceRangesOutput{OK: true, HasMore: hasMore}, nil
	}

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(ctx, blob, matches, threads)
	if err != nil {
		return run9GCSliceRangesOutput{}, fmt.Errorf("delete range objects: %w", err)
	}
	return run9GCSliceRangesOutput{
		OK:             true,
		DeletedObjects: deletedObjects,
		DeletedBytes:   deletedBytes,
		HasMore:        hasMore,
	}, nil
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

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
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

func validateRun9ExactObjectKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("object key must not be empty")
	}
	if key != strings.TrimSpace(key) || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || path.Clean(key) != key {
		return fmt.Errorf("object key must be a normalized relative path: %q", key)
	}
	if !strings.HasPrefix(key, "chunks/") {
		return fmt.Errorf("object key must be under chunks/: %q", key)
	}
	return nil
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
