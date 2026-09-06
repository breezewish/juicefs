package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9RetiredSlicesCapturesNewAllocationWindowOnRemount(t *testing.T) {
	url := "badger://" + t.TempDir()
	conf := meta.DefaultConf()
	conf.MaxDeletes = 0
	m := meta.NewClient(url, conf)
	format := &meta.Format{Name: "test", UUID: "volume", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4}
	require.NoError(t, m.Init(format, true))
	require.NoError(t, m.Shutdown())
	_, err := run9PrepareWritableEpoch(context.Background(), url, 7)
	require.NoError(t, err)
	t.Setenv("JFS_RUN9_RETIRED_SLICES_DIR", t.TempDir())
	t.Setenv("JFS_RUN9_OWNED_EPOCH", "7")
	m = meta.NewClient(url, conf)
	gc, err := beginRun9RetiredSliceGC(m, format)
	require.NoError(t, err)
	require.EqualValues(t, 7<<32, gc.batch.Start)
	var id uint64
	require.Zero(t, m.NewSlice(meta.Background(), &id))
	require.EqualValues(t, 7<<32, id)
	require.NoError(t, m.Shutdown())
	// The next mount reads the advanced reservation boundary, not the epoch base.
	m = meta.NewClient(url, conf)
	defer func() { require.NoError(t, m.Shutdown()) }()
	gc, err = beginRun9RetiredSliceGC(m, format)
	require.NoError(t, err)
	require.EqualValues(t, 7<<32+4096, gc.batch.Start)
	require.Zero(t, m.NewSlice(meta.Background(), &id))
	require.EqualValues(t, gc.batch.Start, id)
	t.Setenv("JFS_RUN9_OWNED_EPOCH", "8")
	_, err = beginRun9RetiredSliceGC(m, format)
	require.ErrorContains(t, err, "outside owned epoch")
}

func TestRun9RetiredSlicesDeletesExactKeysAndPreservesOtherEpochs(t *testing.T) {
	bucket, queue := t.TempDir(), t.TempDir()
	gc := &run9RetiredSliceGC{dir: queue, batch: run9RetiredSliceBatch{
		Version: 1, Format: "lineage", Storage: run9ObjectStorageDescriptor{Storage: "file", Bucket: bucket + "/", UUID: "volume"},
		Layout: run9ObjectLayout{BlockSizeBytes: 4096, HashPrefix: true}, OwnedEpoch: 1, Start: 1 << 32, End: 1<<32 + 4096,
		Slices: []run9LiveSlice{{ID: 1 << 32, Size: 5000}},
	}}
	first := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32, 0, 4096, true))
	last := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32, 1, 904, true))
	inherited := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(2<<32, 0, 4096, true))
	otherVolume := filepath.Join(bucket, "other", chunk.FormatObjectBlockKey(1<<32, 0, 4096, true))
	require.NoError(t, os.MkdirAll(filepath.Dir(first), 0700))
	require.NoError(t, os.WriteFile(first, make([]byte, 4096), 0600))
	require.NoError(t, os.WriteFile(last, make([]byte, 904), 0600))
	require.NoError(t, os.MkdirAll(filepath.Dir(inherited), 0700))
	require.NoError(t, os.WriteFile(inherited, []byte("inherited"), 0600))
	require.NoError(t, os.MkdirAll(filepath.Dir(otherVolume), 0700))
	require.NoError(t, os.WriteFile(otherVolume, []byte("another volume"), 0600))
	require.NoError(t, gc.publish())
	entries, err := os.ReadDir(queue)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	batchPath := filepath.Join(queue, entries[0].Name())
	raw, err := os.ReadFile(batchPath)
	require.NoError(t, err)
	// Corruption cannot authorize DELETE, even when the file is valid JSON.
	require.NoError(t, os.WriteFile(batchPath, append(raw, ' '), 0600))
	out, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.False(t, out.OK)
	require.Contains(t, out.Error, "checksum")
	require.FileExists(t, first)
	require.NoError(t, os.WriteFile(batchPath, raw, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(queue, ".tmp-retired-crashed"), raw, 0600))
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, out.OK)
	require.EqualValues(t, 2, out.DeletedObjects)
	require.EqualValues(t, 5000, out.DeletedLogicalBytes)
	require.NoFileExists(t, first)
	require.NoFileExists(t, last)
	require.FileExists(t, inherited)
	require.FileExists(t, otherVolume)
	require.NoFileExists(t, batchPath)
	// Lost completion / concurrent coarse GC: deleting already missing objects succeeds.
	require.NoError(t, gc.publish())
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, 1, out.CompletedBatches)
	gc.batch.Slices[0].ID = 2 << 32
	require.ErrorContains(t, gc.publish(), "invalid retired slice")
}

func TestRun9RetiredSlicesFailedDeleteRetainsBatchForRetry(t *testing.T) {
	bucket, queue := t.TempDir(), t.TempDir()
	gc := &run9RetiredSliceGC{dir: queue, batch: run9RetiredSliceBatch{
		Version: 1, Format: "lineage", Storage: run9ObjectStorageDescriptor{Storage: "file", Bucket: bucket + "/", UUID: "volume"},
		Layout: run9ObjectLayout{BlockSizeBytes: 4096}, OwnedEpoch: 1, Start: 1 << 32, End: 1<<32 + 4096,
		Slices: []run9LiveSlice{{ID: 1 << 32, Size: 4096}},
	}}
	key := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32, 0, 4096, false))
	// A nonempty directory makes the file backend reject DELETE, even as root.
	require.NoError(t, os.MkdirAll(key, 0700))
	obstacle := filepath.Join(key, "not-an-object")
	require.NoError(t, os.WriteFile(obstacle, nil, 0600))
	require.NoError(t, gc.publish())
	batchPaths, err := filepath.Glob(filepath.Join(queue, "*.json"))
	require.NoError(t, err)
	require.Len(t, batchPaths, 1)
	// A healthy batch behind a failed one can still finish.
	gc.batch.Slices = []run9LiveSlice{{ID: 1<<32 + 1, Size: 4096}}
	require.NoError(t, gc.publish())
	out, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.False(t, out.OK)
	require.Equal(t, 1, out.FailedBatches)
	require.Equal(t, 1, out.CompletedBatches)
	require.FileExists(t, batchPaths[0])
	require.NoError(t, os.Remove(obstacle))
	require.NoError(t, os.Remove(key))
	require.NoError(t, os.WriteFile(key, make([]byte, 4096), 0600))
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, 1, out.CompletedBatches)
	require.NoFileExists(t, key)
	require.NoFileExists(t, batchPaths[0])
}
