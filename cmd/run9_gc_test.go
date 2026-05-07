package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9GCExactObjectsAcceptsEmptyBatch(t *testing.T) {
	out, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  t.TempDir(),
		},
		Threads: 1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Zero(t, out.DeletedObjects)
	require.Zero(t, out.DeletedBytes)
}

func TestRun9GCExactObjectsDeletesExactFileObject(t *testing.T) {
	bucket := t.TempDir()
	key := "chunks/0/0/1_0_3"
	objectPath := filepath.Join(bucket, "fmtroot", key)
	require.NoError(t, os.MkdirAll(filepath.Dir(objectPath), 0o755))
	require.NoError(t, os.WriteFile(objectPath, []byte("abc"), 0o644))

	out, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Objects: []run9GCExactObject{{Key: key, Size: 3}},
		Threads: 1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.DeletedObjects)
	require.Equal(t, uint64(3), out.DeletedBytes)
	require.NoFileExists(t, objectPath)
}

func TestRun9GCExactObjectsRejectsRemovedSecret(t *testing.T) {
	_, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage:   "s3",
			Bucket:    "s3://bucket",
			SecretKey: "removed",
		},
		Objects: []run9GCExactObject{{Key: "chunks/0/0/1_0_1", Size: 1}},
		Threads: 1,
	})

	require.ErrorContains(t, err, "removed secret")
}
