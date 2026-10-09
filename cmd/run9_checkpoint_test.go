package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9GCCheckpointCorruptionDeletesNothingAndHonorsBounds(t *testing.T) {
	dir, bucket := t.TempDir(), t.TempDir()
	m := meta.NewClient("badger://"+dir, meta.DefaultConf())
	require.NoError(t, m.Init(&meta.Format{Name: "snap", Storage: "file", Bucket: bucket + "/", BlockSize: 4, TrashDays: 0}, true))
	require.NoError(t, m.Shutdown())
	writeMetadata := func(corrupt bool) {
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *badger.Txn) error {
			value := make([]byte, 8)
			binary.LittleEndian.PutUint64(value, 20)
			if err := tx.Set([]byte("CnextChunk"), value); err != nil {
				return err
			}
			key := make([]byte, 14)
			key[0] = 'A'
			binary.LittleEndian.PutUint64(key[1:9], 2)
			key[9] = 'C'
			if corrupt {
				return tx.Set(key, []byte{1})
			}
			return tx.Delete(key)
		}))
		require.NoError(t, db.Close())
	}
	writeMetadata(true)
	paths := map[uint64]string{}
	for _, id := range []uint64{9, 10, 19, 20} {
		p := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, false))
		paths[id] = p
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0700))
		require.NoError(t, os.WriteFile(p, []byte("abc"), 0600))
	}
	req := run9GCCheckpointRequest{Directory: dir, Format: "snap", AllocationEnd: 20, Ranges: []run9GCSliceRange{{Start: 10, EndInclusive: 19}}}
	_, err := run9GCCheckpoint(context.Background(), req)
	require.ErrorContains(t, err, "malformed")
	for _, p := range paths {
		require.FileExists(t, p)
	}
	empty := req
	empty.Ranges = nil
	out, err := run9GCCheckpoint(context.Background(), empty)
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Zero(t, out.DeletedObjects)
	writeMetadata(false)
	req.DryRun = true
	out, err = run9GCCheckpoint(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uint64(2), out.CandidateObjects)
	require.Zero(t, out.DeletedObjects)
	for _, p := range paths {
		require.FileExists(t, p)
	}
	req.DryRun = false
	out, err = run9GCCheckpoint(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uint64(2), out.DeletedObjects)
	require.FileExists(t, paths[9])
	require.NoFileExists(t, paths[10])
	require.NoFileExists(t, paths[19])
	require.FileExists(t, paths[20])
	out, err = run9GCCheckpoint(context.Background(), req)
	require.NoError(t, err)
	require.Zero(t, out.DeletedObjects)
}

func TestRun9GCSliceRangesRetainsLiveIDsAndResumesBudget(t *testing.T) {
	bucket := t.TempDir()
	paths := map[uint64]string{}
	for _, id := range []uint64{10, 11, 12} {
		p := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, true))
		paths[id] = p
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0700))
		require.NoError(t, os.WriteFile(p, []byte("abc"), 0600))
	}
	req := run9GCSliceRangesRequest{JuiceFSFormatName: "snap", ObjectLayout: run9ObjectLayout{4096, true}, ObjectStorage: run9ObjectStorageDescriptor{Storage: "file", Bucket: bucket + "/"}, Ranges: []run9GCSliceRange{{Start: 10, EndInclusive: 12}}, RetainSliceIDs: map[uint64]bool{11: true}, MaxDeleteObjects: 1, Threads: 4}
	first, err := run9GCSliceRanges(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.HasMore)
	require.Equal(t, uint64(1), first.DeletedObjects)
	require.NotEmpty(t, first.Next)
	req.After = first.Next
	next, err := run9GCSliceRanges(context.Background(), req)
	require.NoError(t, err)
	require.False(t, next.HasMore)
	require.Equal(t, uint64(1), next.DeletedObjects)
	require.FileExists(t, paths[11])
	require.NoFileExists(t, paths[10])
	require.NoFileExists(t, paths[12])
}

func TestRun9GCSliceRangesShadowResumesWithoutSkippingRetainedObjects(t *testing.T) {
	bucket := t.TempDir()
	for _, id := range []uint64{10, 11, 12, 13} {
		p := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, false))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0700))
		require.NoError(t, os.WriteFile(p, []byte("abc"), 0600))
	}
	req := run9GCSliceRangesRequest{JuiceFSFormatName: "snap", ObjectLayout: run9ObjectLayout{4096, false}, ObjectStorage: run9ObjectStorageDescriptor{Storage: "file", Bucket: bucket + "/"}, Ranges: []run9GCSliceRange{{Start: 10, EndInclusive: 13}}, RetainSliceIDs: map[uint64]bool{11: true}, MaxDeleteObjects: 1, Threads: 4, DryRun: true}
	var total uint64
	for i := 0; i < 3; i++ {
		out, err := run9GCSliceRanges(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, out.DeletedObjects)
		require.Equal(t, uint64(1), out.CandidateObjects)
		total += out.CandidateObjects
		require.Equal(t, i < 2, out.HasMore)
		if out.HasMore {
			require.NotEmpty(t, out.Next)
			require.NotEqual(t, req.After, out.Next)
		}
		req.After = out.Next
	}
	require.Equal(t, uint64(3), total)
}

func TestRun9GCSliceRangesSkipsUnrelatedOwnershipDirectories(t *testing.T) {
	for _, hashPrefix := range []bool{false, true} {
		t.Run(fmt.Sprint(hashPrefix), func(t *testing.T) {
			bucket := t.TempDir()
			// Same hash prefix, distinct million-ID directories.
			for i := uint64(0); i < 128; i++ {
				path := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(256*i+1, 0, 3, hashPrefix))
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
				require.NoError(t, os.WriteFile(path, []byte("abc"), 0600))
			}
			id := uint64(256000001)
			path := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, hashPrefix))
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, []byte("abc"), 0600))
			out, err := run9GCSliceRanges(t.Context(), run9GCSliceRangesRequest{JuiceFSFormatName: "snap", ObjectLayout: run9ObjectLayout{4096, hashPrefix}, ObjectStorage: run9ObjectStorageDescriptor{Storage: "file", Bucket: bucket + "/"}, Ranges: []run9GCSliceRange{{Start: id, EndInclusive: id}}, MaxDeleteObjects: 1})
			require.NoError(t, err)
			require.Equal(t, uint64(1), out.DeletedObjects)
			require.Less(t, out.ScanListedObjects, uint64(16))
			require.NoFileExists(t, path)
		})
	}
}
