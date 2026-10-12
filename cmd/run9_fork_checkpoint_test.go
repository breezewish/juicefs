package cmd

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// writeRun9TestMetadata creates closed metadata whose only file references the
// given slices, with the allocation counter at 20.
func writeRun9TestMetadata(t *testing.T, dir string, format string, bucket string, sliceIDs ...uint64) {
	t.Helper()
	m := meta.NewClient("badger://"+dir, meta.DefaultConf())
	require.NoError(t, m.Init(&meta.Format{Name: format, Storage: "file", Bucket: bucket + "/", BlockSize: 4, TrashDays: 0}, true))
	require.NoError(t, m.Shutdown())
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *badger.Txn) error {
		counter := make([]byte, 8)
		binary.LittleEndian.PutUint64(counter, 20)
		if err := tx.Set([]byte("CnextChunk"), counter); err != nil {
			return err
		}
		inode := make([]byte, 10)
		inode[0], inode[9] = 'A', 'I'
		binary.LittleEndian.PutUint64(inode[1:9], 2)
		if err := tx.Set(inode, (&meta.Attr{Typ: meta.TypeFile, Nlink: 1, Length: uint64(len(sliceIDs))}).Marshal()); err != nil {
			return err
		}
		// One-byte slices placed side by side stay visible together.
		var slices []byte
		for position, id := range sliceIDs {
			slices = binary.BigEndian.AppendUint32(slices, uint32(position))
			slices = binary.BigEndian.AppendUint64(slices, id)
			slices = binary.BigEndian.AppendUint32(slices, 1)
			slices = binary.BigEndian.AppendUint32(slices, 0)
			slices = binary.BigEndian.AppendUint32(slices, 1)
		}
		chunkKey := make([]byte, 14)
		chunkKey[0], chunkKey[9] = 'A', 'C'
		binary.LittleEndian.PutUint64(chunkKey[1:9], 2)
		return tx.Set(chunkKey, slices)
	}))
	require.NoError(t, db.Close())
}

func TestRun9GCCheckpointRetainsWhatChildrenReceivedAtFork(t *testing.T) {
	bucket, owner, child := t.TempDir(), t.TempDir(), t.TempDir()
	// The owner still uses 12. The child received 11 and 13 at Fork; the owner
	// has dropped them since. 10 and 14 were already gone before the Fork.
	writeRun9TestMetadata(t, owner, "snap", bucket, 12)
	writeRun9TestMetadata(t, filepath.Join(child, "meta"), "snap", bucket, 11, 13)
	paths := map[uint64]string{}
	writeObjects := func() {
		for _, id := range []uint64{10, 11, 12, 13, 14} {
			paths[id] = filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, false))
			require.NoError(t, os.MkdirAll(filepath.Dir(paths[id]), 0700))
			require.NoError(t, os.WriteFile(paths[id], []byte("abc"), 0600))
		}
	}
	writeObjects()
	req := run9GCCheckpointRequest{Directory: owner, Format: "snap", AllocationEnd: 20, Ranges: []run9GCSliceRange{{Start: 10, EndInclusive: 19}}, ForkCheckpoints: []run9ForkCheckpoint{{Directory: child, SourceAllocationEnd: 15}}}
	out, err := run9GCCheckpoint(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, out.FallbackForkCheckpoints)
	require.Equal(t, uint64(2), out.DeletedObjects)
	require.NoFileExists(t, paths[10])
	require.FileExists(t, paths[11])
	require.FileExists(t, paths[12])
	require.FileExists(t, paths[13])
	require.NoFileExists(t, paths[14])

	// The scan result replaces the metadata copy for every later round.
	require.FileExists(t, filepath.Join(child, "slices"))
	require.NoError(t, os.RemoveAll(filepath.Join(child, "meta")))
	writeObjects()
	// A deleted owner keeps nothing for itself, only what the child received.
	req.Deleted = true
	out, err = run9GCCheckpoint(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uint64(3), out.DeletedObjects)
	require.NoFileExists(t, paths[10])
	require.FileExists(t, paths[11])
	require.NoFileExists(t, paths[12])
	require.FileExists(t, paths[13])
	require.NoFileExists(t, paths[14])
}

// A fork checkpoint that cannot be used protects as its child did before fork
// checkpoints existed: by everything below the child's fork bound.
func TestRun9GCCheckpointUnusableForkCheckpointFallsBackToForkBound(t *testing.T) {
	bucket, owner := t.TempDir(), t.TempDir()
	writeRun9TestMetadata(t, owner, "snap", bucket)
	below := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(10, 0, 3, false))
	above := filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(15, 0, 3, false))
	missing, otherFormat, damaged, postponed := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	writeRun9TestMetadata(t, filepath.Join(otherFormat, "meta"), "other", bucket, 10)
	list := encodeRun9ForkCheckpointSlices([]uint64{10})
	require.NoError(t, os.WriteFile(filepath.Join(damaged, "slices"), list[:len(list)-1], 0600))
	writeRun9TestMetadata(t, filepath.Join(postponed, "meta"), "snap", bucket, 10)
	for name, directory := range map[string]string{"missing": missing, "another format": otherFormat, "damaged list": damaged, "scan postponed": postponed} {
		for _, object := range []string{below, above} {
			require.NoError(t, os.MkdirAll(filepath.Dir(object), 0700))
			require.NoError(t, os.WriteFile(object, []byte("abc"), 0600))
		}
		budget := run9ForkCheckpointScanBudget
		if directory == postponed {
			run9ForkCheckpointScanBudget = -time.Second
		}
		out, err := run9GCCheckpoint(context.Background(), run9GCCheckpointRequest{Directory: owner, Format: "snap", AllocationEnd: 20,
			Ranges: []run9GCSliceRange{{Start: 10, EndInclusive: 19}}, ForkCheckpoints: []run9ForkCheckpoint{{Directory: directory, SourceAllocationEnd: 13}}})
		run9ForkCheckpointScanBudget = budget
		require.NoError(t, err, name)
		require.Len(t, out.FallbackForkCheckpoints, 1, name)
		require.Equal(t, directory, out.FallbackForkCheckpoints[0].Directory, name)
		require.NotEmpty(t, out.FallbackForkCheckpoints[0].Reason, name)
		require.Equal(t, uint64(1), out.DeletedObjects, name)
		require.FileExists(t, below, name)
		require.NoFileExists(t, above, name)
		// Only a scan that was merely postponed keeps the round pending.
		require.Equal(t, directory == postponed, out.HasMore, name)
	}
	require.NoFileExists(t, filepath.Join(otherFormat, "slices"))
	require.NoFileExists(t, filepath.Join(postponed, "slices"))
}

func TestRun9GCCheckpointStoresForkCheckpointListWithNothingToDelete(t *testing.T) {
	bucket, owner, child := t.TempDir(), t.TempDir(), t.TempDir()
	writeRun9TestMetadata(t, owner, "snap", bucket)
	writeRun9TestMetadata(t, filepath.Join(child, "meta"), "snap", bucket, 11, 13)
	out, err := run9GCCheckpoint(context.Background(), run9GCCheckpointRequest{Directory: owner, Format: "snap", AllocationEnd: 20, ForkCheckpoints: []run9ForkCheckpoint{{Directory: child, SourceAllocationEnd: 20}}})
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Empty(t, out.FallbackForkCheckpoints)
	raw, err := os.ReadFile(filepath.Join(child, "slices"))
	require.NoError(t, err)
	ids, err := decodeRun9ForkCheckpointSlices(raw)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 13}, ids)
}

func TestRun9GCCheckpointScanBudgetAdvancesPastBrokenCopies(t *testing.T) {
	bucket, owner, forks := t.TempDir(), t.TempDir(), t.TempDir()
	broken, alsoBroken, healthy := filepath.Join(forks, "a-broken"), filepath.Join(forks, "b-broken"), filepath.Join(forks, "c-healthy")
	writeRun9TestMetadata(t, owner, "snap", bucket)
	writeRun9TestMetadata(t, filepath.Join(healthy, "meta"), "snap", bucket, 14)
	paths := map[uint64]string{}
	for _, id := range []uint64{10, 14, 15} {
		paths[id] = filepath.Join(bucket, "snap", chunk.FormatObjectBlockKey(id, 0, 3, false))
		require.NoError(t, os.MkdirAll(filepath.Dir(paths[id]), 0700))
		require.NoError(t, os.WriteFile(paths[id], []byte("abc"), 0600))
	}
	budget := run9ForkCheckpointScanBudget
	run9ForkCheckpointScanBudget = time.Nanosecond // One uncached attempt per round.
	t.Cleanup(func() { run9ForkCheckpointScanBudget = budget })
	req := run9GCCheckpointRequest{Directory: owner, Format: "snap", AllocationEnd: 20,
		Ranges:          []run9GCSliceRange{{Start: 10, EndInclusive: 19}},
		ForkCheckpoints: []run9ForkCheckpoint{{Directory: broken, SourceAllocationEnd: 13}, {Directory: alsoBroken, SourceAllocationEnd: 13}, {Directory: healthy, SourceAllocationEnd: 16}}}
	first, err := run9GCCheckpoint(t.Context(), req)
	require.NoError(t, err)
	require.True(t, first.HasMore)
	require.Empty(t, first.Next)
	require.NoFileExists(t, filepath.Join(healthy, "slices"))

	// The broken prefix cannot consume the next round's first scan slot.
	req.ForkCheckpointFailures = first.ForkCheckpointFailures
	second, err := run9GCCheckpoint(t.Context(), req)
	require.NoError(t, err)
	require.True(t, second.HasMore)
	require.Len(t, second.ForkCheckpointFailures, 2)
	require.NoFileExists(t, filepath.Join(healthy, "slices"))

	// Already failed scans protect by F for the rest of this pass; two slow
	// failures cannot keep alternating and prevent a completed object scan.
	req.ForkCheckpointFailures = second.ForkCheckpointFailures
	third, err := run9GCCheckpoint(t.Context(), req)
	require.NoError(t, err)
	require.False(t, third.HasMore)
	require.Empty(t, third.ForkCheckpointFailures)
	require.Len(t, third.FallbackForkCheckpoints, 2)
	require.FileExists(t, filepath.Join(healthy, "slices"))
	require.Equal(t, uint64(1), third.DeletedObjects)
	require.NoFileExists(t, paths[15])
	require.FileExists(t, paths[10]) // Broken proof still protects by F.
	require.FileExists(t, paths[14]) // Healthy proof protects its inherited slice.

	// Cached lists work with no scan budget.
	scan := run9ForkCheckpointScan{}
	ids, err := run9ForkCheckpointSlices(t.Context(), healthy, "snap", &scan)
	require.NoError(t, err)
	require.Equal(t, []uint64{14}, ids)
	require.Zero(t, scan.remaining)

	// Failure memoization is not permanent: a later pass retries repaired copies.
	writeRun9TestMetadata(t, filepath.Join(broken, "meta"), "snap", bucket, 10)
	req.ForkCheckpointFailures = third.ForkCheckpointFailures
	_, err = run9GCCheckpoint(t.Context(), req)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(broken, "slices"))
}

func TestRun9GCCheckpointForkSlicesRoundTripAndRejectDamage(t *testing.T) {
	ids := []uint64{1, 2, 1 << 32, 1<<32 + 5, 1<<64 - 1}
	raw := encodeRun9ForkCheckpointSlices(ids)
	decoded, err := decodeRun9ForkCheckpointSlices(raw)
	require.NoError(t, err)
	require.Equal(t, ids, decoded)

	empty, err := decodeRun9ForkCheckpointSlices(encodeRun9ForkCheckpointSlices(nil))
	require.NoError(t, err)
	require.Empty(t, empty)

	flipped := append([]byte(nil), raw...)
	flipped[len(run9ForkCheckpointSlicesHeader)+1] ^= 1
	_, err = decodeRun9ForkCheckpointSlices(flipped)
	require.ErrorContains(t, err, "malformed")
	_, err = decodeRun9ForkCheckpointSlices(raw[:len(raw)-1])
	require.ErrorContains(t, err, "malformed")
	_, err = decodeRun9ForkCheckpointSlices(nil)
	require.ErrorContains(t, err, "malformed")

	// A list with a valid checksum must still be exactly what the encoder writes.
	sealed := func(body ...byte) []byte {
		list := append([]byte(run9ForkCheckpointSlicesHeader), body...)
		return binary.LittleEndian.AppendUint32(list, crc32.ChecksumIEEE(list))
	}
	decoded, err = decodeRun9ForkCheckpointSlices(sealed(2, 5, 3))
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 8}, decoded)
	for name, list := range map[string][]byte{
		"fewer IDs than counted": sealed(2, 5),
		"more IDs than counted":  sealed(1, 5, 3),
		"repeated ID":            sealed(2, 5, 0),
		"slice ID zero":          sealed(1, 0),
	} {
		_, err = decodeRun9ForkCheckpointSlices(list)
		require.ErrorContains(t, err, "malformed", name)
	}
}
