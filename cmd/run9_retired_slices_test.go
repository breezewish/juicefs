package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

type retiredTestRecord struct {
	id         uint64
	size       uint32
	references int64
}

// Build a real closed Badger with native K records. Tests of retirement itself
// live in pkg/meta and real-FUSE E2E; this fixture isolates queue consumption.
func makeRetiredTestSnapshot(t *testing.T, queue string, format *meta.Format, records []retiredTestRecord) string {
	t.Helper()
	proof := run9RetiredSliceSnapshot{Version: 1, Format: format.Name, OwnedEpoch: 1, Start: 1 << 32}
	raw, err := json.Marshal(proof)
	require.NoError(t, err)
	dir := filepath.Join(queue, fmt.Sprintf("gc-test-%x", sha256.Sum256(raw)))
	require.NoError(t, os.Mkdir(dir, 0700))
	metaDir := filepath.Join(dir, "meta")
	m := meta.NewClient("badger://"+metaDir, meta.DefaultConf())
	require.NoError(t, m.Init(format, true))
	require.NoError(t, m.Shutdown())
	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *badger.Txn) error {
		counter := make([]byte, 8)
		binary.LittleEndian.PutUint64(counter, 1<<32+131072)
		if err := tx.Set([]byte("CnextChunk"), counter); err != nil {
			return err
		}
		for _, r := range records {
			key, value := make([]byte, 13), make([]byte, 8)
			key[0] = 'K'
			binary.BigEndian.PutUint64(key[1:9], r.id)
			binary.BigEndian.PutUint32(key[9:], r.size)
			binary.LittleEndian.PutUint64(value, uint64(r.references))
			if err := tx.Set(key, value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "proof.json"), raw, 0600))
	return dir
}

func TestRun9RetiredSlicesCapturesNewAllocationWindowOnRemount(t *testing.T) {
	url := "badger://" + t.TempDir()
	conf := meta.DefaultConf()
	conf.MaxDeletes = 0
	m := meta.NewClient(url, conf)
	require.NoError(t, m.Init(&meta.Format{Name: "test", UUID: "volume", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4}, true))
	require.NoError(t, m.Shutdown())
	_, err := run9PrepareWritableEpoch(context.Background(), url, 7)
	require.NoError(t, err)
	t.Setenv("JFS_RUN9_OWNED_EPOCH", "7")
	m = meta.NewClient(url, conf)
	start, err := beginRun9SliceAllocation(m)
	require.NoError(t, err)
	require.EqualValues(t, 7<<32, *start)
	var id uint64
	require.Zero(t, m.NewSlice(meta.Background(), &id))
	require.NoError(t, m.Shutdown())
	m = meta.NewClient(url, conf)
	defer m.Shutdown()
	start, err = beginRun9SliceAllocation(m)
	require.NoError(t, err)
	require.EqualValues(t, 7<<32+4096, *start)
	t.Setenv("JFS_RUN9_OWNED_EPOCH", "8")
	_, err = beginRun9SliceAllocation(m)
	require.ErrorContains(t, err, "outside owned epoch")
}

func TestRun9RetiredSlicesResumesBeyondOldTotalLimits(t *testing.T) {
	queue, bucket := t.TempDir(), t.TempDir()
	format := &meta.Format{Name: "many", UUID: "volume", Storage: "file", Bucket: bucket + "/", BlockSize: 4096, HashPrefix: true}
	records := make([]retiredTestRecord, 66000)
	for i := range records {
		records[i] = retiredTestRecord{1<<32 + uint64(i), 1, -1}
	}
	dir := makeRetiredTestSnapshot(t, queue, format, records)
	first := filepath.Join(bucket, "many", chunk.FormatObjectBlockKey(1<<32, 0, 1, true))
	last := filepath.Join(bucket, "many", chunk.FormatObjectBlockKey(1<<32+65999, 0, 1, true))
	require.NoError(t, os.MkdirAll(filepath.Dir(first), 0700))
	require.NoError(t, os.MkdirAll(filepath.Dir(last), 0700))
	require.NoError(t, os.WriteFile(first, []byte{1}, 0600))
	require.NoError(t, os.WriteFile(last, []byte{2}, 0600))
	var firstTurn run9RetiredSliceGCResult
	done, err := consumeRun9RetiredSnapshot(context.Background(), dir, 8192, &firstTurn)
	require.NoError(t, err)
	require.False(t, done)
	require.EqualValues(t, 8192, firstTurn.DeletedObjects)
	require.FileExists(t, filepath.Join(dir, "cursor"))
	require.NoFileExists(t, first)
	require.FileExists(t, last)
	// A new consumer reopens the read-only snapshot and continues after the
	// checkpoint, including records beyond the former 65,536-record scan cap.
	secondTurn, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, secondTurn.OK, secondTurn.Error)
	require.EqualValues(t, 66000-8192, secondTurn.DeletedObjects)
	require.Equal(t, 1, secondTurn.CompletedSnapshots)
	require.NoFileExists(t, last)
	require.NoDirExists(t, dir)
}

func TestRun9RetiredSlicesKeepsLiveHistoricalAndOtherEpochObjects(t *testing.T) {
	queue, bucket := t.TempDir(), t.TempDir()
	format := &meta.Format{Name: "lineage", UUID: "volume", Storage: "file", Bucket: bucket + "/", BlockSize: 4}
	dir := makeRetiredTestSnapshot(t, queue, format, []retiredTestRecord{
		{1<<32 - 1, 1, -1}, {1 << 32, 5000, -1}, {1<<32 + 1, 1, 0}, {2 << 32, 1, -1},
	})
	old := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32-1, 0, 1, false))
	live := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32+1, 0, 1, false))
	other := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(2<<32, 0, 1, false))
	for _, path := range []string{old, live, other} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, []byte{1}, 0600))
	}
	out, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, out.OK, out.Error)
	require.EqualValues(t, 2, out.DeletedObjects)
	require.EqualValues(t, 5000, out.DeletedLogicalBytes)
	require.FileExists(t, old)
	require.FileExists(t, live)
	require.FileExists(t, other)
	require.NoDirExists(t, dir)
}

func TestRun9RetiredSlicesRejectsCorruptProofAndRetriesFailedDelete(t *testing.T) {
	queue, bucket := t.TempDir(), t.TempDir()
	format := &meta.Format{Name: "lineage", UUID: "volume", Storage: "file", Bucket: bucket + "/", BlockSize: 4096}
	dir := makeRetiredTestSnapshot(t, queue, format, []retiredTestRecord{{1 << 32, 1, -1}})
	proofPath := filepath.Join(dir, "proof.json")
	raw, err := os.ReadFile(proofPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(proofPath, append(raw, ' '), 0600))
	out, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.False(t, out.OK)
	require.Contains(t, out.Error, "checksum")
	require.Zero(t, out.DeletedObjects)
	require.NoError(t, os.WriteFile(proofPath, raw, 0600))
	key := filepath.Join(bucket, "lineage", chunk.FormatObjectBlockKey(1<<32, 0, 1, false))
	require.NoError(t, os.MkdirAll(key, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(key, "obstacle"), nil, 0600))
	now := time.Now()
	require.NoError(t, os.Chtimes(dir, now, now))
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.False(t, out.OK)
	require.Equal(t, 1, out.FailedSnapshots)
	require.NoFileExists(t, filepath.Join(dir, "cursor"))
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.Equal(t, 1, out.DeferredSnapshots)
	require.NoError(t, os.Remove(filepath.Join(key, "obstacle")))
	require.NoError(t, os.Remove(key))
	require.NoError(t, os.Chtimes(dir, now, now))
	out, err = run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.True(t, out.OK, out.Error)
	require.Equal(t, 1, out.CompletedSnapshots)
}

func TestRun9RetiredSlicesCorruptMetadataDoesNotBlockOtherSnapshots(t *testing.T) {
	queue := t.TempDir()
	format := &meta.Format{Name: "broken", UUID: "volume", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4096}
	broken := makeRetiredTestSnapshot(t, queue, format, nil)
	metaDir := filepath.Join(broken, "meta")
	require.NoError(t, os.RemoveAll(metaDir))
	require.NoError(t, os.Mkdir(metaDir, 0700))
	format.Name = "healthy"
	healthy := makeRetiredTestSnapshot(t, queue, format, []retiredTestRecord{{1 << 32, 1, -1}})
	// A crash after the completion rename can leave only some metadata files.
	finished := filepath.Join(queue, ".done-interrupted")
	require.NoError(t, os.Mkdir(finished, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(finished, "partial"), nil, 0600))
	unpublished := filepath.Join(queue, ".tmp-unpublished")
	require.NoError(t, os.Mkdir(unpublished, 0700))
	out, err := run9GCRetiredSlices(context.Background(), queue)
	require.NoError(t, err)
	require.False(t, out.OK)
	require.Equal(t, 1, out.FailedSnapshots)
	require.Equal(t, 1, out.CompletedSnapshots)
	require.EqualValues(t, 1, out.DeletedObjects)
	require.NoDirExists(t, healthy)
	require.NoDirExists(t, finished)
	require.DirExists(t, unpublished)
	entries, err := os.ReadDir(metaDir)
	require.NoError(t, err)
	require.Empty(t, entries, "opening an invalid snapshot must not initialize metadata")
}

func TestRun9RetiredSlicesRejectsInvalidCursorBeforeDeleting(t *testing.T) {
	queue, bucket := t.TempDir(), t.TempDir()
	format := &meta.Format{Name: "cursor", UUID: "volume", Storage: "file", Bucket: bucket + "/", BlockSize: 4096}
	dir := makeRetiredTestSnapshot(t, queue, format, []retiredTestRecord{{1 << 32, 1, -1}})
	key := filepath.Join(bucket, "cursor", chunk.FormatObjectBlockKey(1<<32, 0, 1, false))
	require.NoError(t, os.MkdirAll(filepath.Dir(key), 0700))
	require.NoError(t, os.WriteFile(key, []byte{1}, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cursor"), []byte("4b000000020000000000000001"), 0600)) // outside lifecycle
	var out run9RetiredSliceGCResult
	done, err := consumeRun9RetiredSnapshot(context.Background(), dir, 1, &out)
	require.ErrorContains(t, err, "invalid retired slice cursor")
	require.False(t, done)
	require.Zero(t, out.DeletedObjects)
	require.FileExists(t, key)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cursor"), []byte("partial"), 0600))
	_, err = consumeRun9RetiredSnapshot(context.Background(), dir, 1, &out)
	require.ErrorContains(t, err, "invalid snapshot cursor file")
	require.FileExists(t, key)
}

type retiredSliceDeleteStore struct {
	object.ObjectStorage
	deleteFn func(context.Context, string) error
}

func (s *retiredSliceDeleteStore) Delete(ctx context.Context, key string, _ ...object.AttrGetter) error {
	return s.deleteFn(ctx, key)
}

func TestRun9RetiredSlicesCommandKeepsCountsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	object.Register("retired-cancel-test", func(string, string, string, string) (object.ObjectStorage, error) {
		return &retiredSliceDeleteStore{base, func(context.Context, string) error { cancel(); return nil }}, nil
	})
	queue := t.TempDir()
	format := &meta.Format{Name: "cancel", UUID: "volume", Storage: "retired-cancel-test", Bucket: "test", BlockSize: 4096}
	makeRetiredTestSnapshot(t, queue, format, []retiredTestRecord{{1 << 32, 1, -1}})
	var output bytes.Buffer
	app := &cli.App{Writer: &output, Commands: []*cli.Command{cmdRun9GCRetiredSlices()}}
	require.NoError(t, app.RunContext(ctx, []string{"juicefs", "gc-retired-slices", "--queue-dir", queue}))
	var result run9RetiredSliceGCResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.False(t, result.OK)
	require.Contains(t, result.Error, "context canceled")
	require.EqualValues(t, 1, result.DeletedObjects)
}
