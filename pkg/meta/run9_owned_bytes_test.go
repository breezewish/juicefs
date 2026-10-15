package meta

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const run9OwnedTestEpoch = 7

// newRun9OwnedTestMeta returns a tracked epoch whose worker is stopped, so that
// a test decides when marked ranges are recounted.
func newRun9OwnedTestMeta(t testing.TB, dir string, forkFloor uint64) *kvMeta {
	client, err := newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	t.Cleanup(func() { _ = m.Shutdown() })
	f := testFormat()
	f.TrashDays = 0
	require.NoError(t, m.Init(f, false))
	m.conf.MaxDeletes = 0
	start := uint64(run9OwnedTestEpoch) << 32
	require.NoError(t, m.client.txn(Background(), func(tx *kvTxn) error {
		tx.set(m.counterKey("nextChunk"), packCounter(int64(start)))
		return nil
	}, 0))
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start, ForkFloor: forkFloor}))
	m.run9Owned.stopWorker()
	return m
}

func run9OwnedVisible(t testing.TB, m *kvMeta) uint64 {
	t.Helper()
	require.NoError(t, m.Run9RecountOwnedBytes(context.Background()))
	return m.run9Owned.visible.Load()
}

// The sums of the ranges that changed must add up to what one count of the
// whole metadata finds, and to what checkpoint GC keeps after the close.
func TestRun9OwnedBytesRecountOnlyWhatChangedAndMatchCheckpointGC(t *testing.T) {
	ctx := Background()
	dir := t.TempDir()
	m := newRun9OwnedTestMeta(t, dir, 0)
	random := rand.New(rand.NewSource(1))
	sizes := make(map[uint64]uint32)
	var names []string
	inodes := make(map[string]Ino)
	var attr Attr
	for step := range 600 {
		switch op := random.Intn(10); {
		case op < 2 || len(names) == 0: // New file.
			name := strconv.Itoa(step)
			var inode Ino
			require.Zero(t, m.Create(ctx, RootInode, name, 0644, 0, 0, &inode, &attr))
			require.Zero(t, m.Close(ctx, inode))
			names, inodes[name] = append(names, name), inode
		case op < 6: // Write over any part of the first two chunks.
			var id uint64
			require.Zero(t, m.NewSlice(ctx, &id))
			size := uint32(1 + random.Intn(200))
			sizes[id] = size
			require.Zero(t, m.Write(ctx, inodes[names[random.Intn(len(names))]], uint32(random.Intn(2)), uint32(random.Intn(300)), Slice{Id: id, Size: size, Len: size}, time.Now()))
		case op < 7:
			require.Zero(t, m.Truncate(ctx, inodes[names[random.Intn(len(names))]], 0, uint64(random.Intn(400)), &attr, false))
		case op < 8: // Punch a hole.
			var length uint64
			_ = m.Fallocate(ctx, inodes[names[random.Intn(len(names))]], fallocPunchHole|fallocKeepSize, uint64(random.Intn(300)), uint64(1+random.Intn(200)), &length)
		case op < 9: // Replace one file by another.
			if len(names) < 2 {
				continue
			}
			from, to := random.Intn(len(names)), random.Intn(len(names))
			if from == to {
				continue
			}
			var inode Ino
			require.Zero(t, m.Rename(ctx, RootInode, names[from], RootInode, names[to], 0, &inode, &attr))
			inodes[names[to]] = inodes[names[from]]
			delete(inodes, names[from])
			names = append(names[:from], names[from+1:]...)
		default:
			i := random.Intn(len(names))
			require.Zero(t, m.Unlink(ctx, RootInode, names[i]))
			delete(inodes, names[i])
			names = append(names[:i], names[i+1:]...)
		}
		if step%7 != 0 {
			continue
		}
		whole := make(map[uint16]map[uint64]uint32)
		require.NoError(t, m.run9CountOwned(context.Background(), []byte{'A'}, whole))
		unique := make(map[uint64]uint32)
		for _, bucket := range whole {
			for id, size := range bucket {
				unique[id] = size
			}
		}
		var want uint64
		for _, size := range unique {
			want += uint64(size)
		}
		require.Equal(t, want, run9OwnedVisible(t, m), "after step %d", step)
	}
	visible := run9OwnedVisible(t, m)
	require.NotZero(t, visible)
	require.Equal(t, visible, m.Run9SettleOwnedBytes().VisibleBytes)

	require.NoError(t, m.Shutdown())
	checkpoint, err := OpenRun9Checkpoint(dir)
	require.NoError(t, err)
	defer checkpoint.Shutdown()
	ids, err := checkpoint.Run9CheckpointSlices(ctx)
	require.NoError(t, err)
	var kept uint64
	for _, id := range ids {
		kept += uint64(sizes[id])
	}
	require.Equal(t, kept, visible)
}

// Copy the actual stable key/value state into a separate closed Badger DB.
// This is the immutable metadata input used by the online capture counter.
func snapshotRun9OwnedTestMeta(t testing.TB, m *kvMeta) string {
	t.Helper()
	directory := t.TempDir()
	copy, err := newKVMeta("badger", directory, testConfig())
	require.NoError(t, err)
	defer copy.Shutdown()
	var copyErr error
	require.NoError(t, m.client.scan(nil, func(key, value []byte) bool {
		copyErr = copy.(*kvMeta).client.txn(context.Background(), func(tx *kvTxn) error {
			tx.set(key, value)
			return nil
		}, 0)
		return copyErr == nil
	}))
	require.NoError(t, copyErr)
	require.NoError(t, copy.Shutdown())
	return directory
}

func TestRun9OwnedBytesForkCountsCapturedIDsAndLaterCommitsOfEarlierAllocations(t *testing.T) {
	ctx := Background()
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var inode Ino
	require.Zero(t, m.Create(ctx, RootInode, "file", 0644, 0, 0, &inode, &attr))
	var delayed, captured uint64
	// Compaction can reserve an ID before capture and commit it afterwards.
	require.Zero(t, m.NewSlice(ctx, &delayed))
	require.Zero(t, m.NewSlice(ctx, &captured))
	require.Zero(t, m.Write(ctx, inode, 0, 0, Slice{Id: captured, Size: 100, Len: 100}, time.Now()))
	require.Zero(t, m.Write(ctx, inode, 0, 100, Slice{Id: 3<<32 + 5, Size: 100, Len: 100}, time.Now()))
	snapshot := snapshotRun9OwnedTestMeta(t, m)
	// Drop the captured data before the asynchronous snapshot scan. It still
	// belongs to the Fork even though no live recount ever saw it.
	require.Zero(t, m.Truncate(ctx, inode, 0, 0, &attr, false))
	require.NoError(t, m.Run9RecordFork(context.Background(), snapshot))
	require.EqualValues(t, 100, m.run9Owned.offset.Load())
	require.Zero(t, m.Write(ctx, inode, 0, 0, Slice{Id: delayed, Size: 50, Len: 50}, time.Now()))
	require.EqualValues(t, 50, run9OwnedVisible(t, m))
	require.EqualValues(t, 100, m.run9Owned.offset.Load())
	// Recording the same captured IDs again cannot count them twice.
	require.NoError(t, m.Run9RecordFork(context.Background(), snapshot))
	require.Equal(t, Run9OwnedBytes{VisibleBytes: 50, KeptForForksBytes: 100}, *m.Run9SettleOwnedBytes())
	require.Zero(t, m.Truncate(ctx, inode, 0, 0, &attr, false))
	require.Zero(t, run9OwnedVisible(t, m))
}

// pausedOwnedScanClient stalls a real recount after it takes the dirty ranges
// and before it reads their metadata.
type pausedOwnedScanClient struct {
	tkvClient
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (c *pausedOwnedScanClient) scan(prefix []byte, handler func([]byte, []byte) bool) error {
	c.once.Do(func() {
		close(c.paused)
		<-c.resume
	})
	return c.tkvClient.scan(prefix, handler)
}

func TestRun9OwnedBytesCancelledForkDoesNotChangeProtectedSlices(t *testing.T) {
	ctx := Background()
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var inode Ino
	require.Zero(t, m.Create(ctx, RootInode, "file", 0644, 0, 0, &inode, &attr))
	var id uint64
	require.Zero(t, m.NewSlice(ctx, &id))
	require.Zero(t, m.Write(ctx, inode, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	snapshot := snapshotRun9OwnedTestMeta(t, m)
	client := &pausedOwnedScanClient{tkvClient: m.client, paused: make(chan struct{}), resume: make(chan struct{})}
	m.client = client
	resume := sync.OnceFunc(func() { close(client.resume) })
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- m.Run9RecountOwnedBytes(context.Background())
	}()
	t.Cleanup(func() { resume(); <-finished })
	select {
	case <-client.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("recount did not start")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.Run9RecordFork(deadline, snapshot), context.DeadlineExceeded)
	require.Empty(t, m.run9Owned.protected)
	require.Zero(t, m.run9Owned.offset.Load())
	resume()
	require.NoError(t, <-done)
	require.NoError(t, m.Run9RecordFork(context.Background(), snapshot))
	require.Zero(t, run9OwnedVisible(t, m))
	require.EqualValues(t, 100, m.run9Owned.offset.Load())
}

func TestRun9OwnedBytesCancelledScanKeepsItsDirtyRanges(t *testing.T) {
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var inode Ino
	require.Zero(t, m.Create(Background(), RootInode, "file", 0644, 0, 0, &inode, &attr))
	var id uint64
	require.Zero(t, m.NewSlice(Background(), &id))
	require.Zero(t, m.Write(Background(), inode, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	client := &pausedOwnedScanClient{tkvClient: m.client, paused: make(chan struct{}), resume: make(chan struct{})}
	m.client = client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resume := sync.OnceFunc(func() { close(client.resume) })
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- m.Run9RecountOwnedBytes(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		resume()
		<-finished
	})
	select {
	case <-client.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("recount did not start")
	}
	cancel()
	resume()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, m.run9Owned.visible.Load())
	require.False(t, m.run9Owned.current.Load())
	require.EqualValues(t, 100, run9OwnedVisible(t, m))
}

func TestRun9OwnedBytesCountClonedSlicesOnceUntilTheirLastReferenceIsRemoved(t *testing.T) {
	ctx := Background()
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var source, clone Ino
	require.Zero(t, m.Create(ctx, RootInode, "source", 0644, 0, 0, &source, &attr))
	require.Zero(t, m.Create(ctx, RootInode, "clone", 0644, 0, 0, &clone, &attr))
	require.NotEqual(t, run9OwnedBucket(m.inodeKey(source)), run9OwnedBucket(m.inodeKey(clone)))
	var id uint64
	require.Zero(t, m.NewSlice(ctx, &id))
	require.Zero(t, m.Write(ctx, source, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	require.EqualValues(t, 100, run9OwnedVisible(t, m))
	// Same slice through another chunk in the same file and another inode bucket.
	require.Zero(t, m.CopyFileRange(ctx, source, 0, source, ChunkSize, 100, 0, nil, nil))
	require.Zero(t, m.CopyFileRange(ctx, source, 0, clone, 0, 100, 0, nil, nil))
	require.EqualValues(t, 100, run9OwnedVisible(t, m))
	m.run9Owned.markAll()
	require.EqualValues(t, 100, run9OwnedVisible(t, m))
	require.Zero(t, m.Close(ctx, source))
	require.Zero(t, m.Unlink(ctx, RootInode, "source"))
	require.EqualValues(t, 100, run9OwnedVisible(t, m))
	require.Zero(t, m.Truncate(ctx, clone, 0, 0, &attr, false))
	require.Zero(t, run9OwnedVisible(t, m))
}

func TestRun9OwnedBytesRemountKeepsSignedBaselineWhenProtectionChanges(t *testing.T) {
	directory := t.TempDir()
	m := newRun9OwnedTestMeta(t, directory, 0)
	var inode Ino
	var attr Attr
	require.Zero(t, m.Create(Background(), RootInode, "file", 0644, 0, 0, &inode, &attr))
	var id uint64
	require.Zero(t, m.NewSlice(Background(), &id))
	require.Zero(t, m.Write(Background(), inode, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	start := m.Run9SliceAllocationEnd(0)
	require.NoError(t, m.Shutdown())
	// A previous calibrated raw count can be below the newly visible bytes.
	// Keep that signed difference; clamping here would change Portal's basis.
	last := int64(20)
	client, err := newKVMeta("badger", directory, testConfig())
	require.NoError(t, err)
	m = client.(*kvMeta)
	_, err = m.Load(true)
	require.NoError(t, err)
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start, ProtectedIDs: []uint64{id}, LastClosedRaw: &last}))
	m.run9Owned.stopWorker()
	require.Zero(t, m.run9Owned.visible.Load())
	require.EqualValues(t, 20, m.run9Owned.offset.Load())
	require.NoError(t, m.Shutdown())
	// After the child disappears, that same slice becomes visible again.
	client, err = newKVMeta("badger", directory, testConfig())
	require.NoError(t, err)
	m = client.(*kvMeta)
	t.Cleanup(func() { _ = m.Shutdown() })
	_, err = m.Load(true)
	require.NoError(t, err)
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start, LastClosedRaw: &last}))
	m.run9Owned.stopWorker()
	require.EqualValues(t, 100, m.run9Owned.visible.Load())
	require.EqualValues(t, -80, m.run9Owned.offset.Load())
	require.Zero(t, m.Truncate(Background(), inode, 0, 0, &attr, false))
	require.Equal(t, Run9OwnedBytes{KeptForForksBytes: -80}, *m.Run9SettleOwnedBytes())
}

func TestRun9OwnedBytesLegacyFloorDoesNotHideFutureWrites(t *testing.T) {
	directory := t.TempDir()
	client, err := newKVMeta("badger", directory, testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	start := uint64(run9OwnedTestEpoch)<<32 + 100
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set(m.counterKey("nextChunk"), packCounter(int64(start)))
		return nil
	}, 0))
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start, ForkFloor: math.MaxUint64}))
	m.run9Owned.stopWorker()
	var inode Ino
	var attr Attr
	require.Zero(t, m.Create(Background(), RootInode, "file", 0644, 0, 0, &inode, &attr))
	require.Zero(t, m.Write(Background(), inode, 0, 0, Slice{Id: start - 1, Size: 100, Len: 100}, time.Now()))
	var id uint64
	require.Zero(t, m.NewSlice(Background(), &id))
	require.Zero(t, m.Write(Background(), inode, 0, 100, Slice{Id: id, Size: 50, Len: 50}, time.Now()))
	require.EqualValues(t, 50, run9OwnedVisible(t, m))
}

func TestRun9OwnedBytesIncludesHistoricalOwnershipWithoutCountingInheritedData(t *testing.T) {
	client, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	start := uint64(run9OwnedTestEpoch) << 32
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set(m.counterKey("nextChunk"), packCounter(int64(start)))
		return nil
	}, 0))
	var inode Ino
	var attr Attr
	require.Zero(t, m.Create(Background(), RootInode, "file", 0644, 0, 0, &inode, &attr))
	require.Zero(t, m.Write(Background(), inode, 0, 0, Slice{Id: 2<<32 + 1, Size: 100, Len: 100}, time.Now()))
	require.Zero(t, m.Write(Background(), inode, 0, 100, Slice{Id: 3<<32 + 1, Size: 200, Len: 200}, time.Now()))
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{
		Epoch: run9OwnedTestEpoch, AllocationStart: start,
		OwnershipRanges: []Run9OwnedEpochRange{{StartEpoch: 2, EndEpoch: 2}, {StartEpoch: run9OwnedTestEpoch, EndEpoch: run9OwnedTestEpoch}},
	}))
	m.run9Owned.stopWorker()
	// Initialization must scan historical data even before this writable epoch
	// allocates anything. The intervening inherited epoch is never attributed.
	require.EqualValues(t, 100, m.run9Owned.visible.Load())
	snapshot := snapshotRun9OwnedTestMeta(t, m)
	require.NoError(t, m.Run9RecordFork(context.Background(), snapshot))
	require.Zero(t, m.run9Owned.visible.Load())
	require.EqualValues(t, 100, m.run9Owned.offset.Load())
}

func TestRun9OwnedBytesExhaustedBasisDisablesTheOptionalCount(t *testing.T) {
	client, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, Basis: ^uint64(0)}))
	require.Equal(t, ^uint64(0), m.run9Owned.basis.Load())
	require.False(t, m.run9Owned.current.Load())
	require.Nil(t, m.Run9SettleOwnedBytes())
	require.NoError(t, m.Run9RecordFork(context.Background(), "/unused"))
}

func TestRun9OwnedBytesStartupScanFailureLeavesTheOptionalCountUnavailable(t *testing.T) {
	client, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set(m.chunkKey(2, 0), []byte("corrupt"))
		return nil
	}, 0))
	start := uint64(run9OwnedTestEpoch)<<32 + 1
	require.ErrorContains(t, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start, Basis: 7}), "malformed chunk")
	require.EqualValues(t, 7, m.run9Owned.basis.Load())
	require.False(t, m.run9Owned.current.Load())
	require.Nil(t, m.Run9SettleOwnedBytes())
	// Counter failure does not turn a later source Fork into a counter error.
	require.NoError(t, m.Run9RecordFork(context.Background(), "/unused"))
}

// What a changed file costs: one recount of its key range.
func BenchmarkRun9OwnedBytesRecountOneRange(b *testing.B) {
	for _, files := range []int{200_000, 1_000_000} {
		b.Run(strconv.Itoa(files), func(b *testing.B) {
			m := newRun9OwnedTestMeta(b, b.TempDir(), 0)
			const batch = 2000
			for base := 0; base < files; base += batch {
				require.NoError(b, m.client.txn(Background(), func(tx *kvTxn) error {
					for i := base; i < base+batch; i++ {
						inode := Ino(i + 10)
						tx.set(m.inodeKey(inode), (&Attr{Typ: TypeFile, Mode: 0644, Nlink: 1, Length: 9000}).Marshal())
						id := uint64(run9OwnedTestEpoch)<<32 + uint64(i)*2
						tx.set(m.chunkKey(inode, 0), append(marshalSlice(0, id, 8192, 0, 8192), marshalSlice(4096, id+1, 4096, 0, 4096)...))
					}
					return nil
				}, 0))
			}
			start := time.Now()
			require.NoError(b, m.Run9RecountOwnedBytes(context.Background()))
			b.ReportMetric(time.Since(start).Seconds(), "s/whole")
			require.EqualValues(b, files*(8192+4096), m.run9Owned.visible.Load())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.run9Owned.changed([]uint16{uint16(i * 7919)})
				require.NoError(b, m.Run9RecountOwnedBytes(context.Background()))
			}
		})
	}
}

// The write path only notes which key range a transaction touched.
func BenchmarkRun9OwnedBytesWrite(b *testing.B) {
	for _, tracked := range []bool{false, true} {
		b.Run(map[bool]string{false: "stock", true: "tracked"}[tracked], func(b *testing.B) {
			client, err := newKVMeta("badger", b.TempDir(), testConfig())
			require.NoError(b, err)
			m := client.(*kvMeta)
			defer m.Shutdown()
			f := testFormat()
			f.TrashDays = 0
			require.NoError(b, m.Init(f, false))
			start := uint64(run9OwnedTestEpoch) << 32
			require.NoError(b, m.client.txn(Background(), func(tx *kvTxn) error {
				tx.set(m.counterKey("nextChunk"), packCounter(int64(start)))
				return nil
			}, 0))
			if tracked {
				require.NoError(b, m.Run9TrackOwnedBytes(context.Background(), Run9OwnedBytesConfig{Epoch: run9OwnedTestEpoch, AllocationStart: start}))
			}
			ctx := Background()
			var attr Attr
			var inode Ino
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%64 == 0 {
					b.StopTimer()
					require.Zero(b, m.Create(ctx, RootInode, strconv.Itoa(i), 0644, 0, 0, &inode, &attr))
					b.StartTimer()
				}
				var id uint64
				if st := m.NewSlice(ctx, &id); st != 0 {
					b.Fatal(st)
				}
				if st := m.en.doWrite(ctx, inode, 0, uint32(i%4)*4096, Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), new(int), new(dirStat), &attr); st != 0 {
					b.Fatal(st)
				}
			}
		})
	}
}
