package meta

import (
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const run9OwnedTestEpoch = 7

// newRun9OwnedTestMeta returns a tracked epoch whose worker is stopped, so that
// a test decides when marked ranges are recounted.
func newRun9OwnedTestMeta(t testing.TB, dir string, forkBound uint64) *kvMeta {
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
	m.Run9TrackOwnedBytes(run9OwnedTestEpoch, forkBound, 0, start)
	m.run9Owned.stopWorker()
	return m
}

func run9OwnedVisible(m *kvMeta) uint64 {
	m.run9OwnedRecount()
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
		whole := make(map[uint16]uint64)
		require.NoError(t, m.run9CountOwned([]byte{'A'}, 0, math.MaxUint64, whole))
		var want uint64
		for _, bytes := range whole {
			want += bytes
		}
		require.Equal(t, want, run9OwnedVisible(m), "after step %d", step)
	}
	visible := run9OwnedVisible(m)
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

func TestRun9OwnedBytesLeaveOutInheritedDataAndStartAgainAtAFork(t *testing.T) {
	ctx := Background()
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var inode Ino
	require.Zero(t, m.Create(ctx, RootInode, "file", 0644, 0, 0, &inode, &attr))
	write := func(id uint64, off uint32) {
		require.Zero(t, m.Write(ctx, inode, 0, off, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	}
	newID := func() uint64 {
		var id uint64
		require.Zero(t, m.NewSlice(ctx, &id))
		return id
	}

	write(3<<32+5, 0) // A slice of the Fork source is not this epoch's.
	write(newID(), 100)
	require.EqualValues(t, 100, run9OwnedVisible(m))
	// Written just before the Fork: the worker has not counted it yet.
	received := newID()
	write(received, 200)

	// The persisted end covers reserved IDs; the Fork received only used ones.
	// The capture reads no metadata and takes what is counted so far.
	persistedEnd := m.Run9SliceAllocationEnd(0)
	require.Greater(t, persistedEnd, received+1)
	require.Equal(t, received+1, m.Run9RecordFork(persistedEnd))
	require.Zero(t, m.run9Owned.visible.Load())
	require.EqualValues(t, 100, m.run9Owned.keptForForks.Load())

	// Right after it the woken worker adds what the Fork received but was not
	// counted yet. Dropping received data changes nothing; later data counts again.
	m.run9OwnedCountLateChanges()
	require.EqualValues(t, 200, m.run9Owned.keptForForks.Load())
	write(newID(), 100)
	require.EqualValues(t, 100, run9OwnedVisible(m))
	require.EqualValues(t, 200, m.run9Owned.keptForForks.Load())
	write(newID(), 100)
	require.EqualValues(t, 100, run9OwnedVisible(m))
	owned := m.Run9SettleOwnedBytes()
	require.Equal(t, Run9OwnedBytes{VisibleBytes: 100, ForkBound: received + 1, KeptForForksBytes: 200}, *owned)
}

// A recount that a Fork overtakes loses nothing: its ranges count for the
// Fork as far as they were written before it, and again for what came after.
func TestRun9OwnedBytesRecountOvertakenByAFork(t *testing.T) {
	ctx := Background()
	m := newRun9OwnedTestMeta(t, t.TempDir(), 0)
	var attr Attr
	var inode Ino
	require.Zero(t, m.Create(ctx, RootInode, "file", 0644, 0, 0, &inode, &attr))
	write := func(off uint32) {
		var id uint64
		require.Zero(t, m.NewSlice(ctx, &id))
		require.Zero(t, m.Write(ctx, inode, 0, off, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	}
	write(0)
	// The worker has taken the marks of that write when the Fork is captured.
	o := &m.run9Owned
	o.mu.Lock()
	generation := o.generation
	o.mu.Unlock()
	taken := o.takeMarked()
	m.Run9RecordFork(m.Run9SliceAllocationEnd(0))
	write(100)
	o.mu.Lock()
	require.NotEqual(t, generation, o.generation)
	for _, late := range o.late {
		late.buckets = append(late.buckets, taken...)
	}
	o.mu.Unlock()
	o.changed(taken)
	require.EqualValues(t, 100, run9OwnedVisible(m))
	require.EqualValues(t, 100, o.keptForForks.Load())
}

func TestRun9OwnedBytesCountAtMountOnlyWhenOwnDataCanExistAboveTheBound(t *testing.T) {
	start := uint64(run9OwnedTestEpoch)<<32 + 500
	for _, c := range []struct {
		name      string
		forkBound uint64
		count     bool
	}{
		{"a new epoch", 0, true},
		{"everything existed at the last Fork", start, false},
		{"written since the last Fork", start - 1, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			client, err := newKVMeta("badger", t.TempDir(), testConfig())
			require.NoError(t, err)
			m := client.(*kvMeta)
			defer m.Shutdown()
			allocationStart := start
			if c.name == "a new epoch" {
				allocationStart, c.count = uint64(run9OwnedTestEpoch)<<32, false
			}
			m.Run9TrackOwnedBytes(run9OwnedTestEpoch, c.forkBound, 0, allocationStart)
			m.run9Owned.stopWorker()
			require.Equal(t, !c.count, m.run9Owned.current.Load())
			require.Equal(t, c.count, len(m.run9Owned.takeMarked()) == run9OwnedBuckets)
		})
	}
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
			m.run9OwnedRecount()
			b.ReportMetric(time.Since(start).Seconds(), "s/whole")
			require.EqualValues(b, files*(8192+4096), m.run9Owned.visible.Load())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.run9Owned.changed([]uint16{uint16(i * 7919)})
				m.run9OwnedRecount()
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
				m.Run9TrackOwnedBytes(run9OwnedTestEpoch, 0, 0, start)
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
