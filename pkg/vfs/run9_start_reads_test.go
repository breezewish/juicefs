package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func writeStartReadsTestFile(t *testing.T, v *VFS, ctx Context, name string, size int) Ino {
	t.Helper()
	entry, fh, st := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'x'}, size), 0, fh))
	require.Zero(t, v.Flush(ctx, entry.Inode, fh, 0))
	v.Release(ctx, entry.Inode, fh)
	return entry.Inode
}

func readStartReadsTestFile(t *testing.T, v *VFS, ctx Context, ino Ino, off uint64, size int) {
	t.Helper()
	_, fh, st := v.Open(ctx, ino, syscall.O_RDONLY)
	require.Zero(t, st)
	n, st := v.Read(ctx, ino, make([]byte, size), off, fh)
	require.Zero(t, st)
	require.Equal(t, size, n)
	v.Release(ctx, ino, fh)
}

func TestStartReadsListsFilesReadDuringTheWindow(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	first := writeStartReadsTestFile(t, v, ctx, "first", 64<<10)
	second := writeStartReadsTestFile(t, v, ctx, "second", 64<<10)
	removed := writeStartReadsTestFile(t, v, ctx, "removed", 4096)
	late := writeStartReadsTestFile(t, v, ctx, "late", 4096)

	v.TraceStartReads(time.Hour)
	created := writeStartReadsTestFile(t, v, ctx, "created", 4096)
	readStartReadsTestFile(t, v, ctx, second, 0, 4096)
	readStartReadsTestFile(t, v, ctx, first, 8192, 100) // a partial page lists the whole page
	readStartReadsTestFile(t, v, ctx, removed, 0, 4096)
	readStartReadsTestFile(t, v, ctx, created, 0, 4096)
	readStartReadsTestFile(t, v, ctx, first, 32768, 8192)
	require.Zero(t, v.Unlink(ctx, 1, "removed"))
	v.readTrace.until = time.Now() // the start window ends
	readStartReadsTestFile(t, v, ctx, late, 0, 4096)

	files, err := v.StartReads(ctx, StartReadLimits{MaxFiles: 100, MaxBytes: 1 << 30, MaxListBytes: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, []StartRead{
		{Inode: second, Ranges: [][2]uint64{{0, 4096}}},
		{Inode: first, Ranges: [][2]uint64{{8192, 4096}, {32768, 8192}}},
		{Inode: created, Ranges: [][2]uint64{{0, 4096}}},
	}, files)

	files, err = v.StartReads(ctx, StartReadLimits{MaxFiles: 100, MaxBytes: 8192, MaxListBytes: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, []StartRead{
		{Inode: second, Ranges: [][2]uint64{{0, 4096}}},
		{Inode: created, Ranges: [][2]uint64{{0, 4096}}},
	}, files)

	// The list ends before the entry that would make its encoding too large.
	firstEntry, err := json.Marshal(StartRead{Inode: second, Ranges: [][2]uint64{{0, 4096}}})
	require.NoError(t, err)
	files, err = v.StartReads(ctx, StartReadLimits{MaxFiles: 100, MaxBytes: 1 << 30, MaxListBytes: len("[") + len(firstEntry) + len("]")})
	require.NoError(t, err)
	require.Equal(t, []StartRead{{Inode: second, Ranges: [][2]uint64{{0, 4096}}}}, files)

	// A start trace is not a recording's: /read-trace refuses it.
	_, err = v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 100, MaxBytes: 1 << 30, MaxListBytes: 1 << 20})
	require.Error(t, err)
}

// prefetchRecorder records PrefetchStartReads' block requests in order.
type prefetchRecorder struct {
	chunk.ChunkStore
	sync.Mutex
	blocks []prefetchBlock
}

func (r *prefetchRecorder) PrefetchBlock(_ context.Context, id uint64, length uint32, indx int) (bool, error) {
	r.Lock()
	defer r.Unlock()
	r.blocks = append(r.blocks, prefetchBlock{slice: id, sliceSize: length, indx: indx})
	return true, nil
}

func TestPrefetchStartReadsRequestsTheBlocksBehindListedRanges(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	writeAt := func(ino Ino, off uint64, size int) {
		_, fh, st := v.Open(ctx, ino, syscall.O_RDWR)
		require.Zero(t, st)
		require.Zero(t, v.Write(ctx, ino, bytes.Repeat([]byte{'y'}, size), off, fh))
		require.Zero(t, v.Flush(ctx, ino, fh, 0))
		v.Release(ctx, ino, fh)
	}
	slicesOf := func(ino Ino, chunk uint32) []meta.Slice {
		var slices []meta.Slice
		require.Zero(t, v.Meta.Read(ctx, ino, chunk, &slices))
		return slices
	}
	small := writeStartReadsTestFile(t, v, ctx, "small", 4096)
	big := writeStartReadsTestFile(t, v, ctx, "big", 10<<20)
	// An overwrite in the middle leaves the rest of the first slice at Off > 0.
	overwritten := writeStartReadsTestFile(t, v, ctx, "overwritten", 12<<20)
	writeAt(overwritten, 4<<20, 4096)
	// A sparse file: a hole, then data on both sides of the first chunk's end.
	sparse := writeStartReadsTestFile(t, v, ctx, "sparse", 0)
	writeAt(sparse, meta.ChunkSize-4096, 4096)
	writeAt(sparse, meta.ChunkSize, 4096)

	smallSlices, bigSlices := slicesOf(small, 0), slicesOf(big, 0)
	require.Len(t, smallSlices, 1)
	require.Len(t, bigSlices, 1)
	overSlices := slicesOf(overwritten, 0)
	require.Len(t, overSlices, 3)
	require.Equal(t, overSlices[0].Id, overSlices[2].Id)
	require.Equal(t, uint32(4<<20+4096), overSlices[2].Off)
	sparseFirst, sparseSecond := slicesOf(sparse, 0), slicesOf(sparse, 1)
	require.Len(t, sparseFirst, 2)
	require.Zero(t, sparseFirst[0].Id) // the hole
	require.Len(t, sparseSecond, 1)

	recorder := &prefetchRecorder{ChunkStore: v.Store}
	v.Store = recorder
	result := v.PrefetchStartReads(context.Background(), []StartRead{
		{Inode: small, Ranges: [][2]uint64{{0, 4096}}},
		// The first and second 4 MiB blocks, the first twice; past the end is clipped.
		{Inode: big, Ranges: [][2]uint64{{0, 4096}, {8192, 4096}, {5 << 20, 4096}, {12 << 20, 4096}}},
		// The overwriting slice, then the second block of the overwritten one.
		{Inode: overwritten, Ranges: [][2]uint64{{4 << 20, 4096}, {4<<20 + 8192, 4096}}},
		// Nothing for the hole; one block on each side of the chunk boundary.
		{Inode: sparse, Ranges: [][2]uint64{{0, 4096}, {meta.ChunkSize - 4096, 8192}}},
		{Inode: 999999, Ranges: [][2]uint64{{0, 4096}}},
	}, 1)

	require.Equal(t, StartReadsPrefetchResult{Files: 4, Skipped: 1, Blocks: 7}, result)
	require.Equal(t, []prefetchBlock{
		{slice: smallSlices[0].Id, sliceSize: 4096, indx: 0},
		{slice: bigSlices[0].Id, sliceSize: 10 << 20, indx: 0},
		{slice: bigSlices[0].Id, sliceSize: 10 << 20, indx: 1},
		{slice: overSlices[1].Id, sliceSize: 4096, indx: 0},
		{slice: overSlices[0].Id, sliceSize: 12 << 20, indx: 1},
		{slice: sparseFirst[1].Id, sliceSize: 4096, indx: 0},
		{slice: sparseSecond[0].Id, sliceSize: 4096, indx: 0},
	}, recorder.blocks)
}
