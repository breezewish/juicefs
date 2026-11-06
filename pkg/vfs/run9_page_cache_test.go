package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestAddTraceIntervalMergesOverlappingAndTouchingRanges(t *testing.T) {
	var intervals [][2]uint64
	intervals = addTraceInterval(intervals, 8192, 12288)
	intervals = addTraceInterval(intervals, 0, 4096)
	intervals = addTraceInterval(intervals, 20480, 24576)
	require.Equal(t, [][2]uint64{{0, 4096}, {8192, 12288}, {20480, 24576}}, intervals)

	intervals = addTraceInterval(intervals, 4096, 8192) // touches both neighbours
	require.Equal(t, [][2]uint64{{0, 12288}, {20480, 24576}}, intervals)

	intervals = addTraceInterval(intervals, 10000, 30000)
	require.Equal(t, [][2]uint64{{0, 30000}}, intervals)
}

var unboundedTrace = ReadTraceLimits{MaxFiles: 100, MaxBytes: 1 << 30, MaxListBytes: 1 << 20}

func TestReadTraceListsUnchangedFilesInFirstReadOrder(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	lib, st := v.Mkdir(ctx, 1, "lib", 0755, 0)
	require.Zero(t, st)
	writeFile := func(parent Ino, name string, size int) Ino {
		entry, fh, st := v.Create(ctx, parent, name, 0644, 0, syscall.O_RDWR)
		require.Zero(t, st)
		require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'x'}, size), 0, fh))
		require.Zero(t, v.Flush(ctx, entry.Inode, fh, 0))
		v.Release(ctx, entry.Inode, fh)
		return entry.Inode
	}
	read := func(ino Ino, off uint64, size int) {
		_, fh, st := v.Open(ctx, ino, syscall.O_RDONLY)
		require.Zero(t, st)
		n, st := v.Read(ctx, ino, make([]byte, size), off, fh)
		require.Zero(t, st)
		require.Equal(t, size, n)
		v.Release(ctx, ino, fh)
	}
	first := writeFile(1, "first", 64<<10)
	second := writeFile(lib.Inode, "second", 64<<10)
	linked := writeFile(lib.Inode, "z", 4096)
	_, st = v.Link(ctx, linked, 1, "y")
	require.Zero(t, st)
	writeFile(1, "unread", 4096)
	changed := writeFile(1, "changed", 4096)

	v.TraceReads()
	created := writeFile(1, "created", 4096)
	read(second, 0, 4096)
	read(first, 8192, 100) // a partial page traces the whole page
	read(second, 4096, 4096)
	read(first, 32768, 8192)
	read(linked, 0, 4096)
	read(created, 0, 4096)
	read(changed, 0, 4096)
	_, fh, st := v.Open(ctx, changed, syscall.O_RDWR)
	require.Zero(t, st)
	require.Zero(t, v.Write(ctx, changed, []byte("y"), 0, fh))
	require.Zero(t, v.Flush(ctx, changed, fh, 0))
	v.Release(ctx, changed, fh)

	files, err := v.ReadTrace(ctx, unboundedTrace)
	require.NoError(t, err)
	require.Equal(t, []PageCacheFile{
		{Inode: second, Path: "lib/second", Ranges: [][2]uint64{{0, 8192}}},
		{Inode: first, Path: "first", Ranges: [][2]uint64{{8192, 4096}, {32768, 8192}}},
		{Inode: linked, Path: "lib/z", Ranges: [][2]uint64{{0, 4096}}}, // also "y"; the least path wins
	}, files)
}

func TestReadTraceBoundsFilesBytesAndEncoding(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	mknod := func(name string) Ino {
		var ino Ino
		require.Zero(t, v.Meta.Mknod(ctx, 1, name, meta.TypeFile, 0644, 0, 0, "", &ino, nil))
		return ino
	}
	a, big, b, c := mknod("a"), mknod("big"), mknod("b"), mknod("c")
	v.TraceReads()
	v.readTrace.record(a, 0, 4096)
	v.readTrace.record(big, 0, 1<<20)
	v.readTrace.record(b, 0, 4096)
	v.readTrace.record(c, 0, 4096)
	fileA := PageCacheFile{Inode: a, Path: "a", Ranges: [][2]uint64{{0, 4096}}}
	fileB := PageCacheFile{Inode: b, Path: "b", Ranges: [][2]uint64{{0, 4096}}}
	fileC := PageCacheFile{Inode: c, Path: "c", Ranges: [][2]uint64{{0, 4096}}}

	// A file over the byte budget is left out; later files still fit.
	files, err := v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 100, MaxBytes: 3 * 4096, MaxListBytes: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, []PageCacheFile{fileA, fileB, fileC}, files)

	files, err = v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 2, MaxBytes: 3 * 4096, MaxListBytes: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, []PageCacheFile{fileA, fileB}, files)

	// The list ends before its JSON encoding would exceed MaxListBytes.
	encoded, err := json.Marshal([]PageCacheFile{fileA, fileB})
	require.NoError(t, err)
	files, err = v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 100, MaxBytes: 3 * 4096, MaxListBytes: len(encoded)})
	require.NoError(t, err)
	require.Equal(t, []PageCacheFile{fileA, fileB}, files)
	files, err = v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 100, MaxBytes: 3 * 4096, MaxListBytes: len(encoded) - 1})
	require.NoError(t, err)
	require.Equal(t, []PageCacheFile{fileA}, files)
}

type readdirCounter struct {
	meta.Meta
	readdirs int
}

func (m *readdirCounter) Readdir(ctx meta.Context, inode Ino, plus uint8, entries *[]*meta.Entry) syscall.Errno {
	m.readdirs++
	return m.Meta.Readdir(ctx, inode, plus, entries)
}

func TestReadTraceListsEachDirectoryOnceForManyFiles(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	var inodes []Ino
	for d := range 100 {
		var dir Ino
		require.Zero(t, v.Meta.Mkdir(ctx, 1, fmt.Sprintf("d%03d", d), 0755, 0, 0, &dir, nil))
		for f := range 200 {
			var ino Ino
			require.Zero(t, v.Meta.Mknod(ctx, dir, fmt.Sprintf("f%03d", f), meta.TypeFile, 0644, 0, 0, "", &ino, nil))
			inodes = append(inodes, ino)
		}
	}
	v.TraceReads()
	for _, ino := range inodes {
		v.readTrace.record(ino, 0, 4096)
	}
	counter := &readdirCounter{Meta: v.Meta}
	v.Meta = counter

	start := time.Now()
	files, err := v.ReadTrace(ctx, ReadTraceLimits{MaxFiles: 65536, MaxBytes: 1 << 30, MaxListBytes: 8 << 20})
	require.NoError(t, err)
	t.Logf("resolved %d files in %s", len(files), time.Since(start))
	require.Len(t, files, 20000)
	require.Equal(t, "d000/f000", files[0].Path)
	require.Equal(t, "d099/f199", files[19999].Path)
	require.Equal(t, 101, counter.readdirs) // the root and each directory once
}

func TestReadTraceStopsWhenCanceled(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	var ino Ino
	require.Zero(t, v.Meta.Mknod(ctx, 1, "a", meta.TypeFile, 0644, 0, 0, "", &ino, nil))
	v.TraceReads()
	v.readTrace.record(ino, 0, 4096)
	canceled := meta.WrapContext(context.Background())
	canceled.Cancel()
	_, err := v.ReadTrace(canceled, unboundedTrace)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReadTraceRequiresTracing(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	_, err := v.ReadTrace(NewLogContext(meta.NewContext(10, 0, []uint32{0})), unboundedTrace)
	require.ErrorContains(t, err, "does not trace reads")
}

func TestOpenGivesCacheOnlyHandlesOnlyToWarmThreads(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	entry, fh, st := v.Create(NewLogContext(meta.NewContext(10, 0, []uint32{0})), 1, "a", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	v.Release(NewLogContext(meta.NewContext(10, 0, []uint32{0})), entry.Inode, fh)
	v.cacheOnlyThreads.Store(uint32(42), struct{}{})
	cacheOnly := func(pid uint32) bool {
		ctx := NewLogContext(meta.NewContext(pid, 0, []uint32{0}))
		_, fh, st := v.Open(ctx, entry.Inode, syscall.O_RDONLY)
		require.Zero(t, st)
		defer v.Release(ctx, entry.Inode, fh)
		return v.findHandle(entry.Inode, fh).reader.(*fileReader).cacheOnly
	}

	require.True(t, cacheOnly(42))
	require.False(t, cacheOnly(43)) // another thread of the same mount, while 42 warms
	v.cacheOnlyThreads.Delete(uint32(42))
	require.False(t, cacheOnly(42)) // a thread id reused after the warm
}
