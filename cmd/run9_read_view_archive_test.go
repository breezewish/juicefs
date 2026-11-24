package cmd

import (
	"archive/tar"
	"bytes"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

type run9ArchivedEntry struct {
	typeflag           byte
	mode               int64
	uid, gid           int
	size               int64
	linkname           string
	devmajor, devminor int64
	modTime            time.Time
	data               []byte
}

func TestRun9ReadViewArchiveKeepsTreeFidelity(t *testing.T) {
	root, data := newRun9ReadViewTestFS(t), newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, root.MkdirAll(ctx, "/rootfs/out/a/b/c", 0o755, 0))
	require.Zero(t, root.Mkdir(ctx, "/rootfs/out/empty", 0o700, 0))
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/out/a/b/c/file.txt", "nested")
	// A guest chmod +x and chown live only in the override, with type bits.
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/out/tool", "#!/bin/sh\n")
	require.Zero(t, root.SetXattr(ctx, "/rootfs/out/tool", run9OverrideStatXattr, []byte("1000:1001:0100755"), 0))
	require.Zero(t, root.Link(ctx, "/rootfs/out/a/b/c/file.txt", "/rootfs/out/hard"))
	require.Zero(t, root.Symlink(ctx, "a/b/c/file.txt", "/rootfs/out/relative"))
	require.Zero(t, root.Symlink(ctx, "/etc/passwd", "/rootfs/out/absolute"))
	require.Zero(t, root.Symlink(ctx, "missing", "/rootfs/out/dangling"))
	require.Zero(t, root.Symlink(ctx, "a", "/rootfs/out/to-directory"))
	out, errno := root.Lstat(ctx, "/rootfs/out")
	require.Zero(t, errno)
	var inode meta.Ino
	var attr meta.Attr
	require.Zero(t, root.Meta().Mknod(ctx, out.Inode(), "fifo", meta.TypeFIFO, 0o620, 0, 0, "", &inode, &attr))
	// 0x100403 is new_encode_dev(4, 259): minors above 255 use the high bits.
	require.Zero(t, root.Meta().Mknod(ctx, out.Inode(), "tty", meta.TypeCharDev, 0o600, 0, 0x100403, "", &inode, &attr))
	large := make([]byte, 2*run9ReadViewArchiveRangeBytes+123)
	_, err := rand.Read(large)
	require.NoError(t, err)
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/out/large", string(large))
	modTime := time.Unix(1_700_000_000, 123_456_789)
	for _, name := range []string{"/rootfs/out/a/b/c/file.txt", "/rootfs/out/empty"} {
		file, errno := root.Open(ctx, name, 0)
		require.Zero(t, errno)
		require.Zero(t, file.Utime2(ctx, -1, -1, modTime.Unix(), int64(modTime.Nanosecond())))
		require.Zero(t, file.Close(ctx))
	}
	require.Zero(t, root.Symlink(ctx, "/out", "/rootfs/out-link"))
	// The Volume mounted at out/volume is archived in place of its placeholder.
	require.Zero(t, data.Mkdir(ctx, "/rootfs", 0o755, 0))
	require.Zero(t, data.SetXattr(ctx, "/rootfs", run9OverrideStatXattr, []byte("1000:1000:0750"), 0))
	writeRun9ReadViewTestFile(t, data, ctx, "/rootfs/inside.txt", "volume")
	mounted, err := newRun9MountedReadFilesystem(ctx, root, []run9ReadMount{{data: data, mount: "/out/volume"}})
	require.NoError(t, err)
	handler := &run9ReadViewHandler{fs: mounted, generation: 7, dataMounts: []string{mounted.volumes[0].mount}}
	server := httptest.NewServer(handler)
	defer server.Close()

	entries, order := readRun9ReadViewArchive(t, server.URL+"/out-link?archive=tar")

	for name, position := range order {
		if parent := filepath.Dir(filepath.Clean(name)) + "/"; parent != "./" {
			require.Less(t, order[parent], position, "%s precedes %s", parent, name)
		}
	}
	nested := entries["out-link/a/b/c/file.txt"]
	hard := entries["out-link/hard"]
	require.Equal(t, map[string]run9ArchivedEntry{
		"out-link/":                  {typeflag: tar.TypeDir, mode: 0o755, modTime: entries["out-link/"].modTime},
		"out-link/a/":                {typeflag: tar.TypeDir, mode: 0o755, modTime: entries["out-link/a/"].modTime},
		"out-link/a/b/":              {typeflag: tar.TypeDir, mode: 0o755, modTime: entries["out-link/a/b/"].modTime},
		"out-link/a/b/c/":            {typeflag: tar.TypeDir, mode: 0o755, modTime: entries["out-link/a/b/c/"].modTime},
		"out-link/empty/":            {typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		"out-link/tool":              {typeflag: tar.TypeReg, mode: 0o755, uid: 1000, gid: 1001, size: 10, modTime: entries["out-link/tool"].modTime, data: []byte("#!/bin/sh\n")},
		"out-link/relative":          {typeflag: tar.TypeSymlink, mode: 0o777, linkname: "a/b/c/file.txt", modTime: entries["out-link/relative"].modTime},
		"out-link/absolute":          {typeflag: tar.TypeSymlink, mode: 0o777, linkname: "/etc/passwd", modTime: entries["out-link/absolute"].modTime},
		"out-link/dangling":          {typeflag: tar.TypeSymlink, mode: 0o777, linkname: "missing", modTime: entries["out-link/dangling"].modTime},
		"out-link/to-directory":      {typeflag: tar.TypeSymlink, mode: 0o777, linkname: "a", modTime: entries["out-link/to-directory"].modTime},
		"out-link/fifo":              {typeflag: tar.TypeFifo, mode: 0o620, modTime: entries["out-link/fifo"].modTime},
		"out-link/tty":               {typeflag: tar.TypeChar, mode: 0o600, devmajor: 4, devminor: 259, modTime: entries["out-link/tty"].modTime},
		"out-link/large":             {typeflag: tar.TypeReg, mode: 0o644, size: int64(len(large)), modTime: entries["out-link/large"].modTime, data: large},
		"out-link/volume/":           {typeflag: tar.TypeDir, mode: 0o750, uid: 1000, gid: 1000, modTime: entries["out-link/volume/"].modTime},
		"out-link/volume/inside.txt": {typeflag: tar.TypeReg, mode: 0o644, size: 6, modTime: entries["out-link/volume/inside.txt"].modTime, data: []byte("volume")},
		"out-link/a/b/c/file.txt":    nested,
		"out-link/hard":              hard,
	}, entries)
	// The first name of the inode in walk order carries the content.
	require.Less(t, order["out-link/a/b/c/file.txt"], order["out-link/hard"])
	require.Equal(t, run9ArchivedEntry{typeflag: tar.TypeReg, mode: 0o644, size: 6, modTime: modTime, data: []byte("nested")}, nested)
	require.Equal(t, run9ArchivedEntry{typeflag: tar.TypeLink, mode: 0o644, linkname: "out-link/a/b/c/file.txt", modTime: modTime}, hard)
}

func TestRun9ReadViewArchiveNamesFilesAndRoot(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, jfs.MkdirAll(ctx, "/rootfs/work", 0o755, 0))
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/work/built", "binary")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	server := httptest.NewServer(handler)
	defer server.Close()

	file, _ := readRun9ReadViewArchive(t, server.URL+"/work/built?archive=tar")
	require.Len(t, file, 1)
	require.Equal(t, []byte("binary"), file["built"].data)

	root, _ := readRun9ReadViewArchive(t, server.URL+"/?archive=tar")
	require.Equal(t, []string{"./", "./work/", "./work/built"}, sortedRun9ArchiveNames(root))

	for _, tc := range []struct {
		method, target string
		status         int
	}{
		{http.MethodGet, "/missing?archive=tar", http.StatusNotFound},
		{http.MethodHead, "/work?archive=tar", http.StatusMethodNotAllowed},
		{http.MethodGet, "/work?archive=zip", http.StatusBadRequest},
		{http.MethodGet, "/work?archive=tar&list=1", http.StatusBadRequest},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.target, nil))
		require.Equal(t, tc.status, response.Code, tc.target)
	}
}

func TestRun9ReadViewArchiveAbortsAfterFirstByte(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, jfs.MkdirAll(ctx, "/rootfs/out/sub", 0o755, 0))
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/out/sub/corrupt", "x")
	require.Zero(t, jfs.SetXattr(ctx, "/rootfs/out/sub/corrupt", run9OverrideStatXattr, []byte("invalid"), 0))
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/out?archive=tar")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_, err = io.ReadAll(response.Body)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "a failed archive must not end cleanly")
	require.NoError(t, response.Body.Close())

	// A proxy that ends the aborted response cleanly, as Caddy 2.6 does,
	// still delivers an archive that every tar reader rejects, although it
	// failed right after two complete entries.
	clean := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source, err := http.Get(server.URL + r.URL.RequestURI())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer source.Body.Close()
		w.WriteHeader(source.StatusCode)
		_, _ = io.Copy(w, source.Body)
	}))
	defer clean.Close()
	response, err = http.Get(clean.URL + "/out?archive=tar")
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err, "the proxy ended the response cleanly")
	require.NoError(t, response.Body.Close())
	require.NotZero(t, len(body)%512)
	reader := tar.NewReader(bytes.NewReader(body))
	for _, name := range []string{"out/", "out/sub/"} {
		header, err := reader.Next()
		require.NoError(t, err)
		require.Equal(t, name, header.Name)
	}
	_, err = reader.Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	// Before the first byte the failure keeps its status.
	corrupt := httptest.NewRecorder()
	handler.ServeHTTP(corrupt, httptest.NewRequest(http.MethodGet, "/out/sub/corrupt?archive=tar", nil))
	require.Equal(t, http.StatusInternalServerError, corrupt.Code)
}

// A tree deeper than run9ReadViewArchiveMaxDepth fails instead of holding one
// directory page per level without bound.
func TestRun9ReadViewArchiveBoundsDepth(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	deepest := "/rootfs/out"
	for range run9ReadViewArchiveMaxDepth - 1 {
		deepest += "/d"
	}
	require.Zero(t, jfs.MkdirAll(ctx, deepest, 0o755, 0))
	server := httptest.NewServer(&run9ReadViewHandler{fs: jfs, generation: 9})
	defer server.Close()

	entries, _ := readRun9ReadViewArchive(t, server.URL+"/out?archive=tar")
	require.Len(t, entries, run9ReadViewArchiveMaxDepth)

	require.Zero(t, jfs.Mkdir(ctx, deepest+"/d", 0o755, 0))
	response, err := http.Get(server.URL + "/out?archive=tar")
	require.NoError(t, err)
	_, err = io.ReadAll(response.Body)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NoError(t, response.Body.Close())
}

// Queued headers hold the read-ahead budget, so long names cannot grow the
// queue past it.
func TestRun9ReadViewArchiveHeadersHoldBudget(t *testing.T) {
	archive := &run9ReadViewArchive{
		ctx:     t.Context(),
		items:   make(chan *run9ReadViewArchiveItem, run9ReadViewArchiveQueueItems),
		request: newRun9ReadViewArchiveBudget(4<<10, 1),
		process: newRun9ReadViewArchiveBudget(1<<20, 1),
	}
	name := string(bytes.Repeat([]byte{'a'}, 2000))
	require.NoError(t, archive.addHeader(&tar.Header{Name: name}))
	second := make(chan error, 1)
	go func() { second <- archive.addHeader(&tar.Header{Name: name}) }()
	select {
	case <-second:
		require.FailNow(t, "two 3,024-byte headers exceed a 4 KiB budget")
	case <-time.After(100 * time.Millisecond):
	}
	archive.release(<-archive.items)
	require.NoError(t, <-second)
}

// The read-ahead budget, not the file size, bounds memory: with a stalled
// client, a 256 MiB file holds at most run9ReadViewArchiveRequestBytes in the
// archive's buffers and as much again in JuiceFS read pages.
func TestRun9ReadViewArchiveBoundsMemory(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, jfs.Mkdir(ctx, "/rootfs", 0o755, 0))
	// A sparse file reads as zeros without holding its size in the test's
	// object store, whose capacity is 1 GiB.
	file, errno := jfs.Create(ctx, "/rootfs/large", 0o644, 0)
	require.Zero(t, errno)
	require.Zero(t, file.Truncate(ctx, 256<<20))
	require.Zero(t, file.Close(ctx))
	server := httptest.NewServer(&run9ReadViewHandler{fs: jfs, generation: 9})
	defer server.Close()
	baseline := run9ReadViewArchiveHeapAlloc()

	response, err := http.Get(server.URL + "/large?archive=tar")
	require.NoError(t, err)
	defer response.Body.Close()
	head := make([]byte, 1<<20)
	_, err = io.ReadFull(response.Body, head)
	require.NoError(t, err)
	// Socket buffers absorb some output; the reads ahead of it are bounded.
	require.Eventually(t, func() bool {
		return run9ReadViewArchiveHeapAlloc() > baseline+run9ReadViewArchiveRequestBytes/2
	}, 10*time.Second, 10*time.Millisecond, "the archive reads ahead of a stalled client")
	time.Sleep(200 * time.Millisecond)
	// JuiceFS reads each admitted range into its own page before copying it
	// out, so its pages are bounded by the same budget as the archive's.
	held := run9ReadViewArchiveHeapAlloc() - baseline
	require.Less(t, held, uint64(2*run9ReadViewArchiveRequestBytes+16<<20), "held %d MiB", held>>20)

	reader := tar.NewReader(io.MultiReader(bytes.NewReader(head), response.Body))
	header, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, int64(256<<20), header.Size)
	read, err := io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.Equal(t, int64(256<<20), read)
	_, err = reader.Next()
	require.Equal(t, io.EOF, err)
}

// readRun9ReadViewArchive returns every entry by name and its position.
func readRun9ReadViewArchive(t *testing.T, target string) (map[string]run9ArchivedEntry, map[string]int) {
	t.Helper()
	response, err := http.Get(target)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "application/x-tar", response.Header.Get("Content-Type"))
	entries, order := map[string]run9ArchivedEntry{}, map[string]int{}
	reader := tar.NewReader(response.Body)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		if len(data) == 0 {
			data = nil
		}
		order[header.Name] = len(order)
		entries[header.Name] = run9ArchivedEntry{
			typeflag: header.Typeflag,
			mode:     header.Mode,
			uid:      header.Uid,
			gid:      header.Gid,
			size:     header.Size,
			linkname: header.Linkname,
			devmajor: header.Devmajor,
			devminor: header.Devminor,
			modTime:  header.ModTime,
			data:     data,
		}
	}
	// Nothing follows the end-of-archive blocks.
	trailing, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Empty(t, trailing)
	return entries, order
}

func sortedRun9ArchiveNames(entries map[string]run9ArchivedEntry) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// run9ReadViewArchiveHeapAlloc returns live heap bytes. Two collections also free
// buffers idle in sync.Pools, the archive's and JuiceFS's.
func run9ReadViewArchiveHeapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}
