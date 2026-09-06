package cmd

import (
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9MountedReadViewResolvesBothFilesystemsAndSelectedRoot(t *testing.T) {
	root, data := newRun9ReadViewTestFS(t), newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, root.Mkdir(ctx, "/rootfs", 0755, 0))
	require.Zero(t, data.Mkdir(ctx, "/rootfs", 0755, 0))
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/root.txt", "root")
	writeRun9ReadViewTestFile(t, data, ctx, "/rootfs/data.txt", "data")
	require.Zero(t, root.Symlink(ctx, "/state/data/data.txt", "/rootfs/link"))
	require.Zero(t, data.Symlink(ctx, "../../root.txt", "/rootfs/back"))
	require.Zero(t, data.Symlink(ctx, "/data.txt", "/rootfs/local"))
	require.Zero(t, data.Mkdir(ctx, "/rootfs/dir", 0755, 0))
	require.Zero(t, root.Symlink(ctx, "/state/data/dir", "/rootfs/jump"))
	require.Zero(t, root.Symlink(ctx, "jump/../data.txt", "/rootfs/through-parent"))
	mounted, err := newRun9MountedReadFilesystem(ctx, root, data, "/state/data")
	require.NoError(t, err)
	handler := &run9ReadViewHandler{fs: mounted, generation: 7, dataMount: mounted.mount}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/link", nil))
	require.Equal(t, 200, response.Code)
	require.Equal(t, "data", response.Body.String())
	dataETag := response.Header().Get("ETag")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/through-parent", nil))
	require.Equal(t, 200, response.Code)
	require.Equal(t, "data", response.Body.String())
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/state/data/back", nil))
	require.Equal(t, 200, response.Code)
	require.Equal(t, "root", response.Body.String())
	require.NotEqual(t, dataETag, response.Header().Get("ETag"))

	request := httptest.NewRequest(http.MethodGet, "/local", nil)
	request.Header.Set(run9ReadViewRootHeader, "/state/data")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code)
	require.Equal(t, "data", response.Body.String())
	request = httptest.NewRequest(http.MethodGet, "/back", nil)
	request.Header.Set(run9ReadViewRootHeader, "/state/data")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, 404, response.Code)

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?glob=**%2F*.txt", nil))
	require.Equal(t, 200, response.Code)
	require.Contains(t, response.Body.String(), "state/data/data.txt")
	require.Contains(t, response.Body.String(), "root.txt")
	_, errno := root.Lstat(ctx, "/rootfs/state")
	require.Equal(t, syscall.ENOENT, errno, "read views must not create physical placeholders")
}

func TestRun9MountedReadViewPaginatesVirtualMountAncestors(t *testing.T) {
	root, data := newRun9ReadViewTestFS(t), newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, root.Mkdir(ctx, "/rootfs", 0755, 0))
	require.Zero(t, data.Mkdir(ctx, "/rootfs", 0755, 0))
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/a", "")
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/z", "")
	mounted, err := newRun9MountedReadFilesystem(ctx, root, data, "/state/data")
	require.NoError(t, err)
	page, cursor, errno := mounted.ReadDirPage(ctx, "/rootfs", 1, "")
	require.Zero(t, errno)
	require.Len(t, page, 1)
	require.Equal(t, "a", page[0].Name())
	require.Equal(t, "a", cursor)
	page, cursor, errno = mounted.ReadDirPage(ctx, "/rootfs", 1, cursor)
	require.Zero(t, errno)
	require.Len(t, page, 1)
	require.Equal(t, "state", page[0].Name())
	require.Equal(t, "state", cursor)
	page, cursor, errno = mounted.ReadDirPage(ctx, "/rootfs", 1, cursor)
	require.Zero(t, errno)
	require.Len(t, page, 1)
	require.Equal(t, "z", page[0].Name())
	require.Empty(t, cursor)
	page, cursor, errno = mounted.ReadDirPage(ctx, "/rootfs/state", 1, "")
	require.Zero(t, errno)
	require.Len(t, page, 1)
	require.Equal(t, "data", page[0].Name())
	require.Empty(t, cursor)
}

func TestRun9MountedReadViewRejectsHiddenRootDataAndSymlinks(t *testing.T) {
	root, data := newRun9ReadViewTestFS(t), newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, root.Mkdir(ctx, "/rootfs", 0755, 0))
	require.Zero(t, data.Mkdir(ctx, "/rootfs", 0755, 0))
	require.Zero(t, root.Mkdir(ctx, "/rootfs/data", 0755, 0))
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/data/keep", "original")
	_, err := newRun9MountedReadFilesystem(ctx, root, data, "/data")
	require.ErrorContains(t, err, "not empty")
	require.Zero(t, root.Symlink(ctx, "data", "/rootfs/link"))
	_, err = newRun9MountedReadFilesystem(ctx, root, data, "/link/nested")
	require.ErrorContains(t, err, "symlink")
}
