package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	juicefs "github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func TestRun9ReadViewHandlerSupportsHeadRangeAndCacheValidation(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/rootfs", 0o755, 0); err != 0 {
		t.Fatalf("mkdir rootfs: %s", err)
	}
	if err := jfs.Mkdir(ctx, "/rootfs/work", 0o755, 0); err != 0 {
		t.Fatalf("mkdir: %s", err)
	}
	file, errno := jfs.Create(ctx, "/rootfs/work/index.html", 0o644, 0)
	if errno != 0 {
		t.Fatalf("create: %s", errno)
	}
	if _, errno := file.Write(ctx, []byte("hello")); errno != 0 {
		t.Fatalf("write: %s", errno)
	}
	if errno := file.Close(ctx); errno != 0 {
		t.Fatalf("close: %s", errno)
	}

	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/work/index.html", nil))
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != "5" {
		t.Fatalf("unexpected HEAD response: status=%d length=%q body=%q", head.Code, head.Header().Get("Content-Length"), head.Body.String())
	}
	etag := head.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}

	rangeResponse := httptest.NewRecorder()
	rangeRequest := httptest.NewRequest(http.MethodGet, "/work/index.html", nil)
	rangeRequest.Header.Set("Range", "bytes=1-3")
	handler.ServeHTTP(rangeResponse, rangeRequest)
	if rangeResponse.Code != http.StatusPartialContent || rangeResponse.Body.String() != "ell" {
		t.Fatalf("unexpected range response: status=%d body=%q", rangeResponse.Code, rangeResponse.Body.String())
	}
	multiRange := httptest.NewRecorder()
	multiRangeRequest := httptest.NewRequest(http.MethodGet, "/work/index.html", nil)
	multiRangeRequest.Header.Set("Range", "bytes=0-0,4-4")
	handler.ServeHTTP(multiRange, multiRangeRequest)
	mediaType, parameters, err := mime.ParseMediaType(multiRange.Header().Get("Content-Type"))
	if err != nil || multiRange.Code != http.StatusPartialContent || mediaType != "multipart/byteranges" {
		t.Fatalf("unexpected multi-range response: status=%d content-type=%q body=%q", multiRange.Code, multiRange.Header().Get("Content-Type"), multiRange.Body.String())
	}
	parts := multipart.NewReader(bytes.NewReader(multiRange.Body.Bytes()), parameters["boundary"])
	firstPart, err := parts.NextPart()
	if err != nil {
		t.Fatalf("read first range part: %s", err)
	}
	firstBody, err := io.ReadAll(firstPart)
	if err != nil || string(firstBody) != "h" {
		t.Fatalf("unexpected first range part: body=%q err=%v", firstBody, err)
	}
	lastPart, err := parts.NextPart()
	if err != nil {
		t.Fatalf("read last range part: %s", err)
	}
	lastBody, err := io.ReadAll(lastPart)
	if err != nil || string(lastBody) != "o" {
		t.Fatalf("unexpected last range part: body=%q err=%v", lastBody, err)
	}

	notModified := httptest.NewRecorder()
	conditional := httptest.NewRequest(http.MethodGet, "/work/index.html", nil)
	conditional.Header.Set("If-None-Match", etag)
	handler.ServeHTTP(notModified, conditional)
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", notModified.Code)
	}
}

func TestRun9ReadViewHandlerKeepsSymlinksInsideBoxRoot(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/rootfs", 0o755, 0); err != 0 {
		t.Fatalf("mkdir rootfs: %s", err)
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/target.txt", "inside")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/private.txt", "outside")
	if err := jfs.Mkdir(ctx, "/rootfs/dir", 0o755, 0); err != 0 {
		t.Fatalf("mkdir nested directory: %s", err)
	}
	if errno := jfs.Symlink(ctx, "/target.txt", "/rootfs/inside-link"); errno != 0 {
		t.Fatalf("create inside symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "../target.txt", "/rootfs/dir/relative-link"); errno != 0 {
		t.Fatalf("create relative symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "/private.txt", "/rootfs/escape-link"); errno != 0 {
		t.Fatalf("create escape symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "../../private.txt", "/rootfs/dir/relative-escape-link"); errno != 0 {
		t.Fatalf("create relative escape symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "loop-b", "/rootfs/loop-a"); errno != 0 {
		t.Fatalf("create first loop symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "loop-a", "/rootfs/loop-b"); errno != 0 {
		t.Fatalf("create second loop symlink: %s", errno)
	}

	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	inside := httptest.NewRecorder()
	handler.ServeHTTP(inside, httptest.NewRequest(http.MethodGet, "/inside-link", nil))
	if inside.Code != http.StatusOK || inside.Body.String() != "inside" {
		t.Fatalf("absolute symlink did not resolve inside box root: status=%d body=%q", inside.Code, inside.Body.String())
	}
	relative := httptest.NewRecorder()
	handler.ServeHTTP(relative, httptest.NewRequest(http.MethodGet, "/dir/relative-link", nil))
	if relative.Code != http.StatusOK || relative.Body.String() != "inside" {
		t.Fatalf("relative symlink did not resolve inside box root: status=%d body=%q", relative.Code, relative.Body.String())
	}

	for _, requestPath := range []string{"/escape-link", "/dir/relative-escape-link", "/loop-a"} {
		escape := httptest.NewRecorder()
		handler.ServeHTTP(escape, httptest.NewRequest(http.MethodGet, requestPath, nil))
		if escape.Code != http.StatusNotFound || strings.Contains(escape.Body.String(), "outside") {
			t.Fatalf("symlink %s escaped box root or looped: status=%d body=%q", requestPath, escape.Code, escape.Body.String())
		}
	}
}

func TestRun9ReadViewHandlerKeepsSymlinksInsideSelectedRoot(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/workspace", "/rootfs/etc"} {
		if err := jfs.Mkdir(ctx, directory, 0o755, 0); err != 0 {
			t.Fatalf("mkdir %s: %s", directory, err)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/workspace/inside.txt", "inside")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/etc/secret", "outside")
	if errno := jfs.Symlink(ctx, "/inside.txt", "/rootfs/workspace/inside-link"); errno != 0 {
		t.Fatalf("create inside symlink: %s", errno)
	}
	if errno := jfs.Symlink(ctx, "/etc/secret", "/rootfs/workspace/escape-link"); errno != 0 {
		t.Fatalf("create escape symlink: %s", errno)
	}

	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	insideRequest := httptest.NewRequest(http.MethodGet, "/inside-link", nil)
	insideRequest.Header.Set(run9ReadViewRootHeader, "/workspace")
	inside := httptest.NewRecorder()
	handler.ServeHTTP(inside, insideRequest)
	if inside.Code != http.StatusOK || inside.Body.String() != "inside" {
		t.Fatalf("absolute symlink did not resolve inside selected root: status=%d body=%q", inside.Code, inside.Body.String())
	}

	escapeRequest := httptest.NewRequest(http.MethodGet, "/escape-link", nil)
	escapeRequest.Header.Set(run9ReadViewRootHeader, "/workspace")
	escape := httptest.NewRecorder()
	handler.ServeHTTP(escape, escapeRequest)
	if escape.Code != http.StatusNotFound || strings.Contains(escape.Body.String(), "outside") {
		t.Fatalf("symlink escaped selected root: status=%d body=%q", escape.Code, escape.Body.String())
	}

	invalidRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	invalidRequest.Header.Set(run9ReadViewRootHeader, "/workspace/../etc")
	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, invalidRequest)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid root rejection, got status=%d body=%q", invalid.Code, invalid.Body.String())
	}
}

func writeRun9ReadViewTestFile(t *testing.T, jfs *juicefs.FileSystem, ctx meta.Context, name string, content string) {
	t.Helper()
	file, errno := jfs.Create(ctx, name, 0o644, 0)
	if errno != 0 {
		t.Fatalf("create %s: %s", name, errno)
	}
	if _, errno := file.Write(ctx, []byte(content)); errno != 0 {
		t.Fatalf("write %s: %s", name, errno)
	}
	if errno := file.Close(ctx); errno != 0 {
		t.Fatalf("close %s: %s", name, errno)
	}
}

func TestRemoveRun9ReadViewSocketPreservesNonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reader.sock")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write file: %s", err)
	}
	if err := removeRun9ReadViewSocket(path); err == nil {
		t.Fatal("expected non-socket listen path to be rejected")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "keep" {
		t.Fatalf("listen path was modified: content=%q err=%v", raw, err)
	}
}

func TestRun9ReadViewHandlerListsBoundedPages(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/rootfs", 0o755, 0); err != 0 {
		t.Fatalf("mkdir rootfs: %s", err)
	}
	if err := jfs.Mkdir(ctx, "/rootfs/work", 0o755, 0); err != 0 {
		t.Fatalf("mkdir: %s", err)
	}
	for _, name := range []string{"c.png", "a.html", "b.js"} {
		file, errno := jfs.Create(ctx, "/rootfs/work/"+name, 0o644, 0)
		if errno != 0 {
			t.Fatalf("create %s: %s", name, errno)
		}
		_ = file.Close(ctx)
	}

	handler := &run9ReadViewHandler{fs: jfs, generation: 4}
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/work?list=1&limit=2", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first page status=%d body=%q", first.Code, first.Body.String())
	}
	var firstPage run9ReadViewListResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %s", err)
	}
	if len(firstPage.Entries) != 2 || firstPage.Entries[0].Name != "a.html" || firstPage.Entries[1].Name != "b.js" || firstPage.Cursor == "" {
		t.Fatalf("unexpected first page: %+v", firstPage)
	}

	last := httptest.NewRecorder()
	handler.ServeHTTP(last, httptest.NewRequest(http.MethodGet, "/work?list=1&limit=2&cursor="+firstPage.Cursor, nil))
	var lastPage run9ReadViewListResponse
	if err := json.Unmarshal(last.Body.Bytes(), &lastPage); err != nil {
		t.Fatalf("decode last page: %s", err)
	}
	if len(lastPage.Entries) != 1 || lastPage.Entries[0].Name != "c.png" || lastPage.Cursor != "" {
		t.Fatalf("unexpected last page: %+v", lastPage)
	}

	wrongDirectory := httptest.NewRecorder()
	handler.ServeHTTP(wrongDirectory, httptest.NewRequest(http.MethodGet, "/?list=1&cursor="+firstPage.Cursor, nil))
	if wrongDirectory.Code != http.StatusBadRequest {
		t.Fatalf("expected cross-directory cursor rejection, got status=%d body=%q", wrongDirectory.Code, wrongDirectory.Body.String())
	}

	wrongRootRequest := httptest.NewRequest(http.MethodGet, "/work?list=1&cursor="+firstPage.Cursor, nil)
	wrongRootRequest.Header.Set(run9ReadViewRootHeader, "/work")
	wrongRoot := httptest.NewRecorder()
	handler.ServeHTTP(wrongRoot, wrongRootRequest)
	if wrongRoot.Code != http.StatusBadRequest {
		t.Fatalf("expected cross-root cursor rejection, got status=%d body=%q", wrongRoot.Code, wrongRoot.Body.String())
	}
}

func TestRun9ReadViewHandlerSearchesRanksAndExcludesFiles(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/work", "/rootfs/work/src", "/rootfs/work/src/node_modules", "/rootfs/work/.git"} {
		if err := jfs.Mkdir(ctx, directory, 0o755, 0); err != 0 {
			t.Fatalf("mkdir %s: %s", directory, err)
		}
	}
	for _, name := range []string{"/rootfs/work/src/search_helper.go", "/rootfs/work/src/session_search.go", "/rootfs/work/src/node_modules/search_vendor.go", "/rootfs/work/.git/search-index"} {
		writeRun9ReadViewTestFile(t, jfs, ctx, name, "")
	}
	if errno := jfs.Symlink(ctx, "search_helper.go", "/rootfs/work/src/search-link"); errno != 0 {
		t.Fatalf("create symlink: %s", errno)
	}

	handler := &run9ReadViewHandler{fs: jfs, generation: 9}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/work?search=1&q=search&limit=2&exclude_dir=.git&exclude_dir=node_modules", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("search status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewSearchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode search response: %s", err)
	}
	want := []run9ReadViewSearchMatch{{Path: "src/search_helper.go"}, {Path: "src/session_search.go"}}
	if len(result.Matches) != len(want) || result.Matches[0] != want[0] || result.Matches[1] != want[1] || result.Truncated {
		t.Fatalf("unexpected search response: %+v", result)
	}

	bounded := httptest.NewRecorder()
	handler.ServeHTTP(bounded, httptest.NewRequest(http.MethodGet,
		"/work?search=1&q=&limit=1&exclude_dir=.git&exclude_dir=node_modules", nil))
	if err := json.Unmarshal(bounded.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode bounded search response: %s", err)
	}
	if len(result.Matches) != 1 || !result.Truncated || result.Matches[0].Path == "src/search-link" {
		t.Fatalf("unexpected bounded response: %+v", result)
	}
}

func TestRun9ReadViewHandlerSearchRejectsInvalidRequests(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/rootfs", 0o755, 0); err != 0 {
		t.Fatalf("mkdir rootfs: %s", err)
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/file.go", "")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	tests := []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/?search=1&limit=201", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?search=1&exclude_dir=src/generated", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?search=1&list=1", status: http.StatusBadRequest},
		{method: http.MethodHead, path: "/?search=1", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/file.go?search=1", status: http.StatusConflict},
	}
	for _, test := range tests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s: got status=%d body=%q, want %d", test.method, test.path, response.Code, response.Body.String(), test.status)
		}
	}
}

func TestRun9ReadViewSearchKeepsBestBoundedMatches(t *testing.T) {
	result := searchRun9ReadViewPaths([]string{
		"nested/search.go.bak",
		"deep/search.go",
		"other/search-guide.md",
	}, "search.go", 1, false)
	if len(result.Matches) != 1 || result.Matches[0].Path != "deep/search.go" || !result.Truncated {
		t.Fatalf("unexpected bounded search response: %+v", result)
	}
}

func newRun9ReadViewTestFS(t *testing.T) *juicefs.FileSystem {
	t.Helper()
	metadata := meta.NewClient("memkv://", nil)
	format := &meta.Format{Name: "test", UUID: "test-uuid", BlockSize: 4096, Capacity: 1 << 30, DirStats: true}
	if err := metadata.Init(format, true); err != nil {
		t.Fatalf("init metadata: %s", err)
	}
	chunkConf := &chunk.Config{
		BlockSize:   format.BlockSize << 10,
		MaxUpload:   1,
		MaxDownload: 20,
		BufferSize:  32 << 20,
		GetTimeout:  time.Minute,
		PutTimeout:  time.Minute,
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("create object storage: %s", err)
	}
	store := chunk.NewCachedStore(blob, *chunkConf, nil)
	jfs, err := juicefs.NewFileSystem(&vfs.Config{
		Meta:            meta.DefaultConf(),
		Format:          *format,
		Chunk:           chunkConf,
		AttrTimeout:     time.Minute,
		EntryTimeout:    time.Minute,
		DirEntryTimeout: time.Minute,
	}, metadata, store, nil)
	if err != nil {
		t.Fatalf("initialize filesystem: %s", err)
	}
	t.Cleanup(func() {
		_ = jfs.Close()
		_ = metadata.Shutdown()
		object.Shutdown(blob)
	})
	return jfs
}
