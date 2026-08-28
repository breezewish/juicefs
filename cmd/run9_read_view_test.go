package cmd

import (
	"bytes"
	"context"
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

func TestRun9ReadViewHandlerGlobsBoundsAndExcludesFiles(t *testing.T) {
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
		"/work?glob=**%2F%2Asearch%2A.go&limit=2&ranking_query=search&exclude_dir=.git&exclude_dir=node_modules", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode glob response: %s", err)
	}
	want := []run9ReadViewGlobMatch{{Path: "src/search_helper.go"}, {Path: "src/session_search.go"}}
	if len(result.Matches) != len(want) || result.Matches[0] != want[0] || result.Matches[1] != want[1] || result.Truncated {
		t.Fatalf("unexpected glob response: %+v", result)
	}

	bounded := httptest.NewRecorder()
	handler.ServeHTTP(bounded, httptest.NewRequest(http.MethodGet,
		"/work?glob=**%2F%2A&limit=1&exclude_dir=.git&exclude_dir=node_modules", nil))
	if err := json.Unmarshal(bounded.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode bounded glob response: %s", err)
	}
	if len(result.Matches) != 1 || !result.Truncated || result.Matches[0].Path != "src/search_helper.go" {
		t.Fatalf("unexpected bounded response: %+v", result)
	}
}

func TestRun9ReadViewHandlerGlobRespectsNestedGitIgnoreRules(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/generated", "/rootfs/src"} {
		if errno := jfs.Mkdir(ctx, directory, 0o755, 0); errno != 0 {
			t.Fatalf("mkdir %s: %s", directory, errno)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/.gitignore", "generated/\n*.tmp\n*.log\n!keep.log\n")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/src/.gitignore", "!important.tmp\nignored.go\n")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/generated/.gitignore", "!keep.go\n")
	for _, name := range []string{
		"/rootfs/main.go",
		"/rootfs/debug.log",
		"/rootfs/keep.log",
		"/rootfs/generated/keep.go",
		"/rootfs/src/main.go",
		"/rootfs/src/drop.tmp",
		"/rootfs/src/important.tmp",
		"/rootfs/src/ignored.go",
	} {
		writeRun9ReadViewTestFile(t, jfs, ctx, name, "")
	}
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/?glob=**%2F%2A.go&respect_gitignore=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode glob response: %s", err)
	}
	want := []run9ReadViewGlobMatch{{Path: "main.go"}, {Path: "src/main.go"}}
	if len(result.Matches) != len(want) || result.Matches[0] != want[0] || result.Matches[1] != want[1] {
		t.Fatalf("unexpected gitignore glob response: %+v", result)
	}

	literalBase := httptest.NewRecorder()
	handler.ServeHTTP(literalBase, httptest.NewRequest(http.MethodGet,
		"/?glob=src%2F**%2F%2A.tmp&respect_gitignore=1", nil))
	if err := json.Unmarshal(literalBase.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode literal-base response: %s", err)
	}
	if literalBase.Code != http.StatusOK || len(result.Matches) != 1 || result.Matches[0].Path != "src/important.tmp" {
		t.Fatalf("unexpected literal-base response: status=%d result=%+v", literalBase.Code, result)
	}

	excludedParent := httptest.NewRecorder()
	handler.ServeHTTP(excludedParent, httptest.NewRequest(http.MethodGet,
		"/?glob=generated%2F**%2F%2A.go&respect_gitignore=1", nil))
	if err := json.Unmarshal(excludedParent.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode excluded-parent response: %s", err)
	}
	if excludedParent.Code != http.StatusOK || len(result.Matches) != 0 {
		t.Fatalf("unexpected excluded-parent response: status=%d result=%+v", excludedParent.Code, result)
	}

	unfiltered := httptest.NewRecorder()
	handler.ServeHTTP(unfiltered, httptest.NewRequest(http.MethodGet,
		"/?glob=generated%2F**%2F%2A.go", nil))
	if err := json.Unmarshal(unfiltered.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode unfiltered response: %s", err)
	}
	if unfiltered.Code != http.StatusOK || len(result.Matches) != 1 || result.Matches[0].Path != "generated/keep.go" {
		t.Fatalf("unexpected unfiltered response: status=%d result=%+v", unfiltered.Code, result)
	}
}

func TestRun9ReadViewGlobRejectsOversizedGitIgnore(t *testing.T) {
	rules := newRun9ReadViewGlobIgnoreRules()
	if _, err := rules.add("", make([]byte, run9ReadViewGlobMaximumGitIgnoreFileBytes+1)); err == nil || err.Error() != "gitignore data exceeds file glob limits" {
		t.Fatalf("unexpected oversized gitignore error: %v", err)
	}
	if _, err := rules.add("", []byte(strings.Repeat("x", run9ReadViewGlobMaximumGitIgnorePatternBytes+1)+"\n")); err == nil || err.Error() != "gitignore pattern exceeds file glob limits" {
		t.Fatalf("unexpected oversized gitignore pattern error: %v", err)
	}
	if _, err := newRun9ReadViewGlobIgnoreRules().add("", append([]byte{0xef, 0xbb, 0xbf}, []byte(strings.Repeat("x", run9ReadViewGlobMaximumGitIgnorePatternBytes)+"\r\n")...)); err != nil {
		t.Fatalf("unexpected normalized gitignore pattern error: %v", err)
	}
	if _, err := newRun9ReadViewGlobIgnoreRules().add("", []byte(strings.Repeat("x\r", 5000))); err != nil {
		t.Fatalf("unexpected CR-separated gitignore pattern error: %v", err)
	}
	if _, err := newRun9ReadViewGlobIgnoreRules().add("", []byte(strings.Repeat("x\n", run9ReadViewGlobMaximumGitIgnorePatterns))); err != nil {
		t.Fatalf("unexpected gitignore pattern count error at limit: %v", err)
	}
	if _, err := newRun9ReadViewGlobIgnoreRules().add("", []byte(strings.Repeat("x\n", run9ReadViewGlobMaximumGitIgnorePatterns+1))); err == nil || err.Error() != "gitignore pattern count exceeds file glob limits" {
		t.Fatalf("unexpected gitignore pattern count error: %v", err)
	}
}

func TestRun9ReadViewGlobIgnoreRulesKeepSiblingScopesSeparate(t *testing.T) {
	rules := newRun9ReadViewGlobIgnoreRules()
	left, err := rules.add("left", []byte("*.tmp\n"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := rules.add("right", []byte("*.log\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left.matchers) != 1 || len(right.matchers) != 1 || rules.budget.patterns != 2 {
		t.Fatalf("sibling scopes share matchers: left=%d right=%d patterns=%d", len(left.matchers), len(right.matchers), rules.budget.patterns)
	}
}

func TestRun9ReadViewGlobIgnoreRulesBoundMatchingWork(t *testing.T) {
	rules, err := newRun9ReadViewGlobIgnoreRules().add("", []byte("*.tmp\n"))
	if err != nil {
		t.Fatal(err)
	}
	rules.budget.ruleEvaluations = run9ReadViewGlobMaximumGitIgnoreRuleEvaluations - 1
	if _, err := rules.ignores("dir/file.tmp", false); err == nil || err.Error() != "gitignore matching work exceeds file glob limits" {
		t.Fatalf("unexpected gitignore matching work error: %v", err)
	}
}

func TestRun9ReadViewHandlerGlobRejectsInvalidRequests(t *testing.T) {
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
		{method: http.MethodGet, path: "/?glob=", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%5B", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2F%2A.go", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%7Ba%2Cb%7D.go", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&limit=201", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&ranking_query=" + strings.Repeat("a", run9ReadViewGlobMaximumRankingBytes+1), status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&exclude_dir=src%2Fgenerated", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&respect_gitignore=invalid", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&respect_gitignore=1&respect_gitignore=1", status: http.StatusBadRequest},
		{method: http.MethodGet, path: "/?glob=%2A&list=1", status: http.StatusBadRequest},
		{method: http.MethodHead, path: "/?glob=%2A", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/file.go?glob=%2A", status: http.StatusConflict},
	}
	for _, test := range tests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s: got status=%d body=%q, want %d", test.method, test.path, response.Code, response.Body.String(), test.status)
		}
	}
}

func TestRun9ReadViewHandlerGlobMatchesEscapedLiteralBrace(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	if err := jfs.Mkdir(ctx, "/rootfs", 0o755, 0); err != 0 {
		t.Fatalf("mkdir rootfs: %s", err)
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/{foo}.go", "")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/?glob=%5C%7Bfoo%5C%7D.go", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode glob response: %s", err)
	}
	if len(result.Matches) != 1 || result.Matches[0].Path != "{foo}.go" || result.Truncated {
		t.Fatalf("unexpected glob response: %+v", result)
	}

	characterClass := httptest.NewRecorder()
	handler.ServeHTTP(characterClass, httptest.NewRequest(http.MethodGet,
		"/?glob=%5B%7B%5Dfoo%5B%7D%5D.go", nil))
	if characterClass.Code != http.StatusOK {
		t.Fatalf("character class glob status=%d body=%q", characterClass.Code, characterClass.Body.String())
	}
	if err := json.Unmarshal(characterClass.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode character class glob response: %s", err)
	}
	if len(result.Matches) != 1 || result.Matches[0].Path != "{foo}.go" || result.Truncated {
		t.Fatalf("unexpected character class glob response: %+v", result)
	}
}

func TestRun9ReadViewHandlerGlobPrunesLiteralBase(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/src", "/rootfs/other"} {
		if errno := jfs.Mkdir(ctx, directory, 0o755, 0); errno != 0 {
			t.Fatalf("mkdir %s: %s", directory, errno)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/src/src.go", "")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/other/other.go", "")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?glob=src%2F**%2F%2A.go", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	handler.globMu.Lock()
	defer handler.globMu.Unlock()
	if len(handler.glob.paths) != 1 || handler.glob.paths[0] != "src/src.go" {
		t.Fatalf("unexpected cached paths: %v", handler.glob.paths)
	}
}

func TestRun9ReadViewHandlerGlobDoesNotScanBelowPatternDepth(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/nested"} {
		if errno := jfs.Mkdir(ctx, directory, 0o755, 0); errno != 0 {
			t.Fatalf("mkdir %s: %s", directory, errno)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/root.go", "")
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/nested/child.go", "")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?glob=%2A.go", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	handler.globMu.Lock()
	if len(handler.glob.paths) != 1 || handler.glob.paths[0] != "root.go" {
		t.Fatalf("unexpected cached paths: %v", handler.glob.paths)
	}
	handler.globMu.Unlock()

	recursive := httptest.NewRecorder()
	handler.ServeHTTP(recursive, httptest.NewRequest(http.MethodGet, "/?glob=**%2F%2A.go", nil))
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(recursive.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode recursive glob response: %s", err)
	}
	want := []run9ReadViewGlobMatch{{Path: "nested/child.go"}, {Path: "root.go"}}
	if recursive.Code != http.StatusOK || len(result.Matches) != 2 || result.Matches[0] != want[0] || result.Matches[1] != want[1] {
		t.Fatalf("unexpected recursive glob response: status=%d result=%+v", recursive.Code, result)
	}
}

func TestRun9ReadViewHandlerGlobDoesNotFollowLiteralBaseSymlink(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/target"} {
		if errno := jfs.Mkdir(ctx, directory, 0o755, 0); errno != 0 {
			t.Fatalf("mkdir %s: %s", directory, errno)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/target/secret.go", "")
	if errno := jfs.Symlink(ctx, "target", "/rootfs/alias"); errno != 0 {
		t.Fatalf("create symlink: %s", errno)
	}
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/?glob=alias%2F**%2F%2A.go", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode glob response: %s", err)
	}
	if len(result.Matches) != 0 || result.Truncated {
		t.Fatalf("unexpected glob response: %+v", result)
	}
}

func TestRun9ReadViewHandlerGlobUnescapesLiteralBase(t *testing.T) {
	jfs := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	for _, directory := range []string{"/rootfs", "/rootfs/fooa"} {
		if errno := jfs.Mkdir(ctx, directory, 0o755, 0); errno != 0 {
			t.Fatalf("mkdir %s: %s", directory, errno)
		}
	}
	writeRun9ReadViewTestFile(t, jfs, ctx, "/rootfs/fooa/file.go", "")
	handler := &run9ReadViewHandler{fs: jfs, generation: 9}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/?glob=foo%5Ca%2F%2A.go", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("glob status=%d body=%q", response.Code, response.Body.String())
	}
	var result run9ReadViewGlobResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode glob response: %s", err)
	}
	if len(result.Matches) != 1 || result.Matches[0].Path != "fooa/file.go" || result.Truncated {
		t.Fatalf("unexpected glob response: %+v", result)
	}
}

func TestRun9ReadViewGlobKeepsLexicalTopN(t *testing.T) {
	result, err := globRun9ReadViewPaths(context.Background(), []string{
		"z.go",
		"nested/b.go",
		"a.go",
		"README.md",
	}, "**/*.go", "", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Matches) != 2 || result.Matches[0].Path != "a.go" || result.Matches[1].Path != "nested/b.go" || !result.Truncated {
		t.Fatalf("unexpected bounded glob response: %+v", result)
	}
}

func TestRun9ReadViewGlobRanksBeforeApplyingLimit(t *testing.T) {
	result, err := globRun9ReadViewPaths(context.Background(), []string{
		"a-transcriptview/preview.go",
		"a-transcriptview/viewer.go",
		"zeta/view.go",
	}, "**/*view*", "view", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Matches) != 2 || result.Matches[0].Path != "zeta/view.go" || result.Matches[1].Path != "a-transcriptview/viewer.go" || !result.Truncated {
		t.Fatalf("unexpected ranked glob response: %+v", result)
	}
}

func TestRun9ReadViewGlobOrdersSameNamesByDirectory(t *testing.T) {
	result, err := globRun9ReadViewPaths(context.Background(), []string{
		"src/bravo/view.go",
		"src/alpha/view.go",
	}, "**/view.go", "view.go", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Matches) != 2 || result.Matches[0].Path != "src/alpha/view.go" || result.Matches[1].Path != "src/bravo/view.go" || result.Truncated {
		t.Fatalf("unexpected same-name glob response: %+v", result)
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
