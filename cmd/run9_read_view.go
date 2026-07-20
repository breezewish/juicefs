package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

const run9ReadViewMaxListLimit = 1000

const (
	run9ReadViewRoot       = "/rootfs"
	run9ReadViewMaxSymlink = 40
)

type run9ReadViewHandler struct {
	fs         *fs.FileSystem
	generation uint64
}

type run9ReadViewFile struct {
	file *fs.File
	ctx  meta.Context
}

func (f *run9ReadViewFile) Read(p []byte) (int, error) {
	return f.file.Read(f.ctx, p)
}

func (f *run9ReadViewFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(f.ctx, offset, whence)
}

func (f *run9ReadViewFile) Close() error {
	if errno := f.file.Close(f.ctx); errno != 0 {
		return errno
	}
	return nil
}

type run9ReadViewListEntry struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
	ETag    string `json:"etag"`
}

type run9ReadViewListResponse struct {
	Entries []run9ReadViewListEntry `json:"entries"`
	Cursor  string                  `json:"cursor,omitempty"`
}

func (h *run9ReadViewHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestPath, err := validateRun9ReadViewPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("list") == "1" {
		h.serveList(w, r, requestPath)
		return
	}
	h.serveFile(w, r, requestPath)
}

func (h *run9ReadViewHandler) serveFile(w http.ResponseWriter, r *http.Request, requestPath string) {
	ctx := run9ReadViewContext(r.Context())
	fsPath, stat, errno := resolveRun9ReadViewPath(h.fs, ctx, requestPath)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	etag := run9ReadViewETag(h.generation, stat)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	w.Header().Set("X-Run9-File-Type", run9ReadViewFileType(stat))
	if stat.IsDir() {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "path is a directory; use ?list=1", http.StatusConflict)
		return
	}
	if stat.Attr().Typ != meta.TypeFile {
		http.NotFound(w, r)
		return
	}

	file, errno := h.fs.Open(ctx, fsPath, vfs.MODE_MASK_R)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	reader := &run9ReadViewFile{file: file, ctx: ctx}
	defer reader.Close()
	http.ServeContent(w, r, path.Base(requestPath), stat.ModTime(), reader)
}

func (h *run9ReadViewHandler) serveList(w http.ResponseWriter, r *http.Request, requestPath string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "directory listing requires GET", http.StatusMethodNotAllowed)
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > run9ReadViewMaxListLimit {
			http.Error(w, "limit must be between 1 and 1000", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	cursor, err := decodeRun9ReadViewCursor(r.URL.Query().Get("cursor"), h.generation, requestPath)
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}
	ctx := run9ReadViewContext(r.Context())
	fsPath, _, errno := resolveRun9ReadViewPath(h.fs, ctx, requestPath)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	entries, next, errno := h.fs.ReadDirPage(ctx, fsPath, limit, cursor)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	response := run9ReadViewListResponse{Entries: make([]run9ReadViewListEntry, 0, len(entries))}
	for _, entry := range entries {
		response.Entries = append(response.Entries, run9ReadViewListEntry{
			Name:    entry.Name(),
			Type:    run9ReadViewFileType(entry),
			Size:    entry.Size(),
			ModTime: entry.ModTime().UTC().Format(time.RFC3339Nano),
			ETag:    run9ReadViewETag(h.generation, entry),
		})
	}
	if next != "" {
		response.Cursor = encodeRun9ReadViewCursor(h.generation, requestPath, next)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	_ = json.NewEncoder(w).Encode(response)
}

// resolveRun9ReadViewPath follows symlinks as if /rootfs were the filesystem
// root. FileSystem's normal absolute-symlink handling is mountpoint-oriented;
// using it directly here would let /foo resolve against the metadata volume
// root instead of the box root.
func resolveRun9ReadViewPath(jfs *fs.FileSystem, ctx meta.Context, requestPath string) (string, *fs.FileStat, syscall.Errno) {
	pending := splitRun9ReadViewPath(requestPath)
	resolved := make([]string, 0, len(pending))
	if len(pending) == 0 {
		stat, errno := jfs.Lstat(ctx, run9ReadViewRoot)
		return run9ReadViewRoot, stat, errno
	}

	symlinks := 0
	for len(pending) > 0 {
		name := pending[0]
		pending = pending[1:]
		candidate := path.Join(run9ReadViewRoot, strings.Join(resolved, "/"), name)
		stat, errno := jfs.Lstat(ctx, candidate)
		if errno != 0 {
			return "", nil, errno
		}
		if !stat.IsSymlink() {
			if len(pending) > 0 && !stat.IsDir() {
				return "", nil, syscall.ENOTDIR
			}
			resolved = append(resolved, name)
			if len(pending) == 0 {
				return candidate, stat, 0
			}
			continue
		}

		symlinks++
		if symlinks > run9ReadViewMaxSymlink {
			return "", nil, syscall.ELOOP
		}
		target, errno := jfs.Readlink(ctx, candidate)
		if errno != 0 {
			return "", nil, errno
		}
		if len(target) == 0 || strings.ContainsRune(string(target), '\x00') {
			return "", nil, syscall.EACCES
		}
		virtualParent := "/" + strings.Join(resolved, "/")
		virtualTarget := string(target)
		if !strings.HasPrefix(virtualTarget, "/") {
			virtualTarget = path.Join(virtualParent, virtualTarget)
		}
		pending = append(splitRun9ReadViewPath(virtualTarget), pending...)
		resolved = resolved[:0]
	}
	return "", nil, syscall.ENOENT
}

func splitRun9ReadViewPath(value string) []string {
	value = strings.Trim(path.Clean("/"+strings.TrimPrefix(value, "/")), "/")
	if value == "" {
		return nil
	}
	return strings.Split(value, "/")
}

func validateRun9ReadViewPath(value string) (string, error) {
	if value == "" || value[0] != '/' || strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("path must be absolute")
	}
	clean := path.Clean(value)
	if clean != value && value != clean+"/" {
		return "", fmt.Errorf("path must be canonical")
	}
	return clean, nil
}

func encodeRun9ReadViewCursor(generation uint64, requestPath string, name string) string {
	value := strconv.FormatUint(generation, 10) + "\x00" + requestPath + "\x00" + name
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeRun9ReadViewCursor(value string, generation uint64, requestPath string) (string, error) {
	if value == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "\x00", 3)
	if len(parts) != 3 || parts[0] != strconv.FormatUint(generation, 10) || parts[1] != requestPath || parts[2] == "" || strings.ContainsAny(parts[2], "/\x00") {
		return "", fmt.Errorf("cursor does not belong to this directory generation")
	}
	return parts[2], nil
}

func run9ReadViewContext(ctx context.Context) meta.Context {
	return meta.WrapContext(ctx)
}

func run9ReadViewETag(generation uint64, stat *fs.FileStat) string {
	return fmt.Sprintf("\"g%x-i%x-s%x-m%x\"", generation, uint64(stat.Inode()), uint64(stat.Size()), uint64(stat.ModTime().UnixNano()))
}

func run9ReadViewFileType(stat *fs.FileStat) string {
	switch {
	case stat.IsDir():
		return "directory"
	case stat.IsSymlink():
		return "symlink"
	case stat.Attr().Typ == meta.TypeFile:
		return "file"
	default:
		return "other"
	}
}

func writeRun9ReadViewError(w http.ResponseWriter, err error) {
	switch err {
	case syscall.ENOENT, syscall.ELOOP:
		http.Error(w, "not found", http.StatusNotFound)
	case syscall.EACCES, syscall.EPERM:
		http.Error(w, "forbidden", http.StatusForbidden)
	case syscall.ENOTDIR:
		http.Error(w, "not a directory", http.StatusConflict)
	case syscall.EINVAL:
		http.Error(w, "invalid request", http.StatusBadRequest)
	default:
		http.Error(w, "file read failed", http.StatusInternalServerError)
	}
}

func cmdRun9ServeReadView() *cli.Command {
	return &cli.Command{
		Name:   "serve-read-view",
		Usage:  "serve one immutable run9 metadata clone over private HTTP",
		Hidden: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "listen", Required: true},
			&cli.StringFlag{Name: "cache-dir", Required: true},
			&cli.Uint64Flag{Name: "generation", Required: true},
		},
		Action: serveRun9ReadView,
	}
}

func serveRun9ReadView(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("usage: juicefs serve-read-view --listen <unix-socket> --cache-dir <dir> --generation <n> <badger-meta-url>")
	}
	metaURL := c.Args().First()
	parsedMetaURL, err := url.Parse(metaURL)
	if err != nil || parsedMetaURL.Scheme != "badger" || len(parsedMetaURL.Query()["readonly"]) != 1 || parsedMetaURL.Query().Get("readonly") != "1" {
		return fmt.Errorf("read view metadata URL must request native read-only mode")
	}
	if c.Uint64("generation") == 0 {
		return fmt.Errorf("generation must be greater than zero")
	}
	listenPath := strings.TrimSpace(c.String("listen"))
	if listenPath == "" {
		return fmt.Errorf("listen path is required")
	}
	if err := removeRun9ReadViewSocket(listenPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", listenPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket: %w", err)
	}
	defer listener.Close()
	if err := os.Chmod(listenPath, 0o600); err != nil {
		return fmt.Errorf("chmod listen socket: %w", err)
	}
	listenerInfo, err := os.Lstat(listenPath)
	if err != nil {
		return fmt.Errorf("inspect created listen socket: %w", err)
	}
	defer func() {
		current, err := os.Lstat(listenPath)
		if err == nil && os.SameFile(listenerInfo, current) {
			_ = os.Remove(listenPath)
		}
	}()

	metaConf := meta.DefaultConf()
	metaConf.ReadOnly = true
	metaConf.NoBGJob = true
	metaConf.AtimeMode = meta.NoAtime
	metaCli := meta.NewClient(metaURL, metaConf)
	defer metaCli.Shutdown()
	format, err := metaCli.Load(true)
	if err != nil {
		return fmt.Errorf("load read view metadata: %w", err)
	}
	blob, err := NewReloadableStorage(format, metaCli, nil)
	if err != nil {
		return fmt.Errorf("open read view object storage: %w", err)
	}
	defer object.Shutdown(blob)
	chunkConf := getDefaultChunkConf(format)
	chunkConf.CacheDir = c.String("cache-dir")
	chunkConf.CacheSize = 1024
	chunkConf.CacheMode = 0o600
	chunkConf.AutoCreate = true
	chunkConf.Prefetch = 1
	chunkConf.SelfCheck(format.UUID)
	store := chunk.NewCachedStore(blob, *chunkConf, nil)
	if err := metaCli.NewSession(false); err != nil {
		return fmt.Errorf("start read-only metadata session: %w", err)
	}
	defer metaCli.CloseSession()
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Version:         version.Version(),
		Chunk:           chunkConf,
		AttrTimeout:     time.Minute,
		EntryTimeout:    time.Minute,
		DirEntryTimeout: time.Minute,
	}
	jfs, err := fs.NewFileSystem(vfsConf, metaCli, store, nil)
	if err != nil {
		return fmt.Errorf("initialize read view filesystem: %w", err)
	}
	defer jfs.Close()

	server := &http.Server{
		Handler:           &run9ReadViewHandler{fs: jfs, generation: c.Uint64("generation")},
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(c.Context, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func removeRun9ReadViewSocket(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect listen socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("listen path exists and is not a Unix socket: %q", socketPath)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove stale listen socket: %w", err)
	}
	return nil
}

var _ io.ReadSeekCloser = (*run9ReadViewFile)(nil)
