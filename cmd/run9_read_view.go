package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	run9ReadViewRootHeader = "X-Run9-File-Root"
	run9ReadViewModeHeader = "X-Run9-File-Mode"
	// BoxLite virtiofs keeps guest ownership and mode here when they differ
	// from the runtime-owned host inode; Linux allows user.* only on regular
	// files and directories.
	run9OverrideStatXattr        = "user.containers.override_stat"
	run9SpecialOverrideStatXattr = "system.containers.override_stat"
)

type run9ReadViewHandler struct {
	fs         run9ReadFilesystem
	dataMounts []string
	generation uint64
	globMu     sync.Mutex
	glob       run9ReadViewGlobCache
	globFlight chan struct{}
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
	Mode    uint32 `json:"mode"` // Guest permission bits, see run9ReadViewGuest.
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
	root := r.Header.Get(run9ReadViewRootHeader)
	if root == "" {
		root = "/"
	}
	root, err = validateRun9ReadViewPath(root)
	if err != nil {
		http.Error(w, "file root must be canonical", http.StatusBadRequest)
		return
	}
	ctx := run9ReadViewContext(r.Context())
	filesystemRoot, rootStat, errno := resolveRun9ReadViewPath(h.fs, ctx, run9ReadViewRoot, root)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	if !rootStat.IsDir() {
		http.Error(w, "file root is not a directory", http.StatusConflict)
		return
	}
	query := r.URL.Query()
	_, globRequested := query["glob"]
	listRequested := query.Get("list") == "1"
	if globRequested && listRequested {
		http.Error(w, "file glob and directory list cannot be combined", http.StatusBadRequest)
		return
	}
	archiveRequested, err := run9ReadViewArchiveRequested(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if archiveRequested {
		if globRequested || listRequested {
			http.Error(w, "file archive cannot be combined with glob or directory list", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "file archive requires GET", http.StatusMethodNotAllowed)
			return
		}
		h.serveArchive(w, r, filesystemRoot, requestPath)
		return
	}
	if globRequested {
		h.serveGlob(w, r, filesystemRoot, root, requestPath)
		return
	}
	if listRequested {
		h.serveList(w, r, filesystemRoot, root, requestPath)
		return
	}
	h.serveFile(w, r, filesystemRoot, requestPath)
}

func (h *run9ReadViewHandler) serveFile(w http.ResponseWriter, r *http.Request, filesystemRoot string, requestPath string) {
	ctx := run9ReadViewContext(r.Context())
	fsPath, stat, errno := resolveRun9ReadViewPath(h.fs, ctx, filesystemRoot, requestPath)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	if stat.IsDir() || stat.Attr().Typ == meta.TypeFile {
		guest, err := run9ReadViewGuest(h.fs, ctx, fsPath, stat)
		if err != nil {
			writeRun9ReadViewError(w, err)
			return
		}
		w.Header().Set(run9ReadViewModeHeader, fmt.Sprintf("%04o", guest.mode))
	}
	etag := h.fileETag(fsPath, stat)
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

func (h *run9ReadViewHandler) serveList(w http.ResponseWriter, r *http.Request, filesystemRoot string, root string, requestPath string) {
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
	cursor, err := decodeRun9ReadViewCursor(r.URL.Query().Get("cursor"), h.generation, root, requestPath)
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}
	ctx := run9ReadViewContext(r.Context())
	fsPath, _, errno := resolveRun9ReadViewPath(h.fs, ctx, filesystemRoot, requestPath)
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
		entryPath := path.Join(fsPath, entry.Name())
		guest, err := run9ReadViewGuest(h.fs, ctx, entryPath, entry)
		if err != nil {
			writeRun9ReadViewError(w, err)
			return
		}
		response.Entries = append(response.Entries, run9ReadViewListEntry{
			Name:    entry.Name(),
			Type:    run9ReadViewFileType(entry),
			Size:    entry.Size(),
			ModTime: entry.ModTime().UTC().Format(time.RFC3339Nano),
			ETag:    h.fileETag(entryPath, entry),
			Mode:    guest.mode,
		})
	}
	if next != "" {
		response.Cursor = encodeRun9ReadViewCursor(h.generation, root, requestPath, next)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	_ = json.NewEncoder(w).Encode(response)
}

// resolveRun9ReadViewPath follows symlinks as if filesystemRoot were "/".
// FileSystem's normal absolute-symlink handling is mountpoint-oriented; using
// it directly here would let an absolute target escape the selected file root.
func resolveRun9ReadViewPath(jfs run9ReadFilesystem, ctx meta.Context, filesystemRoot string, requestPath string) (string, *fs.FileStat, syscall.Errno) {
	pending := splitRun9ReadViewPath(requestPath)
	resolved := make([]string, 0, len(pending))
	if len(pending) == 0 {
		stat, errno := jfs.Lstat(ctx, filesystemRoot)
		return filesystemRoot, stat, errno
	}

	symlinks := 0
	for len(pending) > 0 {
		name := pending[0]
		pending = pending[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			if len(resolved) > 0 {
				resolved = resolved[:len(resolved)-1]
			}
			continue
		}
		candidate := path.Join(filesystemRoot, strings.Join(resolved, "/"), name)
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
		virtualTarget := string(target)
		if strings.HasPrefix(virtualTarget, "/") {
			resolved = resolved[:0]
		}
		// Resolve links before processing subsequent '..': lexical path.Clean
		// would change a target such as "link-to-other-mount/../file".
		pending = append(splitRun9ReadViewPath(virtualTarget), pending...)
	}
	result := path.Join(filesystemRoot, strings.Join(resolved, "/"))
	stat, errno := jfs.Lstat(ctx, result)
	return result, stat, errno
}

func splitRun9ReadViewPath(value string) []string {
	value = strings.TrimPrefix(value, "/")
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

func encodeRun9ReadViewCursor(generation uint64, root string, requestPath string, name string) string {
	value := strconv.FormatUint(generation, 10) + "\x00" + root + "\x00" + requestPath + "\x00" + name
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeRun9ReadViewCursor(value string, generation uint64, root string, requestPath string) (string, error) {
	if value == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "\x00", 4)
	if len(parts) != 4 || parts[0] != strconv.FormatUint(generation, 10) || parts[1] != root || parts[2] != requestPath || parts[3] == "" || strings.ContainsAny(parts[3], "/\x00") {
		return "", fmt.Errorf("cursor does not belong to this directory generation")
	}
	return parts[3], nil
}

func run9ReadViewContext(ctx context.Context) meta.Context {
	return meta.WrapContext(ctx)
}

func run9ReadViewETag(generation uint64, stat *fs.FileStat) string {
	return fmt.Sprintf("\"g%x-i%x-s%x-m%x\"", generation, uint64(stat.Inode()), uint64(stat.Size()), uint64(stat.ModTime().UnixNano()))
}

// run9ReadViewGuestStat is the ownership and permission bits the guest sees.
type run9ReadViewGuestStat struct {
	uid, gid uint32
	mode     uint32 // rwx plus setuid, setgid and sticky; never file type bits.
}

// The reader's own identity stands for guest root, as in run9 guest tar
// archives; any other inode owner is reported unchanged.
var run9ReadViewRuntimeUID, run9ReadViewRuntimeGID = uint32(os.Getuid()), uint32(os.Getgid())

// run9ReadViewGuest returns what the guest sees for one inode, the same rule
// run9 guest tar archives use: each BoxLite override-stat field when present,
// otherwise the inode's. A chown or chmod inside the guest may be recorded only
// in the override, never on the inode. Symlinks carry no override. Virtiofs
// may store file type bits in the override mode, so only its low 12 bits are
// kept. A corrupt override is an error, never a fallback. The read stays inside
// this process's metadata engine; name must already be resolved, with no
// symlink components.
func run9ReadViewGuest(jfs run9ReadFilesystem, ctx meta.Context, name string, stat *fs.FileStat) (run9ReadViewGuestStat, error) {
	attr := stat.Attr()
	guest := run9ReadViewGuestStat{uid: attr.Uid, gid: attr.Gid, mode: uint32(attr.Mode) & 0o7777}
	if guest.uid == run9ReadViewRuntimeUID {
		guest.uid = 0
	}
	if guest.gid == run9ReadViewRuntimeGID {
		guest.gid = 0
	}
	key := run9OverrideStatXattr
	switch {
	case stat.IsSymlink():
		return guest, nil
	case !stat.IsDir() && attr.Typ != meta.TypeFile:
		key = run9SpecialOverrideStatXattr
	}
	value, errno := jfs.GetXattr(ctx, name, key)
	if errno == meta.ENOATTR {
		return guest, nil
	}
	if errno != 0 {
		return run9ReadViewGuestStat{}, errno
	}
	fields := strings.Split(string(value), ":")
	if len(fields) != 3 {
		return run9ReadViewGuestStat{}, fmt.Errorf("override stat on %q must be uid:gid:mode, got %q", name, value)
	}
	for i, target := range []*uint32{&guest.uid, &guest.gid} {
		if fields[i] == "x" {
			continue
		}
		id, err := strconv.ParseUint(fields[i], 10, 32)
		if err != nil {
			return run9ReadViewGuestStat{}, fmt.Errorf("parse override stat owner on %q: %w", name, err)
		}
		*target = uint32(id)
	}
	if fields[2] != "x" {
		mode, err := strconv.ParseUint(fields[2], 8, 32)
		if err != nil {
			return run9ReadViewGuestStat{}, fmt.Errorf("parse override stat mode on %q: %w", name, err)
		}
		guest.mode = uint32(mode) & 0o7777
	}
	return guest, nil
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
			&cli.StringFlag{Name: "clean-cache-dir"},
			&cli.Uint64Flag{Name: "generation", Required: true},
			&cli.StringFlag{Name: "data-volumes", Value: "[]"},
		},
		Action: serveRun9ReadView,
	}
}

func serveRun9ReadView(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("usage: juicefs serve-read-view --listen <unix-socket> --cache-dir <dir> --generation <n> <badger-meta-url>")
	}
	metaURL := c.Args().First()
	if c.Uint64("generation") == 0 {
		return fmt.Errorf("generation must be greater than zero")
	}
	listenPath := strings.TrimSpace(c.String("listen"))
	if listenPath == "" {
		return fmt.Errorf("listen path is required")
	}
	jfs, closeRoot, err := openRun9ReadFilesystem(metaURL, c.String("cache-dir"), c.String("clean-cache-dir"))
	if err != nil {
		return err
	}
	defer closeRoot()
	handler := &run9ReadViewHandler{fs: jfs, generation: c.Uint64("generation")}
	var volumes []run9ReadMount
	identity := sha256.New()
	fmt.Fprintf(identity, "%s\x00%d", metaURL, handler.generation)
	var configs []struct {
		MetaURL       string `json:"meta_url"`
		MountPath     string `json:"mount_path"`
		Generation    uint64 `json:"generation"`
		CleanCacheDir string `json:"clean_cache_dir"`
	}
	// Use one JSON array, not a CSV-aware slice flag: metadata URLs and
	// legitimate mount paths may themselves contain commas.
	if err := json.Unmarshal([]byte(c.String("data-volumes")), &configs); err != nil {
		return fmt.Errorf("invalid data volumes: %w", err)
	}
	for i, config := range configs {
		if config.MetaURL == "" || config.MountPath == "" || config.Generation == 0 {
			return fmt.Errorf("data metadata URL, generation and mount path must be supplied together")
		}
		data, closeData, err := openRun9ReadFilesystem(config.MetaURL, filepath.Join(c.String("cache-dir"), fmt.Sprintf("data-%d", i)), config.CleanCacheDir)
		if err != nil {
			return fmt.Errorf("open data read view: %w", err)
		}
		defer closeData()
		volumes = append(volumes, run9ReadMount{data: data, mount: config.MountPath})
		fmt.Fprintf(identity, "\x00%s\x00%d\x00%s", config.MetaURL, config.Generation, config.MountPath)
	}
	if len(volumes) != 0 {
		mounted, err := newRun9MountedReadFilesystem(run9ReadViewContext(c.Context), jfs, volumes)
		if err != nil {
			return err
		}
		handler.fs = mounted
		for _, volume := range mounted.volumes {
			handler.dataMounts = append(handler.dataMounts, volume.mount)
		}
	}
	// Generations are local to each Snap. Standalone Volume views must also
	// bind cursors and validators to their metadata identity, not just its epoch.
	handler.generation = binary.BigEndian.Uint64(identity.Sum(nil)[:8])

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

	server := &http.Server{
		Handler:           handler,
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

func openRun9ReadFilesystem(metaURL, cacheDir, cleanCacheDir string) (jfs *fs.FileSystem, closeView func(), err error) {
	parsed, err := url.Parse(metaURL)
	if err != nil || parsed.Scheme != "badger" || len(parsed.Query()["readonly"]) != 1 || parsed.Query().Get("readonly") != "1" {
		return nil, nil, fmt.Errorf("read view metadata URL must request native read-only mode")
	}
	metaConf := meta.DefaultConf()
	metaConf.ReadOnly = true
	metaConf.NoBGJob = true
	metaConf.AtimeMode = meta.NoAtime
	metaCli := meta.NewClient(metaURL, metaConf)
	defer func() {
		if err != nil {
			metaCli.Shutdown()
		}
	}()
	format, err := metaCli.Load(true)
	if err != nil {
		return nil, nil, fmt.Errorf("load read view metadata: %w", err)
	}
	blob, err := NewReloadableStorage(format, metaCli, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open read view object storage: %w", err)
	}
	defer func() {
		if err != nil {
			object.Shutdown(blob)
		}
	}()
	chunkConf := getDefaultChunkConf(format)
	chunkConf.CacheDir = cacheDir
	chunkConf.CleanCacheDir = cleanCacheDir
	chunkConf.CacheSize = 1024
	chunkConf.CacheMode = 0o600
	// Match mounted filesystems: immutable block-cache reads should retain
	// kernel pages unless the operator explicitly disables the OS cache.
	chunkConf.OSCache = os.Getenv("JFS_DROP_OSCACHE") == ""
	chunkConf.AutoCreate = true
	chunkConf.Prefetch = 1
	chunkConf.SelfCheck(format.UUID)
	store := chunk.NewCachedStore(blob, *chunkConf, nil)
	if err = metaCli.NewSession(false); err != nil {
		return nil, nil, fmt.Errorf("start read-only metadata session: %w", err)
	}
	defer func() {
		if err != nil {
			_ = metaCli.CloseSession()
		}
	}()
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Version:         version.Version(),
		Chunk:           chunkConf,
		AttrTimeout:     time.Minute,
		EntryTimeout:    time.Minute,
		DirEntryTimeout: time.Minute,
	}
	jfs, err = fs.NewFileSystem(vfsConf, metaCli, store, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize read view filesystem: %w", err)
	}
	return jfs, func() {
		jfs.Close()
		_ = metaCli.CloseSession()
		object.Shutdown(blob)
		metaCli.Shutdown()
	}, nil

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
