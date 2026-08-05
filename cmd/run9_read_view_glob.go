package cmd

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

const (
	run9ReadViewGlobDefaultLimit         = 50
	run9ReadViewGlobMaximumLimit         = 200
	run9ReadViewGlobMaximumPatternBytes  = 256
	run9ReadViewGlobMaximumExcludes      = 32
	run9ReadViewGlobMaximumEntries       = 100_000
	run9ReadViewGlobMaximumPathBytes     = 16 << 20
	run9ReadViewGlobMaximumDepth         = 64
	run9ReadViewGlobReadDirBatch         = 1000
	run9ReadViewGlobCancellationInterval = 256
)

type run9ReadViewGlobRequest struct {
	pattern              string
	limit                int
	scanBase             string
	scanPrefix           []string
	excludedDirectories  []string
	excludedDirectorySet map[string]struct{}
}

type run9ReadViewGlobMatch struct {
	Path string `json:"path"`
}

type run9ReadViewGlobResponse struct {
	Matches   []run9ReadViewGlobMatch `json:"matches"`
	Truncated bool                    `json:"truncated"`
}

type run9ReadViewGlobCache struct {
	key        string
	paths      []string
	incomplete bool
}

type run9ReadViewGlobScan struct {
	paths      []string
	pathBytes  int
	visited    int
	incomplete bool
	stopped    bool
}

type run9ReadViewGlobHeap []string

func (files run9ReadViewGlobHeap) Len() int { return len(files) }

// Less reverses lexical order so the largest retained path is the heap root.
func (files run9ReadViewGlobHeap) Less(left, right int) bool { return files[left] > files[right] }

func (files run9ReadViewGlobHeap) Swap(left, right int) {
	files[left], files[right] = files[right], files[left]
}

func (files *run9ReadViewGlobHeap) Push(value any) { *files = append(*files, value.(string)) }

func (files *run9ReadViewGlobHeap) Pop() any {
	old := *files
	last := old[len(old)-1]
	*files = old[:len(old)-1]
	return last
}

func (h *run9ReadViewHandler) serveGlob(w http.ResponseWriter, r *http.Request, filesystemRoot string, root string, requestPath string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "file glob requires GET", http.StatusMethodNotAllowed)
		return
	}
	globRequest, err := parseRun9ReadViewGlobRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := run9ReadViewContext(r.Context())
	fsPath, stat, errno := resolveRun9ReadViewPath(h.fs, ctx, filesystemRoot, requestPath)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	if !stat.IsDir() {
		http.Error(w, "path is not a directory", http.StatusConflict)
		return
	}
	paths, incomplete, errno := h.globPaths(ctx, fsPath, root, requestPath, globRequest)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	result, err := globRun9ReadViewPaths(r.Context(), paths, globRequest.pattern, globRequest.limit, incomplete)
	if err != nil {
		writeRun9ReadViewError(w, err)
		return
	}
	writeRun9ReadViewGlobResponse(w, result)
}

func parseRun9ReadViewGlobRequest(r *http.Request) (run9ReadViewGlobRequest, error) {
	pattern := r.URL.Query().Get("glob")
	if pattern == "" {
		return run9ReadViewGlobRequest{}, fmt.Errorf("glob pattern is required")
	}
	if len(pattern) > run9ReadViewGlobMaximumPatternBytes {
		return run9ReadViewGlobRequest{}, fmt.Errorf("glob pattern must be at most %d bytes", run9ReadViewGlobMaximumPatternBytes)
	}
	if strings.HasPrefix(pattern, "/") {
		return run9ReadViewGlobRequest{}, fmt.Errorf("glob pattern must be relative to the requested directory")
	}
	if strings.Contains(pattern, "{") {
		return run9ReadViewGlobRequest{}, fmt.Errorf("glob brace alternatives are not supported")
	}
	if !doublestar.ValidatePattern(pattern) {
		return run9ReadViewGlobRequest{}, fmt.Errorf("invalid glob pattern")
	}

	limit := run9ReadViewGlobDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > run9ReadViewGlobMaximumLimit {
			return run9ReadViewGlobRequest{}, fmt.Errorf("limit must be between 1 and %d", run9ReadViewGlobMaximumLimit)
		}
		limit = parsed
	}
	excluded := r.URL.Query()["exclude_dir"]
	if len(excluded) > run9ReadViewGlobMaximumExcludes {
		return run9ReadViewGlobRequest{}, fmt.Errorf("at most %d excluded directory names are allowed", run9ReadViewGlobMaximumExcludes)
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\\\x00") {
			return run9ReadViewGlobRequest{}, fmt.Errorf("excluded directory names must be single path components")
		}
		excludedSet[name] = struct{}{}
	}
	normalizedExcluded := make([]string, 0, len(excludedSet))
	for name := range excludedSet {
		normalizedExcluded = append(normalizedExcluded, name)
	}
	sort.Strings(normalizedExcluded)
	scanBase, _ := doublestar.SplitPattern(pattern)
	if scanBase == "." {
		scanBase = ""
	}
	var scanPrefix []string
	if scanBase != "" {
		scanPrefix = strings.Split(scanBase, "/")
	}
	return run9ReadViewGlobRequest{
		pattern:              pattern,
		limit:                limit,
		scanBase:             scanBase,
		scanPrefix:           scanPrefix,
		excludedDirectories:  normalizedExcluded,
		excludedDirectorySet: excludedSet,
	}, nil
}

func (h *run9ReadViewHandler) globPaths(ctx meta.Context, fsPath string, root string, requestPath string, request run9ReadViewGlobRequest) ([]string, bool, syscall.Errno) {
	key := root + "\x00" + requestPath + "\x00" + request.scanBase + "\x00" + strings.Join(request.excludedDirectories, "\x00")
	var ownedFlight chan struct{}
	for {
		h.globMu.Lock()
		if h.glob.key == key {
			paths := h.glob.paths
			incomplete := h.glob.incomplete
			h.globMu.Unlock()
			return paths, incomplete, 0
		}
		if h.globFlight == nil {
			ownedFlight = make(chan struct{})
			h.globFlight = ownedFlight
			h.globMu.Unlock()
			break
		}
		flight := h.globFlight
		h.globMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, syscall.EINTR
		case <-flight:
		}
	}
	defer func() {
		h.globMu.Lock()
		close(ownedFlight)
		h.globFlight = nil
		h.globMu.Unlock()
	}()

	scan := run9ReadViewGlobScan{paths: make([]string, 0, 1024)}
	if errno := collectRun9ReadViewGlobPaths(h.fs, ctx, fsPath, "", 0, request.scanPrefix, request.excludedDirectorySet, &scan); errno != 0 {
		return nil, false, errno
	}
	h.globMu.Lock()
	h.glob = run9ReadViewGlobCache{key: key, paths: scan.paths, incomplete: scan.incomplete}
	h.globMu.Unlock()
	return scan.paths, scan.incomplete, 0
}

// collectRun9ReadViewGlobPaths traverses the immutable generation with native
// bounded metadata pages. Directory entries are never resolved as paths, so
// symlinks are not followed while collecting candidates for pattern matching.
func collectRun9ReadViewGlobPaths(jfs *fs.FileSystem, ctx meta.Context, fsDirectory string, relativeDirectory string, depth int, scanPrefix []string, excluded map[string]struct{}, scan *run9ReadViewGlobScan) syscall.Errno {
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return syscall.EINTR
		}
		entries, next, errno := jfs.ReadDirPage(ctx, fsDirectory, run9ReadViewGlobReadDirBatch, cursor)
		if errno != 0 {
			return errno
		}
		for _, entry := range entries {
			if scan.visited >= run9ReadViewGlobMaximumEntries {
				scan.incomplete = true
				scan.stopped = true
				return 0
			}
			scan.visited++
			if depth < len(scanPrefix) && entry.Name() != scanPrefix[depth] {
				continue
			}
			relativePath := path.Join(relativeDirectory, entry.Name())
			switch {
			case entry.Attr().Typ == meta.TypeFile:
				if depth < len(scanPrefix) {
					continue
				}
				if scan.pathBytes+len(relativePath) > run9ReadViewGlobMaximumPathBytes {
					scan.incomplete = true
					scan.stopped = true
					return 0
				}
				scan.paths = append(scan.paths, relativePath)
				scan.pathBytes += len(relativePath)
			case entry.IsDir():
				if _, skip := excluded[entry.Name()]; skip {
					continue
				}
				if depth >= run9ReadViewGlobMaximumDepth {
					scan.incomplete = true
					continue
				}
				if errno := collectRun9ReadViewGlobPaths(jfs, ctx, path.Join(fsDirectory, entry.Name()), relativePath, depth+1, scanPrefix, excluded, scan); errno != 0 {
					return errno
				}
				if scan.stopped {
					return 0
				}
			}
		}
		if next == "" {
			return 0
		}
		cursor = next
	}
}

func globRun9ReadViewPaths(ctx context.Context, paths []string, pattern string, limit int, incomplete bool) (run9ReadViewGlobResponse, error) {
	best := make(run9ReadViewGlobHeap, 0, limit)
	heap.Init(&best)
	truncated := incomplete
	for index, filePath := range paths {
		if index%run9ReadViewGlobCancellationInterval == 0 {
			if err := ctx.Err(); err != nil {
				return run9ReadViewGlobResponse{}, err
			}
		}
		if !doublestar.MatchUnvalidated(pattern, filePath) {
			continue
		}
		if len(best) < limit {
			heap.Push(&best, filePath)
			continue
		}
		truncated = true
		if filePath < best[0] {
			heap.Pop(&best)
			heap.Push(&best, filePath)
		}
	}
	sort.Strings(best)
	response := run9ReadViewGlobResponse{Matches: make([]run9ReadViewGlobMatch, len(best)), Truncated: truncated}
	for index, filePath := range best {
		response.Matches[index] = run9ReadViewGlobMatch{Path: filePath}
	}
	return response, nil
}

func writeRun9ReadViewGlobResponse(w http.ResponseWriter, result run9ReadViewGlobResponse) {
	body, err := json.Marshal(result)
	if err != nil {
		writeRun9ReadViewError(w, err)
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}
