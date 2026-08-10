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
	run9ReadViewGlobMaximumRankingBytes  = 256
	run9ReadViewGlobMaximumExcludes      = 32
	run9ReadViewGlobMaximumEntries       = 100_000
	run9ReadViewGlobMaximumPathBytes     = 16 << 20
	run9ReadViewGlobMaximumDepth         = 64
	run9ReadViewGlobReadDirBatch         = 1000
	run9ReadViewGlobCancellationInterval = 256
)

type run9ReadViewGlobRequest struct {
	pattern               string
	rankingQuery          string
	limit                 int
	scanBase              string
	maximumDepth          int
	depthLimitTruncates   bool
	baseExceedsDepthLimit bool
	excludedDirectories   []string
	excludedDirectorySet  map[string]struct{}
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

type run9ReadViewGlobHeap struct {
	paths        []string
	rankingQuery string
}

func (files run9ReadViewGlobHeap) Len() int { return len(files.paths) }

// Less keeps the worst retained match at the heap root.
func (files run9ReadViewGlobHeap) Less(left, right int) bool {
	return compareRun9ReadViewGlobPaths(files.paths[left], files.paths[right], files.rankingQuery) > 0
}

func (files run9ReadViewGlobHeap) Swap(left, right int) {
	files.paths[left], files.paths[right] = files.paths[right], files.paths[left]
}

func (files *run9ReadViewGlobHeap) Push(value any) { files.paths = append(files.paths, value.(string)) }

func (files *run9ReadViewGlobHeap) Pop() any {
	old := files.paths
	last := old[len(old)-1]
	files.paths = old[:len(old)-1]
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
	result, err := globRun9ReadViewPaths(r.Context(), paths, globRequest.pattern, globRequest.rankingQuery, globRequest.limit, incomplete)
	if err != nil {
		writeRun9ReadViewError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	_ = json.NewEncoder(w).Encode(result)
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
	if hasRun9ReadViewGlobBraceAlternative(pattern) {
		return run9ReadViewGlobRequest{}, fmt.Errorf("glob brace alternatives are not supported")
	}
	if !doublestar.ValidatePattern(pattern) {
		return run9ReadViewGlobRequest{}, fmt.Errorf("invalid glob pattern")
	}
	rankingQuery := r.URL.Query().Get("ranking_query")
	if len(rankingQuery) > run9ReadViewGlobMaximumRankingBytes {
		return run9ReadViewGlobRequest{}, fmt.Errorf("ranking query must be at most %d bytes", run9ReadViewGlobMaximumRankingBytes)
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
	scanBase, remainingPattern := doublestar.SplitPattern(pattern)
	if scanBase == "." {
		scanBase = ""
	} else {
		scanBase = unescapeRun9ReadViewGlobBase(scanBase)
	}
	maximumDepth, depthLimitTruncates, baseExceedsDepthLimit := run9ReadViewGlobDepthLimit(scanBase, remainingPattern)
	return run9ReadViewGlobRequest{
		pattern:               pattern,
		rankingQuery:          rankingQuery,
		limit:                 limit,
		scanBase:              scanBase,
		maximumDepth:          maximumDepth,
		depthLimitTruncates:   depthLimitTruncates,
		baseExceedsDepthLimit: baseExceedsDepthLimit,
		excludedDirectories:   normalizedExcluded,
		excludedDirectorySet:  excludedSet,
	}, nil
}

// SplitPattern only unescapes metacharacters, while Match treats a backslash
// before any byte as an escape. The directory lookup needs the latter form.
func unescapeRun9ReadViewGlobBase(scanBase string) string {
	var unescaped strings.Builder
	unescaped.Grow(len(scanBase))
	for index := 0; index < len(scanBase); index++ {
		if scanBase[index] == '\\' && index+1 < len(scanBase) {
			index++
		}
		unescaped.WriteByte(scanBase[index])
	}
	return unescaped.String()
}

func hasRun9ReadViewGlobBraceAlternative(pattern string) bool {
	inCharacterClass := false
	for index := 0; index < len(pattern); index++ {
		if pattern[index] == '\\' {
			index++
			continue
		}
		if pattern[index] == '[' && !inCharacterClass {
			inCharacterClass = true
			continue
		}
		if pattern[index] == ']' && inCharacterClass {
			inCharacterClass = false
			continue
		}
		if pattern[index] == '{' && !inCharacterClass {
			return true
		}
	}
	return false
}

// run9ReadViewGlobDepthLimit avoids descending below levels the pattern can
// match. depthLimitTruncates is true only when the 64-level safety bound, not
// the pattern shape, is what stops traversal.
func run9ReadViewGlobDepthLimit(scanBase string, remainingPattern string) (maximumDepth int, depthLimitTruncates bool, baseExceedsDepthLimit bool) {
	baseDepth := 0
	if scanBase != "" {
		baseDepth = strings.Count(scanBase, "/") + 1
	}
	if baseDepth > run9ReadViewGlobMaximumDepth {
		return 0, true, true
	}
	safetyDepth := run9ReadViewGlobMaximumDepth - baseDepth
	for _, segment := range strings.Split(remainingPattern, "/") {
		if segment == "**" {
			return safetyDepth, true, false
		}
	}
	patternDepth := strings.Count(remainingPattern, "/")
	if patternDepth > safetyDepth {
		return safetyDepth, true, false
	}
	return patternDepth, false, false
}

func (h *run9ReadViewHandler) globPaths(ctx meta.Context, fsPath string, root string, requestPath string, request run9ReadViewGlobRequest) ([]string, bool, syscall.Errno) {
	key := root + "\x00" + requestPath + "\x00" + request.scanBase + "\x00" + strconv.Itoa(request.maximumDepth) + "\x00" + strconv.FormatBool(request.depthLimitTruncates) + "\x00" + strings.Join(request.excludedDirectories, "\x00")
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
	if request.baseExceedsDepthLimit {
		scan.incomplete = true
	} else {
		scanPath, found, errno := openRun9ReadViewGlobBase(h.fs, ctx, fsPath, request.scanBase, request.excludedDirectorySet)
		if errno != 0 {
			return nil, false, errno
		}
		if found {
			if errno := collectRun9ReadViewGlobPaths(h.fs, ctx, scanPath, request.scanBase, 0, request.maximumDepth, request.depthLimitTruncates, request.excludedDirectorySet, &scan); errno != 0 {
				return nil, false, errno
			}
		}
	}
	h.globMu.Lock()
	h.glob = run9ReadViewGlobCache{key: key, paths: scan.paths, incomplete: scan.incomplete}
	h.globMu.Unlock()
	return scan.paths, scan.incomplete, 0
}

func openRun9ReadViewGlobBase(jfs *fs.FileSystem, ctx meta.Context, directory string, scanBase string, excluded map[string]struct{}) (string, bool, syscall.Errno) {
	current := directory
	if scanBase == "" {
		return current, true, 0
	}
	for _, component := range strings.Split(scanBase, "/") {
		if component == "" || component == "." || component == ".." {
			return "", false, 0
		}
		if _, skip := excluded[component]; skip {
			return "", false, 0
		}
		current = path.Join(current, component)
		stat, errno := jfs.Lstat(ctx, current)
		if errno == syscall.ENOENT || errno == syscall.ENOTDIR {
			return "", false, 0
		}
		if errno != 0 {
			return "", false, errno
		}
		if !stat.IsDir() || stat.IsSymlink() {
			return "", false, 0
		}
	}
	return current, true, 0
}

// collectRun9ReadViewGlobPaths traverses the immutable generation with native
// bounded metadata pages. Directory entries are never resolved as paths, so
// symlinks are not followed while collecting candidates for pattern matching.
func collectRun9ReadViewGlobPaths(jfs *fs.FileSystem, ctx meta.Context, fsDirectory string, relativeDirectory string, depth int, maximumDepth int, depthLimitTruncates bool, excluded map[string]struct{}, scan *run9ReadViewGlobScan) syscall.Errno {
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
			relativePath := path.Join(relativeDirectory, entry.Name())
			switch {
			case entry.Attr().Typ == meta.TypeFile:
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
				if depth >= maximumDepth {
					if depthLimitTruncates {
						scan.incomplete = true
					}
					continue
				}
				if errno := collectRun9ReadViewGlobPaths(jfs, ctx, path.Join(fsDirectory, entry.Name()), relativePath, depth+1, maximumDepth, depthLimitTruncates, excluded, scan); errno != 0 {
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

func globRun9ReadViewPaths(ctx context.Context, paths []string, pattern string, rankingQuery string, limit int, incomplete bool) (run9ReadViewGlobResponse, error) {
	best := run9ReadViewGlobHeap{paths: make([]string, 0, limit), rankingQuery: rankingQuery}
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
		if best.Len() < limit {
			heap.Push(&best, filePath)
			continue
		}
		truncated = true
		if compareRun9ReadViewGlobPaths(filePath, best.paths[0], rankingQuery) < 0 {
			heap.Pop(&best)
			heap.Push(&best, filePath)
		}
	}
	sort.Slice(best.paths, func(left, right int) bool {
		return compareRun9ReadViewGlobPaths(best.paths[left], best.paths[right], rankingQuery) < 0
	})
	response := run9ReadViewGlobResponse{Matches: make([]run9ReadViewGlobMatch, len(best.paths)), Truncated: truncated}
	for index, filePath := range best.paths {
		response.Matches[index] = run9ReadViewGlobMatch{Path: filePath}
	}
	return response, nil
}
