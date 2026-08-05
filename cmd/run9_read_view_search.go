package cmd

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

const (
	run9ReadViewSearchDefaultLimit      = 50
	run9ReadViewSearchMaximumLimit      = 200
	run9ReadViewSearchMaximumQueryBytes = 256
	run9ReadViewSearchMaximumExcludes   = 32
	run9ReadViewSearchMaximumEntries    = 100_000
	run9ReadViewSearchMaximumPathBytes  = 16 << 20
	run9ReadViewSearchMaximumDepth      = 64
	run9ReadViewSearchReadDirBatch      = 1000
)

type run9ReadViewSearchRequest struct {
	query                string
	limit                int
	excludedDirectories  []string
	excludedDirectorySet map[string]struct{}
}

type run9ReadViewSearchMatch struct {
	Path string `json:"path"`
}

type run9ReadViewSearchResponse struct {
	Matches   []run9ReadViewSearchMatch `json:"matches"`
	Truncated bool                      `json:"truncated"`
}

type run9ReadViewSearchCache struct {
	key        string
	paths      []string
	incomplete bool
}

type run9ReadViewSearchScan struct {
	paths      []string
	pathBytes  int
	visited    int
	incomplete bool
	stopped    bool
}

type run9ReadViewSearchRank struct {
	scope    int
	kind     int
	gaps     int
	start    int
	depth    int
	length   int
	folded   string
	original string
}

type run9ReadViewRankedFile struct {
	path string
	rank run9ReadViewSearchRank
}

type run9ReadViewRankedFileHeap []run9ReadViewRankedFile

func (files run9ReadViewRankedFileHeap) Len() int { return len(files) }

func (files run9ReadViewRankedFileHeap) Less(left, right int) bool {
	return run9ReadViewSearchRankLess(files[right].rank, files[left].rank)
}

func (files run9ReadViewRankedFileHeap) Swap(left, right int) {
	files[left], files[right] = files[right], files[left]
}

func (files *run9ReadViewRankedFileHeap) Push(value any) {
	*files = append(*files, value.(run9ReadViewRankedFile))
}

func (files *run9ReadViewRankedFileHeap) Pop() any {
	old := *files
	last := old[len(old)-1]
	*files = old[:len(old)-1]
	return last
}

func (h *run9ReadViewHandler) serveSearch(w http.ResponseWriter, r *http.Request, filesystemRoot string, root string, requestPath string) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "file search requires GET", http.StatusMethodNotAllowed)
		return
	}
	request, err := parseRun9ReadViewSearchRequest(r)
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
	paths, incomplete, errno := h.searchPaths(ctx, fsPath, root, requestPath, request)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	writeRun9ReadViewSearchResponse(w, searchRun9ReadViewPaths(paths, request.query, request.limit, incomplete))
}

func parseRun9ReadViewSearchRequest(r *http.Request) (run9ReadViewSearchRequest, error) {
	query := r.URL.Query().Get("q")
	if len(query) > run9ReadViewSearchMaximumQueryBytes {
		return run9ReadViewSearchRequest{}, fmt.Errorf("search query must be at most %d bytes", run9ReadViewSearchMaximumQueryBytes)
	}
	limit := run9ReadViewSearchDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > run9ReadViewSearchMaximumLimit {
			return run9ReadViewSearchRequest{}, fmt.Errorf("limit must be between 1 and %d", run9ReadViewSearchMaximumLimit)
		}
		limit = parsed
	}
	excluded := r.URL.Query()["exclude_dir"]
	if len(excluded) > run9ReadViewSearchMaximumExcludes {
		return run9ReadViewSearchRequest{}, fmt.Errorf("at most %d excluded directory names are allowed", run9ReadViewSearchMaximumExcludes)
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\\\x00") {
			return run9ReadViewSearchRequest{}, fmt.Errorf("excluded directory names must be single path components")
		}
		excludedSet[name] = struct{}{}
	}
	normalizedExcluded := make([]string, 0, len(excludedSet))
	for name := range excludedSet {
		normalizedExcluded = append(normalizedExcluded, name)
	}
	sort.Strings(normalizedExcluded)
	return run9ReadViewSearchRequest{
		query:                query,
		limit:                limit,
		excludedDirectories:  normalizedExcluded,
		excludedDirectorySet: excludedSet,
	}, nil
}

func (h *run9ReadViewHandler) searchPaths(ctx meta.Context, fsPath string, root string, requestPath string, request run9ReadViewSearchRequest) ([]string, bool, syscall.Errno) {
	key := root + "\x00" + requestPath + "\x00" + strings.Join(request.excludedDirectories, "\x00")
	h.searchMu.Lock()
	defer h.searchMu.Unlock()
	if h.search.key == key {
		return h.search.paths, h.search.incomplete, 0
	}

	scan := run9ReadViewSearchScan{paths: make([]string, 0, 1024)}
	if errno := collectRun9ReadViewSearchPaths(h.fs, ctx, fsPath, "", 0, request.excludedDirectorySet, &scan); errno != 0 {
		return nil, false, errno
	}
	h.search = run9ReadViewSearchCache{key: key, paths: scan.paths, incomplete: scan.incomplete}
	return scan.paths, scan.incomplete, 0
}

// collectRun9ReadViewSearchPaths traverses the immutable generation with native
// bounded metadata pages. It does not resolve directory entries as paths, so
// symlinks are never followed while collecting search candidates.
func collectRun9ReadViewSearchPaths(jfs *fs.FileSystem, ctx meta.Context, fsDirectory string, relativeDirectory string, depth int, excluded map[string]struct{}, scan *run9ReadViewSearchScan) syscall.Errno {
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return syscall.EINTR
		}
		entries, next, errno := jfs.ReadDirPage(ctx, fsDirectory, run9ReadViewSearchReadDirBatch, cursor)
		if errno != 0 {
			return errno
		}
		for _, entry := range entries {
			if scan.visited >= run9ReadViewSearchMaximumEntries {
				scan.incomplete = true
				scan.stopped = true
				return 0
			}
			scan.visited++
			relativePath := path.Join(relativeDirectory, entry.Name())
			switch {
			case entry.Attr().Typ == meta.TypeFile:
				if scan.pathBytes+len(relativePath) > run9ReadViewSearchMaximumPathBytes {
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
				if depth >= run9ReadViewSearchMaximumDepth {
					scan.incomplete = true
					continue
				}
				if errno := collectRun9ReadViewSearchPaths(jfs, ctx, path.Join(fsDirectory, entry.Name()), relativePath, depth+1, excluded, scan); errno != 0 {
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

func searchRun9ReadViewPaths(paths []string, query string, limit int, incomplete bool) run9ReadViewSearchResponse {
	best := make(run9ReadViewRankedFileHeap, 0, limit)
	heap.Init(&best)
	truncated := incomplete
	for _, filePath := range paths {
		rank, matches := rankRun9ReadViewPath(query, filePath)
		if !matches {
			continue
		}
		candidate := run9ReadViewRankedFile{path: filePath, rank: rank}
		if len(best) < limit {
			heap.Push(&best, candidate)
			continue
		}
		truncated = true
		if run9ReadViewSearchRankLess(candidate.rank, best[0].rank) {
			heap.Pop(&best)
			heap.Push(&best, candidate)
		}
	}
	sort.Slice(best, func(left, right int) bool {
		return run9ReadViewSearchRankLess(best[left].rank, best[right].rank)
	})
	response := run9ReadViewSearchResponse{Matches: make([]run9ReadViewSearchMatch, len(best)), Truncated: truncated}
	for index, match := range best {
		response.Matches[index] = run9ReadViewSearchMatch{Path: match.path}
	}
	return response
}

func rankRun9ReadViewPath(query string, filePath string) (run9ReadViewSearchRank, bool) {
	foldedPath := strings.ToLower(filePath)
	foldedQuery := strings.ToLower(query)
	rank := run9ReadViewSearchRank{
		depth:    strings.Count(filePath, "/"),
		length:   len([]rune(foldedPath)),
		folded:   foldedPath,
		original: filePath,
	}
	if foldedQuery == "" {
		return rank, true
	}
	if !strings.Contains(foldedQuery, "/") {
		if kind, gaps, start, matches := rankRun9ReadViewSearchTarget([]rune(foldedQuery), []rune(strings.ToLower(path.Base(filePath)))); matches {
			rank.kind = kind
			rank.gaps = gaps
			rank.start = start
			return rank, true
		}
	}
	kind, gaps, start, matches := rankRun9ReadViewSearchTarget([]rune(foldedQuery), []rune(foldedPath))
	if !matches {
		return run9ReadViewSearchRank{}, false
	}
	rank.scope = 1
	rank.kind = kind
	rank.gaps = gaps
	rank.start = start
	return rank, true
}

func rankRun9ReadViewSearchTarget(query []rune, candidate []rune) (kind int, gaps int, start int, matches bool) {
	if len(query) == len(candidate) && string(query) == string(candidate) {
		return 0, 0, 0, true
	}
	if len(query) <= len(candidate) && string(query) == string(candidate[:len(query)]) {
		return 1, 0, 0, true
	}
	if offset := strings.Index(string(candidate), string(query)); offset >= 0 {
		return 2, 0, len([]rune(string(candidate)[:offset])), true
	}
	bestStart := -1
	bestSpan := len(candidate) + 1
	for candidateStart, character := range candidate {
		if character != query[0] {
			continue
		}
		queryIndex := 1
		for candidateIndex := candidateStart + 1; candidateIndex < len(candidate) && queryIndex < len(query); candidateIndex++ {
			if candidate[candidateIndex] == query[queryIndex] {
				queryIndex++
				if queryIndex == len(query) {
					span := candidateIndex - candidateStart + 1
					if span < bestSpan {
						bestStart = candidateStart
						bestSpan = span
					}
				}
			}
		}
	}
	if bestStart < 0 {
		return 0, 0, 0, false
	}
	return 3, bestSpan - len(query), bestStart, true
}

func run9ReadViewSearchRankLess(left, right run9ReadViewSearchRank) bool {
	leftValues := [...]int{left.scope, left.kind, left.gaps, left.start, left.depth, left.length}
	rightValues := [...]int{right.scope, right.kind, right.gaps, right.start, right.depth, right.length}
	for index := range leftValues {
		if leftValues[index] != rightValues[index] {
			return leftValues[index] < rightValues[index]
		}
	}
	if left.folded != right.folded {
		return left.folded < right.folded
	}
	return left.original < right.original
}

func writeRun9ReadViewSearchResponse(w http.ResponseWriter, result run9ReadViewSearchResponse) {
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
