package vfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Run9 recorded prewarm keeps a workload's page-cache working set warm in new
// mounts. A recording mount traces which ranges of which files its workload
// reads (ReadTrace). A later mount of the same filesystem lineage reads exactly
// those ranges through its own mountpoint before its workload asks for them
// (WarmPageCache), so the kernel page cache of that mount already holds them.
// Forks clone metadata, so an unchanged file keeps its inode number.

const tracePageSize = 4096

// PageCacheFile lists the byte ranges of one file in a page-cache working set.
type PageCacheFile struct {
	Inode Ino `json:"inode"`
	// Path is relative to the filesystem root, without a leading slash.
	Path string `json:"path"`
	// Ranges are [offset, length] pairs, page aligned, ascending and disjoint.
	Ranges [][2]uint64 `json:"ranges"`
}

// readTrace records the page ranges a mount reads, per file, and the order in
// which files were first read.
type readTrace struct {
	sync.Mutex
	start time.Time
	until time.Time // reads after it are not recorded; zero records all
	order []Ino
	files map[Ino][][2]uint64 // [start, end) intervals, ascending and disjoint
}

// TraceReads makes this mount record the ranges it reads for ReadTrace. It
// must be called before the mount serves requests.
func (v *VFS) TraceReads() {
	v.readTrace = &readTrace{start: time.Now(), files: make(map[Ino][][2]uint64)}
}

func (t *readTrace) record(ino Ino, off uint64, n int) {
	if !t.until.IsZero() && time.Now().After(t.until) {
		return
	}
	start := off &^ (tracePageSize - 1)
	end := (off + uint64(n) + tracePageSize - 1) &^ (tracePageSize - 1)
	t.Lock()
	defer t.Unlock()
	intervals, seen := t.files[ino]
	if !seen {
		t.order = append(t.order, ino)
	}
	t.files[ino] = addTraceInterval(intervals, start, end)
}

// snapshot copies the files read so far, in order of first read.
func (t *readTrace) snapshot() ([]Ino, map[Ino][][2]uint64) {
	t.Lock()
	defer t.Unlock()
	order := append([]Ino(nil), t.order...)
	traced := make(map[Ino][][2]uint64, len(t.files))
	for ino, intervals := range t.files {
		traced[ino] = append([][2]uint64(nil), intervals...)
	}
	return order, traced
}

// addTraceInterval inserts [start, end), merging it with every interval it
// overlaps or touches.
func addTraceInterval(intervals [][2]uint64, start, end uint64) [][2]uint64 {
	i := sort.Search(len(intervals), func(i int) bool { return intervals[i][1] >= start })
	j := i
	for j < len(intervals) && intervals[j][0] <= end {
		start = min(start, intervals[j][0])
		end = max(end, intervals[j][1])
		j++
	}
	if i == j {
		intervals = append(intervals, [2]uint64{})
		copy(intervals[i+1:], intervals[i:])
	} else {
		intervals = append(intervals[:i+1], intervals[j:]...)
	}
	intervals[i] = [2]uint64{start, end}
	return intervals
}

// ReadTraceLimits bounds a ReadTrace result.
type ReadTraceLimits struct {
	MaxFiles int
	// MaxBytes bounds the total length of the listed ranges.
	MaxBytes uint64
	// MaxListBytes bounds the JSON encoding of the list.
	MaxListBytes int
}

// ReadTrace returns the regular files this mount has read, in order of first
// read, with the ranges read from each. A file whose ranges would exceed
// MaxBytes is left out; the list ends before it would exceed MaxFiles or
// MaxListBytes. Files this mount created or changed are left out too: their
// ctime is not before tracing began, and their content is not part of the
// filesystem the mount started from.
func (v *VFS) ReadTrace(ctx meta.Context, limits ReadTraceLimits) ([]PageCacheFile, error) {
	t := v.readTrace
	if t == nil || !t.until.IsZero() {
		return nil, errors.New("this mount does not trace reads")
	}
	order, traced := t.snapshot()
	paths := tracePaths{ctx: ctx, m: v.Meta, dirs: make(map[Ino]*traceDir)}
	var files []PageCacheFile
	var bytes uint64
	listBytes := len("[")
	for _, ino := range order {
		if len(files) == limits.MaxFiles {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ranges := make([][2]uint64, 0, len(traced[ino]))
		var size uint64
		for _, interval := range traced[ino] {
			ranges = append(ranges, [2]uint64{interval[0], interval[1] - interval[0]})
			size += interval[1] - interval[0]
		}
		if bytes+size > limits.MaxBytes {
			continue
		}
		var attr Attr
		if st := v.Meta.GetAttr(ctx, ino, &attr); st == syscall.ENOENT {
			continue
		} else if st != 0 {
			return nil, fmt.Errorf("getattr inode %d: %w", ino, st)
		}
		if attr.Typ != meta.TypeFile || !time.Unix(attr.Ctime, int64(attr.Ctimensec)).Before(t.start) {
			continue
		}
		path, err := paths.file(ino, attr.Parent)
		if err != nil {
			return nil, err
		}
		if path == "" {
			continue
		}
		file := PageCacheFile{Inode: ino, Path: path, Ranges: ranges}
		encoded, err := json.Marshal(file)
		if err != nil {
			return nil, err
		}
		// Each entry is followed by "," or the closing "]".
		if listBytes+len(encoded)+1 > limits.MaxListBytes {
			break
		}
		listBytes += len(encoded) + 1
		bytes += size
		files = append(files, file)
	}
	return files, nil
}

// tracePaths resolves traced inodes to paths for one ReadTrace call. It lists
// each directory at most once, so resolving many files of few directories
// costs about one metadata lookup per file.
type tracePaths struct {
	ctx  meta.Context
	m    meta.Meta
	dirs map[Ino]*traceDir // nil for a directory no longer reachable from the root
}

type traceDir struct {
	path  string         // relative to the root, with a trailing slash; "" for the root
	names map[Ino]string // child inode to its least name
}

// file returns the least path of a file whose attr.Parent is parent, or "" if
// the root no longer reaches it. A hard-linked file has no single parent.
func (p *tracePaths) file(ino, parent Ino) (string, error) {
	parents := map[Ino]int{parent: 1}
	if parent == 0 {
		parents = p.m.GetParents(p.ctx, ino)
	}
	var least string
	for parent := range parents {
		d, err := p.dir(parent)
		if err != nil {
			return "", err
		}
		if d == nil {
			continue
		}
		if name, ok := d.names[ino]; ok && (least == "" || d.path+name < least) {
			least = d.path + name
		}
	}
	return least, nil
}

func (p *tracePaths) dir(ino Ino) (*traceDir, error) {
	d, ok := p.dirs[ino]
	if !ok {
		var err error
		if d, err = p.listDir(ino); err != nil {
			return nil, err
		}
		p.dirs[ino] = d
	}
	return d, nil
}

// listDir lists a directory, or returns nil if the root no longer reaches it.
func (p *tracePaths) listDir(ino Ino) (*traceDir, error) {
	var path string
	if ino != meta.RootInode {
		var attr Attr
		if st := p.m.GetAttr(p.ctx, ino, &attr); st == syscall.ENOENT {
			return nil, nil
		} else if st != 0 {
			return nil, fmt.Errorf("getattr directory %d: %w", ino, st)
		}
		parent, err := p.dir(attr.Parent)
		if err != nil || parent == nil {
			return nil, err
		}
		name, ok := parent.names[ino]
		if !ok {
			return nil, nil
		}
		path = parent.path + name + "/"
	}
	var entries []*meta.Entry
	if st := p.m.Readdir(p.ctx, ino, 0, &entries); st == syscall.ENOENT {
		return nil, nil
	} else if st != 0 {
		return nil, fmt.Errorf("readdir %d: %w", ino, st)
	}
	d := &traceDir{path: path, names: make(map[Ino]string, len(entries))}
	for _, e := range entries {
		name := string(e.Name)
		if name == "." || name == ".." {
			continue
		}
		if least, ok := d.names[e.Inode]; !ok || name < least {
			d.names[e.Inode] = name
		}
	}
	return d, nil
}

// PageCacheWarmResult summarizes one WarmPageCache call.
type PageCacheWarmResult struct {
	// Files counts files whose listed ranges were all read.
	Files int `json:"files"`
	// Bytes counts bytes read into the page cache.
	Bytes uint64 `json:"bytes"`
	// Skipped counts files that were gone, replaced, or not locally cached.
	Skipped int `json:"skipped"`
	// Stopped explains an early stop: "budget", or the context's error.
	Stopped string `json:"stopped,omitempty"`
}
