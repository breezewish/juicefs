package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Run9 start reads let a mount start on a host that does not hold its data
// about as fast as on one that does. A mount traces the file ranges it reads
// during its start (TraceStartReads, StartReads); the next mount of the same
// filesystem fetches the blocks behind those ranges from object storage, many
// at once and in the order they were first read (PrefetchStartReads), so the
// workload finds them in the local cache instead of fetching one at a time as
// its reads arrive. The list names inodes, not paths: it is only ever used on
// the filesystem whose reads it traced, whose inodes persist across mounts.

// StartRead lists the ranges of one file a mount read during its start.
type StartRead struct {
	Inode Ino `json:"inode"`
	// Ranges are [offset, length] pairs, page aligned, ascending and disjoint.
	Ranges [][2]uint64 `json:"ranges"`
}

// StartReadLimits bounds a StartReads result.
type StartReadLimits struct {
	MaxFiles int
	// MaxBytes bounds the total length of the listed ranges; a file whose
	// ranges would exceed it is left out.
	MaxBytes uint64
	// MaxListBytes bounds the JSON encoding of the list.
	MaxListBytes int
}

// TraceStartReads makes this mount record the ranges it reads during its
// first window, for StartReads. It must be called before the mount serves
// requests. Files written during the window stay listed: unlike a recording,
// a start list describes this filesystem, not a base it was forked from.
func (v *VFS) TraceStartReads(window time.Duration) {
	start := time.Now()
	v.readTrace = &readTrace{start: start, until: start.Add(window), files: make(map[Ino][][2]uint64)}
}

// StartReads returns the regular files this mount read during its start
// window, in order of first read, with the ranges read from each. The list
// ends before it would exceed MaxFiles or MaxListBytes.
func (v *VFS) StartReads(ctx meta.Context, limits StartReadLimits) ([]StartRead, error) {
	t := v.readTrace
	if t == nil || t.until.IsZero() {
		return nil, errors.New("this mount does not trace its start reads")
	}
	order, traced := t.snapshot()
	var files []StartRead
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
		if attr.Typ != meta.TypeFile {
			continue
		}
		file := StartRead{Inode: ino, Ranges: ranges}
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

// StartReadsPrefetchResult summarizes one PrefetchStartReads call.
type StartReadsPrefetchResult struct {
	// Files counts listed files that are still regular files of this mount.
	Files int
	// Skipped counts listed files that are gone or no longer regular files.
	Skipped int
	// Blocks counts the blocks fetched from object storage; blocks a local
	// cache already held are neither fetched nor counted.
	Blocks int64
	// Failed counts blocks whose fetch failed; the workload reads them as usual.
	Failed int64
	// Stopped explains an early stop: the context's cause.
	Stopped string
}

// prefetchBlock is block indx of a slice of sliceSize bytes.
type prefetchBlock struct {
	slice     uint64
	sliceSize uint32
	indx      int
}

// PrefetchStartReads loads the blocks behind the listed ranges into the local
// caches with up to concurrency fetches at once, in list order. Every fetch
// reads the current metadata and immutable blocks, so a stale list costs only
// wasted fetches: ranges past a file's end are clipped and changed files load
// their current blocks.
func (v *VFS) PrefetchStartReads(ctx context.Context, files []StartRead, concurrency int) StartReadsPrefetchResult {
	var result StartReadsPrefetchResult
	var fetched, failed atomic.Int64
	work := make(chan prefetchBlock)
	var workers sync.WaitGroup
	for range concurrency {
		workers.Go(func() {
			for b := range work {
				ok, err := v.Store.PrefetchBlock(ctx, b.slice, b.sliceSize, b.indx)
				if ok {
					fetched.Add(1)
				}
				if err != nil && ctx.Err() == nil {
					failed.Add(1)
					logger.Warnf("prefetch start read block %d of slice %d: %s", b.indx, b.slice, err)
				}
			}
		})
	}
	mctx := meta.WrapContext(ctx)
	blockSize := uint64(v.Conf.Chunk.BlockSize)
	seen := make(map[prefetchBlock]struct{})
produce:
	for _, file := range files {
		var attr Attr
		if st := v.Meta.GetAttr(mctx, file.Inode, &attr); st != 0 || attr.Typ != meta.TypeFile {
			result.Skipped++
			continue
		}
		result.Files++
		// Ranges ascend, so consecutive ranges of one chunk reuse its slices.
		var slices []meta.Slice
		sliceChunk := uint32(math.MaxUint32)
		for _, r := range file.Ranges {
			end := min(r[0]+r[1], attr.Length)
			for off := r[0]; off < end; {
				indx := uint32(off / meta.ChunkSize)
				chunkStart := uint64(indx) * meta.ChunkSize
				chunkEnd := min(end, chunkStart+meta.ChunkSize)
				if indx != sliceChunk {
					if st := v.Meta.Read(mctx, file.Inode, indx, &slices); st != 0 {
						if ctx.Err() != nil {
							break produce
						}
						logger.Warnf("prefetch start reads of inode %d chunk %d: %s", file.Inode, indx, st)
						break
					}
					sliceChunk = indx
				}
				// Slices tile the chunk in order, each from where the last ended.
				sliceStart := chunkStart
				for _, s := range slices {
					lo, hi := max(off, sliceStart), min(chunkEnd, sliceStart+uint64(s.Len))
					sliceLo := uint64(s.Off) + lo - sliceStart
					sliceStart += uint64(s.Len)
					if s.Id == 0 || lo >= hi {
						continue
					}
					sliceHi := sliceLo + hi - lo
					for block := sliceLo / blockSize; block*blockSize < sliceHi; block++ {
						b := prefetchBlock{slice: s.Id, sliceSize: s.Size, indx: int(block)}
						if _, ok := seen[b]; ok {
							continue
						}
						seen[b] = struct{}{}
						select {
						case work <- b:
						case <-ctx.Done():
							break produce
						}
					}
				}
				off = chunkEnd
			}
		}
	}
	close(work)
	workers.Wait()
	result.Blocks, result.Failed = fetched.Load(), failed.Load()
	if err := ctx.Err(); err != nil {
		result.Stopped = context.Cause(ctx).Error()
	}
	return result
}
