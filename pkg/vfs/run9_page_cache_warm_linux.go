package vfs

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// warmReadSize bounds one read, and so each worker's buffer.
const warmReadSize = 1 << 20

// WarmPageCache reads the listed ranges through this mount's own mountpoint so
// its kernel page cache holds them, using up to concurrency worker threads and
// at most maxBytes. Files are claimed in list order.
//
// Workers open files from threads the FUSE handlers recognize, which gives them
// cache-only handles (DataReader.OpenCacheOnly): a range whose blocks are not
// already in a local cache is skipped rather than fetched from object storage,
// and nothing is read ahead. Workers also disable kernel readahead, so only the
// listed pages enter the page cache, as ordinary evictable pages.
//
// A path is resolved without following symlinks or leaving the mount, and the
// file is read only if it is still the listed regular file.
func (v *VFS) WarmPageCache(ctx context.Context, files []PageCacheFile, maxBytes uint64, concurrency int) (PageCacheWarmResult, error) {
	// Opening the root with openat2 also proves the kernel has it (Linux 5.6+).
	root, err := unix.Openat2(unix.AT_FDCWD, v.Conf.Meta.MountPoint, &unix.OpenHow{Flags: unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC})
	if err == unix.ENOSYS {
		return PageCacheWarmResult{}, errors.New("page cache warming unsupported: the kernel lacks openat2")
	} else if err != nil {
		return PageCacheWarmResult{}, fmt.Errorf("open mountpoint: %w", err)
	}
	defer unix.Close(root)

	w := &pageCacheWarm{v: v, root: root, files: files, maxBytes: maxBytes}
	results := make([]PageCacheWarmResult, concurrency)
	var workers sync.WaitGroup
	for i := range results {
		workers.Go(func() { results[i] = w.work(ctx) })
	}
	workers.Wait()

	var total PageCacheWarmResult
	for _, r := range results {
		total.Files += r.Files
		total.Bytes += r.Bytes
		total.Skipped += r.Skipped
	}
	if total.Files+total.Skipped < len(files) {
		if w.overBudget.Load() {
			total.Stopped = "budget"
		} else {
			total.Stopped = context.Cause(ctx).Error()
		}
	}
	return total, nil
}

type pageCacheWarm struct {
	v          *VFS
	root       int
	files      []PageCacheFile
	maxBytes   uint64
	next       atomic.Int64
	reserved   atomic.Uint64
	overBudget atomic.Bool
}

func (w *pageCacheWarm) work(ctx context.Context) (result PageCacheWarmResult) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	tid := uint32(unix.Gettid())
	w.v.cacheOnlyThreads.Store(tid, struct{}{})
	defer w.v.cacheOnlyThreads.Delete(tid)

	buf := make([]byte, warmReadSize)
	for ctx.Err() == nil && !w.overBudget.Load() {
		i := w.next.Add(1) - 1
		if i >= int64(len(w.files)) {
			return
		}
		read, complete := w.file(ctx, w.files[i], buf)
		result.Bytes += read
		if complete {
			result.Files++
		} else if ctx.Err() == nil && !w.overBudget.Load() {
			result.Skipped++
		}
	}
	return
}

// file reads one file's ranges and reports the bytes read and whether every
// range was read.
func (w *pageCacheWarm) file(ctx context.Context, f PageCacheFile, buf []byte) (read uint64, complete bool) {
	// O_NONBLOCK only keeps a FIFO now at this path from blocking the open.
	how := &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	}
	fd, err := unix.Openat2(w.root, f.Path, how)
	for err == unix.EINTR {
		fd, err = unix.Openat2(w.root, f.Path, how)
	}
	if err != nil {
		return 0, false
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Ino != uint64(f.Inode) {
		return 0, false
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, 0); err != nil {
		return 0, false
	}
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM); err != nil {
		return 0, false
	}
	for _, r := range f.Ranges {
		for off, end := r[0], r[0]+r[1]; off < end; {
			size := min(end-off, warmReadSize)
			if ctx.Err() != nil || !w.reserve(size) {
				return read, false
			}
			n, err := unix.Pread(fd, buf[:size], int64(off))
			for err == unix.EINTR {
				n, err = unix.Pread(fd, buf[:size], int64(off))
			}
			if err != nil {
				// EIO: a block of this range is not in a local cache.
				return read, false
			}
			if n == 0 {
				break // the file is shorter than when it was traced
			}
			read += uint64(n)
			off += uint64(n)
		}
	}
	return read, true
}

func (w *pageCacheWarm) reserve(n uint64) bool {
	for {
		used := w.reserved.Load()
		if used+n > w.maxBytes {
			w.overBudget.Store(true)
			return false
		}
		if w.reserved.CompareAndSwap(used, used+n) {
			return true
		}
	}
}
