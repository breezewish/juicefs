//go:build linux

package cmd

import (
	"container/heap"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

type run9LocalReader struct{ root *os.Root }

func openRun9LocalReader(path string) (run9ReadFilesystem, func(), error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, nil, err
	}
	return &run9LocalReader{root}, func() { _ = root.Close() }, nil
}

func localReaderPath(path string) (string, syscall.Errno) {
	if path == run9ReadViewRoot {
		return ".", 0
	}
	relative, ok := strings.CutPrefix(path, run9ReadViewRoot+"/")
	if !ok {
		return "", syscall.ENOENT
	}
	return relative, 0
}

func localReadErr(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return syscall.EIO
}

func (v *run9LocalReader) Lstat(_ meta.Context, path string) (*fs.FileStat, syscall.Errno) {
	name, e := localReaderPath(path)
	if e != 0 {
		return nil, e
	}
	info, err := v.root.Lstat(name)
	if err != nil {
		return nil, localReadErr(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	var typ uint8
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFREG:
		typ = meta.TypeFile
	case syscall.S_IFDIR:
		typ = meta.TypeDirectory
	case syscall.S_IFLNK:
		typ = meta.TypeSymlink
	case syscall.S_IFBLK:
		typ = meta.TypeBlockDev
	case syscall.S_IFCHR:
		typ = meta.TypeCharDev
	case syscall.S_IFIFO:
		typ = meta.TypeFIFO
	case syscall.S_IFSOCK:
		typ = meta.TypeSocket
	default:
		return nil, syscall.EIO
	}
	attr := meta.Attr{Typ: typ, Mode: uint16(stat.Mode & 07777), Uid: stat.Uid, Gid: stat.Gid, Length: uint64(stat.Size), Nlink: uint32(stat.Nlink), Mtime: stat.Mtim.Sec, Mtimensec: uint32(stat.Mtim.Nsec), Ctime: stat.Ctim.Sec, Ctimensec: uint32(stat.Ctim.Nsec), Atime: stat.Atim.Sec, Atimensec: uint32(stat.Atim.Nsec), Full: true}
	return fs.AttrToFileInfo(meta.Ino(stat.Ino), &attr).WithName(info.Name()), 0
}

func (v *run9LocalReader) Readlink(_ meta.Context, path string) ([]byte, syscall.Errno) {
	name, e := localReaderPath(path)
	if e != 0 {
		return nil, e
	}
	target, err := v.root.Readlink(name)
	return []byte(target), localReadErr(err)
}

func (v *run9LocalReader) OpenRead(_ meta.Context, path string) (io.ReadSeekCloser, syscall.Errno) {
	name, e := localReaderPath(path)
	if e != 0 {
		return nil, e
	}
	f, err := v.root.Open(name)
	if err != nil {
		return nil, localReadErr(err)
	}
	return f, 0
}

// Directory order in ext4 is not lexical. Keep only one response page while
// scanning, so a large immutable directory cannot exhaust the reader's memory.
type readNames []string

func (h readNames) Len() int           { return len(h) }
func (h readNames) Less(i, j int) bool { return h[i] > h[j] }
func (h readNames) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *readNames) Push(v any)        { *h = append(*h, v.(string)) }
func (h *readNames) Pop() any          { v := (*h)[len(*h)-1]; *h = (*h)[:len(*h)-1]; return v }

func (v *run9LocalReader) ReadDirPage(ctx meta.Context, path string, limit int, cursor string) ([]*fs.FileStat, string, syscall.Errno) {
	if limit < 1 || limit > 10000 {
		return nil, "", syscall.EINVAL
	}
	name, e := localReaderPath(path)
	if e != 0 {
		return nil, "", e
	}
	f, err := v.root.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, "", localReadErr(err)
	}
	defer f.Close()
	var names readNames
	more := false
	for {
		if ctx.Canceled() {
			return nil, "", syscall.EINTR
		}
		batch, err := f.Readdirnames(256)
		for _, n := range batch {
			if n <= cursor {
				continue
			}
			if len(names) < limit {
				heap.Push(&names, n)
				continue
			}
			more = true
			if n < names[0] {
				names[0] = n
				heap.Fix(&names, 0)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, "", localReadErr(err)
		}
	}
	sort.Strings(names)
	entries := make([]*fs.FileStat, 0, len(names))
	for _, n := range names {
		stat, e := v.Lstat(ctx, path+"/"+n)
		if e != 0 {
			return nil, "", e
		}
		entries = append(entries, stat)
	}
	next := ""
	if more {
		next = names[len(names)-1]
	}
	return entries, next, 0
}
