package fs

import (
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
)

const maxReadDirPageSize = 1000

type pagedMetadata interface {
	ReaddirPage(ctx meta.Context, inode meta.Ino, after string, limit int, entries *[]*meta.Entry) (bool, syscall.Errno)
}

// ReadDirPage lists a bounded, name-ordered page from a directory. Cursor is
// the final name from the preceding page; an empty next cursor means the page
// is terminal. This run9 extension is available for transactional key-value
// metadata backends, including Badger.
func (fs *FileSystem) ReadDirPage(ctx meta.Context, path string, limit int, cursor string) (entries []*FileStat, nextCursor string, err syscall.Errno) {
	if limit <= 0 || limit > maxReadDirPageSize {
		return nil, "", syscall.EINVAL
	}
	fi, err := fs.resolve(ctx, path, true)
	if err != 0 {
		return nil, "", err
	}
	if !fi.IsDir() {
		return nil, "", syscall.ENOTDIR
	}

	paged, ok := fs.m.(pagedMetadata)
	if !ok {
		return nil, "", syscall.ENOTSUP
	}
	var page []*meta.Entry
	more, err := paged.ReaddirPage(ctx, fi.inode, cursor, limit, &page)
	if err != 0 {
		return nil, "", err
	}
	entries = make([]*FileStat, 0, len(page))
	for _, entry := range page {
		stat := AttrToFileInfo(entry.Inode, entry.Attr)
		stat.name = string(entry.Name)
		entries = append(entries, stat)
	}
	if more {
		nextCursor = entries[len(entries)-1].Name()
	}
	return entries, nextCursor, 0
}
