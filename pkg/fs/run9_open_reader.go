package fs

import (
	"io"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// OpenRead opens a seekable read-only stream bound to the caller's context.
func (fs *FileSystem) OpenRead(ctx meta.Context, name string) (io.ReadSeekCloser, syscall.Errno) {
	file, errno := fs.Open(ctx, name, vfs.MODE_MASK_R)
	if errno != 0 {
		return nil, errno
	}
	return &contextReader{file, ctx}, 0
}

type contextReader struct {
	file *File
	ctx  meta.Context
}

func (r *contextReader) Read(p []byte) (int, error)         { return r.file.Read(r.ctx, p) }
func (r *contextReader) Seek(n int64, w int) (int64, error) { return r.file.Seek(r.ctx, n, w) }
func (r *contextReader) Close() error {
	if errno := r.file.Close(r.ctx); errno != 0 {
		return errno
	}
	return nil
}
