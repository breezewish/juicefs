package cmd

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"golang.org/x/sync/semaphore"
)

// A read-view archive streams one path as tar. One walker visits the tree in
// output order and queues each entry's header followed by its content as
// fixed-size ranges. Range reads run in parallel ahead of one writer, which
// emits the queue in order. A queued item holds bytes of the read-ahead budget
// from the moment it is admitted until the writer has written it: a range its
// length, a header its names plus run9ReadViewArchiveHeaderBytes. Queue memory
// is therefore bounded by the budget, never by the tree, its names or any
// file. The guest controls the tree, so the walk state is bounded too: one
// directory page per level up to run9ReadViewArchiveMaxDepth, and the
// hard-link table. Portal bounds how many archives run at once.
const (
	// run9ReadViewArchiveRangeBytes is one JuiceFS block: a cold range is one
	// object read, and a large file is read as many ranges in parallel.
	run9ReadViewArchiveRangeBytes = 4 << 20
	// Each archive buffers at most this many read-ahead bytes and reads.
	run9ReadViewArchiveRequestBytes = 64 << 20
	run9ReadViewArchiveRequestReads = 64
	// run9ReadViewArchiveHeaderBytes is charged per queued header on top of
	// its names, covering the header and its encoding.
	run9ReadViewArchiveHeaderBytes = 1 << 10
	// run9ReadViewArchiveQueueItems bounds queued headers and ranges.
	run9ReadViewArchiveQueueItems = 1024
	// run9ReadViewArchivePageEntries is the directory page each level of the
	// walk holds while it descends.
	run9ReadViewArchivePageEntries = 64
	// run9ReadViewArchiveMaxDepth bounds directory levels below the archived
	// path; a deeper tree fails the archive rather than being cut silently.
	run9ReadViewArchiveMaxDepth = 128
	// run9ReadViewArchiveLinkBytes bounds the first names remembered to emit
	// later names of the same inode as hard links, charging each remembered
	// inode run9ReadViewArchiveLinkEntryBytes beyond its name. Once it is
	// full, a later name of an inode not yet remembered is a full copy.
	run9ReadViewArchiveLinkBytes      = 8 << 20
	run9ReadViewArchiveLinkEntryBytes = 128
)

// All archives of this reader process share one budget, so concurrent archives
// cannot multiply the per-archive bound: 128 MiB of read-ahead and 128 reads.
var run9ReadViewArchiveProcessBudget = newRun9ReadViewArchiveBudget(128<<20, 128)

// Full-size ranges dominate archive memory; reuse them across archives.
var run9ReadViewArchiveRanges = sync.Pool{New: func() any {
	buffer := make([]byte, run9ReadViewArchiveRangeBytes)
	return &buffer
}}

type run9ReadViewArchiveBudget struct {
	bytes, reads *semaphore.Weighted
}

func newRun9ReadViewArchiveBudget(bytes int64, reads int64) run9ReadViewArchiveBudget {
	return run9ReadViewArchiveBudget{bytes: semaphore.NewWeighted(bytes), reads: semaphore.NewWeighted(reads)}
}

// run9ReadViewArchiveItem is one queued header or file range, in output order.
type run9ReadViewArchiveItem struct {
	header *tar.Header
	size   int64         // Bytes held in both byte budgets until written.
	ready  chan struct{} // Closed once a range's data or err is set; nil for a header.
	data   []byte
	err    error
}

type run9ReadViewArchive struct {
	ctx     context.Context
	metaCtx meta.Context
	items   chan *run9ReadViewArchiveItem
	request run9ReadViewArchiveBudget
	process run9ReadViewArchiveBudget
	reads   sync.WaitGroup
	walkErr error // Written by the walker before it closes items.
}

type run9ReadViewArchiveInode struct {
	mount int // run9ReadViewHandler.dataMountIndex
	inode fs.Ino
}

type run9ReadViewArchiveLink struct {
	name      string
	remaining uint32 // Names of the inode not yet visited.
}

type run9ReadViewArchiveWalker struct {
	handler   *run9ReadViewHandler
	archive   *run9ReadViewArchive
	links     map[run9ReadViewArchiveInode]*run9ReadViewArchiveLink
	linkBytes int
}

// run9ReadViewArchiveRequested accepts only archive=tar, the one format served.
func run9ReadViewArchiveRequested(query url.Values) (bool, error) {
	values, requested := query["archive"]
	if !requested {
		return false, nil
	}
	if len(values) != 1 || values[0] != "tar" {
		return false, fmt.Errorf("archive must be tar when provided")
	}
	return true, nil
}

// serveArchive streams requestPath, with its symlinks followed, as tar.
// Errors before the first byte keep their HTTP status. After it, the response
// is aborted, never ended cleanly, so a client cannot mistake a short archive
// for a complete one; the end-of-archive blocks are written only on success.
func (h *run9ReadViewHandler) serveArchive(w http.ResponseWriter, r *http.Request, filesystemRoot string, requestPath string) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	archive := &run9ReadViewArchive{
		ctx:     ctx,
		metaCtx: run9ReadViewContext(ctx),
		items:   make(chan *run9ReadViewArchiveItem, run9ReadViewArchiveQueueItems),
		request: newRun9ReadViewArchiveBudget(run9ReadViewArchiveRequestBytes, run9ReadViewArchiveRequestReads),
		process: run9ReadViewArchiveProcessBudget,
	}
	fsPath, stat, errno := resolveRun9ReadViewPath(h.fs, archive.metaCtx, filesystemRoot, requestPath)
	if errno != 0 {
		writeRun9ReadViewError(w, errno)
		return
	}
	walker := &run9ReadViewArchiveWalker{handler: h, archive: archive, links: make(map[run9ReadViewArchiveInode]*run9ReadViewArchiveLink)}
	name := path.Base(requestPath)
	if requestPath == "/" {
		name = "."
	}
	walked := make(chan struct{})
	go func() {
		defer close(walked)
		archive.walkErr = walker.visit(fsPath, name, stat)
		close(archive.items)
	}()

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Cache-Control", "no-store")
	written, err := archive.writeTo(w)
	cancel()
	archive.discardQueued()
	<-walked
	archive.reads.Wait()
	if err == nil {
		return
	}
	if !written {
		writeRun9ReadViewError(w, err)
		return
	}
	logger.Warnf("run9 read view archive aborted: %s", err)
	panic(http.ErrAbortHandler)
}

// writeTo emits queued items in order. It flushes only when the next item is
// not ready, so output streams without a flush per write. Once the archive's
// context ends it stops between items, and a write already blocked on the
// client fails at once.
func (archive *run9ReadViewArchive) writeTo(w http.ResponseWriter) (bool, error) {
	output := &run9ReadViewArchiveOutput{response: w, controller: http.NewResponseController(w)}
	stopUnblocking := context.AfterFunc(archive.ctx, func() {
		_ = output.controller.SetWriteDeadline(time.Now())
	})
	defer stopUnblocking()
	tarWriter := tar.NewWriter(output)
	fail := func(err error) (bool, error) {
		// The caller aborts the response, which discards what was not flushed,
		// so flush now. Every tar entry ends on a 512-byte block, so a stream
		// that does not stop at a multiple of 512 bytes cannot read as a
		// complete archive, even if an intermediary ends the aborted response
		// cleanly; one more byte makes sure of that. After a cancel the write
		// deadline has passed and neither the byte nor the flush goes out;
		// the gateway, seeing this response abort, adds the byte instead.
		if output.written != 0 {
			if output.written%512 == 0 {
				_, _ = output.Write([]byte{0xff})
			}
			_ = output.flush()
		}
		return output.written != 0, err
	}
	for {
		var item *run9ReadViewArchiveItem
		var ok bool
		select {
		case item, ok = <-archive.items:
		default:
			output.flush()
			select {
			case item, ok = <-archive.items:
			case <-archive.ctx.Done():
				return fail(context.Cause(archive.ctx))
			}
		}
		if !ok {
			break
		}
		if item.ready != nil {
			select {
			case <-item.ready:
			default:
				output.flush()
				<-item.ready
			}
		}
		var err error
		switch {
		case archive.ctx.Err() != nil:
			err = context.Cause(archive.ctx)
		case item.err != nil:
			err = item.err
		case item.header != nil:
			err = tarWriter.WriteHeader(item.header)
		default:
			_, err = tarWriter.Write(item.data)
		}
		archive.release(item)
		if err != nil {
			return fail(err)
		}
	}
	if archive.walkErr != nil {
		return fail(archive.walkErr)
	}
	if err := tarWriter.Close(); err != nil {
		return fail(err)
	}
	return output.written != 0, output.flush()
}

// discardQueued releases what the writer will never write. It returns once
// the walker has stopped and every queued read has finished.
func (archive *run9ReadViewArchive) discardQueued() {
	for item := range archive.items {
		if item.ready != nil {
			<-item.ready
		}
		archive.release(item)
	}
}

func (archive *run9ReadViewArchive) push(item *run9ReadViewArchiveItem) error {
	select {
	case archive.items <- item:
		return nil
	case <-archive.ctx.Done():
		if item.ready != nil {
			<-item.ready
		}
		archive.release(item)
		return context.Cause(archive.ctx)
	}
}

func (archive *run9ReadViewArchive) release(item *run9ReadViewArchiveItem) {
	archive.request.bytes.Release(item.size)
	archive.process.bytes.Release(item.size)
	if data := item.data; len(data) == run9ReadViewArchiveRangeBytes {
		run9ReadViewArchiveRanges.Put(&data)
	}
	item.data = nil
}

// addHeader queues header, holding its size in the byte budgets.
func (archive *run9ReadViewArchive) addHeader(header *tar.Header) error {
	size := int64(len(header.Name)+len(header.Linkname)) + run9ReadViewArchiveHeaderBytes
	if err := acquireRun9ReadViewArchive(archive.ctx, size, archive.request.bytes, archive.process.bytes); err != nil {
		return err
	}
	return archive.push(&run9ReadViewArchiveItem{header: header, size: size})
}

// addFile queues header and then the content of the regular file at fsPath.
// Each range opens its own handle: a handle serializes its reads.
func (archive *run9ReadViewArchive) addFile(header *tar.Header, jfs run9ReadFilesystem, fsPath string) error {
	if err := archive.addHeader(header); err != nil {
		return err
	}
	for offset := int64(0); offset < header.Size; offset += run9ReadViewArchiveRangeBytes {
		size := min(run9ReadViewArchiveRangeBytes, header.Size-offset)
		if err := acquireRun9ReadViewArchive(archive.ctx, size, archive.request.bytes, archive.process.bytes); err != nil {
			return err
		}
		if err := acquireRun9ReadViewArchive(archive.ctx, 1, archive.request.reads, archive.process.reads); err != nil {
			archive.request.bytes.Release(size)
			archive.process.bytes.Release(size)
			return err
		}
		item := &run9ReadViewArchiveItem{size: size, ready: make(chan struct{})}
		archive.reads.Add(1)
		go func(offset int64) {
			defer archive.reads.Done()
			item.data, item.err = readRun9ReadViewArchiveRange(jfs, archive.metaCtx, fsPath, offset, size)
			archive.request.reads.Release(1)
			archive.process.reads.Release(1)
			close(item.ready)
		}(offset)
		if err := archive.push(item); err != nil {
			return err
		}
	}
	return nil
}

// acquireRun9ReadViewArchive takes n from every semaphore, or from none.
func acquireRun9ReadViewArchive(ctx context.Context, n int64, semaphores ...*semaphore.Weighted) error {
	for index, weighted := range semaphores {
		if err := weighted.Acquire(ctx, n); err != nil {
			for _, held := range semaphores[:index] {
				held.Release(n)
			}
			return context.Cause(ctx)
		}
	}
	return nil
}

func readRun9ReadViewArchiveRange(jfs run9ReadFilesystem, ctx meta.Context, fsPath string, offset int64, size int64) ([]byte, error) {
	file, errno := jfs.Open(ctx, fsPath, vfs.MODE_MASK_R)
	if errno != 0 {
		return nil, errno
	}
	defer file.Close(ctx)
	var data []byte
	if size == run9ReadViewArchiveRangeBytes {
		data = *run9ReadViewArchiveRanges.Get().(*[]byte)
	} else {
		data = make([]byte, size)
	}
	for read := 0; read < len(data); {
		n, err := file.Pread(ctx, data[read:], offset+int64(read))
		read += n
		if (err == io.EOF || n == 0 && err == nil) && read < len(data) {
			err = fmt.Errorf("file %q is shorter than its metadata", fsPath)
		}
		if err != nil && read < len(data) {
			if len(data) == run9ReadViewArchiveRangeBytes {
				run9ReadViewArchiveRanges.Put(&data)
			}
			return nil, err
		}
	}
	return data, nil
}

type run9ReadViewArchiveOutput struct {
	response   http.ResponseWriter
	controller *http.ResponseController
	written    int64 // Bytes handed to the response.
}

func (output *run9ReadViewArchiveOutput) Write(data []byte) (int, error) {
	n, err := output.response.Write(data)
	output.written += int64(n)
	return n, err
}

func (output *run9ReadViewArchiveOutput) flush() error {
	if output.written == 0 {
		return nil
	}
	return output.controller.Flush()
}

// visit archives one resolved entry and, for a directory, its descendants.
func (walker *run9ReadViewArchiveWalker) visit(fsPath string, name string, stat *fs.FileStat) error {
	header, err := walker.header(fsPath, name, stat)
	if err != nil || header == nil {
		return err
	}
	switch {
	case stat.IsDir():
		if err := walker.archive.addHeader(header); err != nil {
			return err
		}
		return walker.walkDirectory(fsPath, header.Name)
	case stat.Attr().Typ == meta.TypeFile:
		return walker.addFile(fsPath, header, stat)
	default:
		return walker.archive.addHeader(header)
	}
}

// walkDirectory archives a directory's descendants. Each level holds one
// bounded page while it descends.
func (walker *run9ReadViewArchiveWalker) walkDirectory(fsDirectory string, prefix string) error {
	if strings.Count(prefix, "/") > run9ReadViewArchiveMaxDepth {
		return fmt.Errorf("directory %q is more than %d levels deep", prefix, run9ReadViewArchiveMaxDepth)
	}
	cursor := ""
	for {
		entries, next, errno := walker.handler.fs.ReadDirPage(walker.archive.metaCtx, fsDirectory, run9ReadViewArchivePageEntries, cursor)
		if errno != 0 {
			return errno
		}
		for _, entry := range entries {
			if err := walker.visit(path.Join(fsDirectory, entry.Name()), prefix+entry.Name(), entry); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}

// addFile archives a regular file, or a hard link to the first name of its
// inode already in the archive.
func (walker *run9ReadViewArchiveWalker) addFile(fsPath string, header *tar.Header, stat *fs.FileStat) error {
	if nlink := stat.Attr().Nlink; nlink > 1 {
		inode := run9ReadViewArchiveInode{mount: walker.handler.dataMountIndex(fsPath), inode: stat.Inode()}
		if link := walker.links[inode]; link != nil {
			header.Typeflag, header.Linkname, header.Size = tar.TypeLink, link.name, 0
			// Forget an inode once all of its names are archived.
			if link.remaining--; link.remaining == 0 {
				delete(walker.links, inode)
				walker.linkBytes -= len(link.name) + run9ReadViewArchiveLinkEntryBytes
			}
			return walker.archive.addHeader(header)
		}
		if cost := len(header.Name) + run9ReadViewArchiveLinkEntryBytes; walker.linkBytes+cost <= run9ReadViewArchiveLinkBytes {
			walker.links[inode] = &run9ReadViewArchiveLink{name: header.Name, remaining: nlink - 1}
			walker.linkBytes += cost
		}
	}
	return walker.archive.addFile(header, walker.handler.fs, fsPath)
}

// header describes one entry with the ownership and mode the guest sees
// (run9ReadViewGuest) and its mtime. It returns nil for a socket, which tar
// cannot represent; like GNU tar, the archive omits it.
func (walker *run9ReadViewArchiveWalker) header(fsPath string, name string, stat *fs.FileStat) (*tar.Header, error) {
	attr := stat.Attr()
	header := &tar.Header{Name: name, Format: tar.FormatPAX, ModTime: stat.ModTime()}
	switch attr.Typ {
	case meta.TypeDirectory:
		header.Typeflag, header.Name = tar.TypeDir, name+"/"
	case meta.TypeFile:
		header.Typeflag, header.Size = tar.TypeReg, stat.Size()
	case meta.TypeSymlink:
		target, errno := walker.handler.fs.Readlink(walker.archive.metaCtx, fsPath)
		if errno != 0 {
			return nil, errno
		}
		header.Typeflag, header.Linkname = tar.TypeSymlink, string(target)
	case meta.TypeCharDev, meta.TypeBlockDev:
		header.Typeflag = tar.TypeChar
		if attr.Typ == meta.TypeBlockDev {
			header.Typeflag = tar.TypeBlock
		}
		// FUSE stores the kernel's new_encode_dev form.
		header.Devmajor = int64(attr.Rdev>>8) & 0xfff
		header.Devminor = int64(attr.Rdev&0xff) | int64(attr.Rdev>>12)&^0xff
	case meta.TypeFIFO:
		header.Typeflag = tar.TypeFifo
	default:
		return nil, nil
	}
	guest, err := run9ReadViewGuest(walker.handler.fs, walker.archive.metaCtx, fsPath, stat)
	if err != nil {
		return nil, fmt.Errorf("describe %q: %w", name, err)
	}
	header.Mode, header.Uid, header.Gid = int64(guest.mode), int(guest.uid), int(guest.gid)
	return header, nil
}
