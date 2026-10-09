package vfs

import (
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type checkpointRenameHandler struct {
	meta.DirHandler
	name    string
	deleted chan struct{}
	resume  chan struct{}
}

func (h *checkpointRenameHandler) Delete(name string) {
	if name == h.name {
		h.name = ""
		close(h.deleted)
		<-h.resume
	}
}
func (h *checkpointRenameHandler) Insert(_ Ino, name string, _ *Attr) { h.name = name }

func TestRun9DirectoryRenameDoesNotExposeFalseEOF(t *testing.T) {
	d := &checkpointRenameHandler{name: "child", deleted: make(chan struct{}), resume: make(chan struct{})}
	h := &handle{dirHandler: d}
	v := &VFS{handles: map[Ino][]*handle{1: {h}}}
	renamed := make(chan struct{})
	go func() { v.renameDirHandle(1, "child", ".gc.child.token", 2, &Attr{}); close(renamed) }()
	<-d.deleted
	observed := make(chan string, 1)
	go func() { h.Lock(); observed <- d.name; h.Unlock() }()
	select {
	case <-observed:
		t.Fatal("reader saw the delete/insert gap")
	case <-time.After(10 * time.Millisecond):
	}
	close(d.resume)
	require.Equal(t, ".gc.child.token", <-observed)
	<-renamed
}
