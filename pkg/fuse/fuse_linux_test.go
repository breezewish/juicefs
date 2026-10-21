package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/posixtest"
	"golang.org/x/sys/unix"
)

func init() {
	posixtest.All["ReopenUnlinkedByHandle"] = ReopenUnlinkedByHandle
}

// ReopenUnlinkedByHandle covers virtiofs and overlayfs, which reopen an
// unlinked file through a path handle after the last name is gone.
func ReopenUnlinkedByHandle(t *testing.T, mp string) {
	path := filepath.Join(mp, "status")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	handle, err := syscall.Open(path, unix.O_PATH, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(handle)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/self/fd/%d", handle))
	if err != nil {
		t.Fatalf("reopen unlinked file: %s", err)
	}
	if string(data) != "old" {
		t.Fatalf("expect old but got %q", data)
	}
}
