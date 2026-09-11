//go:build !linux || nobadger

package cmd

import "github.com/juicedata/juicefs/pkg/vfs"

func installRun9Capture(*vfs.VFS, *uint64) (func(), error) {
	return func() {}, nil
}
