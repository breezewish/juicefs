//go:build !windows && !linux
// +build !windows,!linux

package cmd

import (
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func installForkFinalizeHandler(metaCli meta.Meta, v *vfs.VFS, blob object.ObjectStorage) {
	// Fork feature: linux only.
}
