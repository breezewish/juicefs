//go:build !linux || !run9_checkpoint_research || nobadger

package cmd

import "github.com/juicedata/juicefs/pkg/vfs"

func installResearchCheckpoint(_ *vfs.VFS, _ *uint64) (func(), error) {
	return func() {}, nil
}
