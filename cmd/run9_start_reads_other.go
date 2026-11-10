//go:build !linux

package cmd

import "github.com/juicedata/juicefs/pkg/vfs"

// startRun9StartReads does nothing: run9 Box mounts run on Linux only.
func startRun9StartReads(_ *vfs.VFS, _ string, _ string) {
	logger.Warnf("start reads are not supported on this platform")
}

func finishRun9StartReads() {}
