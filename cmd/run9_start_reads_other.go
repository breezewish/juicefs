//go:build !linux

package cmd

import "github.com/juicedata/juicefs/pkg/vfs"

const run9StartReadsEnv = "JFS_RUN9_START_READS"

// startRun9StartReads does nothing: run9 Box mounts run on Linux only.
func startRun9StartReads(_ *vfs.VFS, _ string) {
	logger.Warnf("start reads are not supported on this platform")
}

func finishRun9StartReads() {}
