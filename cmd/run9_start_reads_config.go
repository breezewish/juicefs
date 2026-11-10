package cmd

import "time"

// This file is the one place that configures run9 Box start reads (see
// vfs.TraceStartReads and run9_start_reads_linux.go). run9rt turns them on per
// mount through the environment rather than flags, so a mismatched pair never
// fails a mount: an older juicefs ignores run9PrefetchHostDirEnv and prefetches
// without host-wide limits, and with an older run9rt, which never sets it,
// this juicefs records starts but prefetches nothing until run9rt catches up.
// Run9's design document
// docs/features/box-start-prefetch/design_box_start_prefetch.md explains each
// value.
const (
	// run9StartReadsEnv names the file listing what the filesystem's
	// previous start read. Set, the mount prefetches that list and replaces it
	// with the reads of its own start.
	run9StartReadsEnv = "JFS_RUN9_START_READS"
	// run9PrefetchHostDirEnv names the host directory whose lock files every
	// mount on the host shares to bound prefetch concurrency and bandwidth
	// together. Without it the mount records its start but prefetches nothing:
	// no mount prefetches outside the host-wide limits.
	run9PrefetchHostDirEnv = "JFS_RUN9_PREFETCH_HOST_DIR"
)

// startReadsConfig holds the limits of one mount's start reads and of all
// prefetches on a host. It is a variable only so tests can shrink it.
var startReadsConfig = struct {
	// Window covers a Box start: boot, the first commands and the programs
	// they start. Reads after it are the workload's, not the start's.
	Window time.Duration
	// MaxFiles and MaxListBytes bound the list a mount writes and reads.
	MaxFiles     int
	MaxListBytes int
	// MaxRangeBytes bounds the total length of the ranges a mount lists. It
	// no longer bounds what a prefetch costs (MountMaxBytes does), but a
	// juicefs from before MountMaxBytes rejects a list past it, so a rollback
	// still reads the lists this one writes.
	MaxRangeBytes uint64
	// MountConcurrency bounds the blocks one mount fetches at once, and so its
	// memory: each fetch holds two buffers of one block (4 MiB by default),
	// the caller's and the download's.
	MountConcurrency int
	// MountMaxBytes bounds the bytes one mount's prefetch fetches from object
	// storage. A sparse list can name one small range in each of many blocks;
	// this caps what its whole blocks cost.
	MountMaxBytes int64
	// Timeout ends a prefetch that is still running.
	Timeout time.Duration
	// HostConcurrency bounds the blocks all mounts on a host fetch at once.
	HostConcurrency int
	// HostBytesPerSecond bounds the rate at which all mounts on a host start
	// fetching block bytes. It is about what one cold Box start's prefetch
	// reached alone (180 blocks in 2.2 s), so a lone start is not slowed and
	// concurrent starts share it instead of taking the host's whole network
	// from the workloads' own reads.
	HostBytesPerSecond int64
}{
	Window:             30 * time.Second,
	MaxFiles:           16384,
	MaxListBytes:       16 << 20,
	MaxRangeBytes:      2 << 30,
	MountConcurrency:   8,
	MountMaxBytes:      2 << 30,
	Timeout:            2 * time.Minute,
	HostConcurrency:    16,
	HostBytesPerSecond: 320 << 20,
}
