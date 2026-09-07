# Run9 empty filesystem initialization

This fork adds the hidden `format --run9-init-empty-epoch` option for Run9's
fresh empty Snap creation. Ordinary JuiceFS format, mount and finalize keep
their existing behavior.

After standard format initialization and object-storage validation, a fresh
Badger filesystem receives `rootfs` (mode 0755, invoking process owner), native
directory and filesystem statistics, and the Run9 writable slice allocation
fence in one metadata transaction. No mounted session or data slice exists.
The existing metadata client supplies the storage descriptor and size, then
success is emitted only after checked shutdown flushes Badger's SkipWAL state.

The initializer refuses existing formats and non-fresh allocation counters.
It is not an online mutation API or a metadata repair operation. Run9 still
owns the unpublished temporary directory and atomically publishes it with the
normal Snap ownership, descriptor, epoch and settled-generation records.

Parity tests live in `pkg/meta/run9_empty_filesystem_test.go` and
`cmd/run9_format_empty_test.go`. When syncing upstream, keep the initializer's
native inode, entry and statistics encoding aligned with `kvMeta.doMknod` and
`baseMeta.Mknod`.
