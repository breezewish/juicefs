# AGENTS.md

## Workflow Principles

- **NEVER** use git to discard or revert any changes not made by you - they are purposely changed by user and you should live with that.

## Documented Divergences

This repo is a fork of upstream JuiceFS (https://github.com/juicedata/juicefs). We intend to keep the codebase as close as possible to upstream, but we also need to make some customizations to better fit our use cases and requirements.

Unless explicitly documented below, changes should preserve parity. Files in docs/ are intentionally leave unchanged even when behavior is changed, as they are only used by upstream.

When introducing new changes, first identify whether it is a divergence from upstream or it makes the code more aligned with upstream. Divergences must be documented in places below to avoid regression when syncing changes from upstream:

- Record divergences in this file, keep words concise but clear, be specific about the new behavior. No need to cover details, just the high level idea and the rationale.
- Record divergences in doc comments.
- Cover divergences via proper tests (unit / end to end).

### Makefile targets

- A new target named `juicefs.run9` is introduced.
  It is the same as `juicefs` build target, just renamed to make it clear that it is for the run9 fork.

Related files:

- `Makefile`

### Badger

- Accept `badger://path?nextchunk=<n>` as the metadata address. Only the query param `nextchunk` is supported. When `nextchunk` is set, it overrides the value of `CnextChunk`. Overriding `CnextChunk` is the core of ensuring forked metadata will not conflict with the original metadata when both are mounted. `nextchunk` is allowed to be set to any value. The caller is responsible for ensuring the value is properly set to avoid conflicts. If `nextchunk` is not set, it means nextchunk will not be overridden (upstream original behavior is preserved).

- When `nextchunk` is set, run9 also writes `Crun9NextChunkLimit` and rejects `NewSlice` after that writable epoch range is exhausted. This keeps one prepared metadata lineage inside one owned slice-id epoch range.

- A customized Badger dependency supports SkipWAL and avoids directory size scans when metrics are disabled. run9 disables Badger metrics for per-snap metadata, so the scan would add remote filesystem round trips without producing metrics.

- run9 forces badger `ValueLogFileSize` down to `64 MiB` when opening metadata. run9 keeps badger directories inside the shared_meta JuiceFS mount, and the upstream `1 GiB` default preallocates `2 GiB` `*.vlog` files. In production this has surfaced as dangling vlog entries and `DB.Close` truncate `ENOENT` on the outer JuiceFS/FUSE layer during finalize, so run9 keeps the value logs small to stay on the boring path.

- run9 disables Badger's local `LOCK` file guard. run9rt already holds the distributed per-snap lock before opening writable metadata, while immutable readers use a private clone. The duplicate local guard only adds shared-FUSE metadata round trips to first mount.

- Accept `badger://path?readonly=1` and open Badger in its native read-only mode. This is used only for immutable metadata clones held by the run9 file gateway; it cannot be combined with `nextchunk`.

Related files:

- `pkg/meta/base.go`
- `pkg/meta/tkv_badger.go`
- `pkg/meta/tkv_badger_nextchunk_test.go`
- `pkg/meta/tkv_badger_options_test.go`
- `go.mod`

### Redis Batch Clone

- Redis metadata clones all non-directory entries in one watched transaction,
  using pipelined reads and one atomic publish. This keeps run9 snap forks from
  issuing one Redis transaction per Badger metadata file under burst load.

Related files:

- `pkg/meta/redis_batch_clone_run9.go`

### Immutable Read View Server

- The hidden `serve-read-view` command holds one immutable Badger generation and its object storage client open, and serves GET, HEAD, Range, conditional requests, bounded directory listings, and bounded doublestar globs over regular files through a mode-0600 Unix socket. Requests may select a filesystem root; absolute and relative symlinks remain confined to that root. Glob traversal stays inside the reader so remote callers need only one request.

- `pkg/fs.FileSystem.ReadDirPage` provides bounded, name-cursor directory reads for the immutable reader. It uses a Badger key scan directly instead of materializing the complete directory; other metadata backends keep their upstream contract unchanged.

Related files:

- `cmd/main.go`
- `cmd/run9_read_view.go`
- `cmd/run9_read_view_glob.go`
- `cmd/run9_read_view_test.go`
- `pkg/meta/run9_readdir_page.go`
- `pkg/fs/run9_readdir_page.go`
- `pkg/fs/run9_readdir_page_test.go`

### Meta tests hygiene

- Badger-related unit tests use `t.TempDir()` instead of fixed relative paths to avoid leaving large badger files under `pkg/meta/` after running `go test`.

Related files:

- `pkg/meta/tkv_test.go`
- `pkg/meta/load_dump_test.go`

### Usage reporting disabled by default

- Usage reporting is always disabled by default.

Related files:

- `cmd/flags.go`

### Immutable Shared Block Cache

- `--shared-cache-dir` adds one pre-populated read-only cache source after the
  mount's private cache. JuiceFS never writes, stages, evicts, removes, or
  repairs files there; misses continue through the normal object-store path and
  any downloaded block is cached only in the private cache directory.

Related files:

- `cmd/flags.go`
- `cmd/mount.go`
- `pkg/chunk/cached_store.go`
- `pkg/chunk/cached_store_shared_cache_fork.go`
- `pkg/chunk/cached_store_shared_cache_fork_test.go`

### Badger Close Safely

- Metadata client is explicitly shut down after format via `m.Shutdown()`. It prevents a bug where badger is not properly closed when format exits. As badger is customized without WAL, not closing it properly can cause data corruption as data is still in memtable and not flushed to disk.

  TODO: Find a way to ensure memtables are flushed even on fatal/abnormal exits (e.g. `logger.Fatalf` / `os.Exit` bypass defers), so we don't rely on always calling `Shutdown()`.

- Metadata client is explicitly shut down when mount exits via `metaCli.Shutdown()` for the same reason.

Related files:

- `cmd/format.go`
- `cmd/format_test.go`
- `cmd/mount.go`
- `cmd/mount_shutdown_fork.go`
- `cmd/mount_unix.go`
- `cmd/mount_shutdown_test.go`

### Mount Supervisor Cleanup

- When `JFS_RUN9_SUPERVISOR_RECORD` is set, the background mount supervisor writes its pid + starttime to that record file before launching the child mount process. run9rt uses that identity to kill the supervisor if startup later fails before a normal `umount-finalize` path can prove and clean it up.

- When `JFS_RUN9_MOUNT_RECORD` is set, the background mount child writes its own pid + starttime as soon as stage 3 starts. run9rt uses that identity to trust the live mount process before JuiceFS publishes `.jfs.config`, so box cold start no longer pays an extra wait for that file.

Related files:

- `cmd/mount.go`
- `cmd/mount_run9_supervisor_record_fork.go`
- `cmd/mount_run9_supervisor_record_fork_test.go`
- `cmd/mount_run9_mount_record.go`
- `cmd/mount_run9_mount_record_fork.go`
- `cmd/mount_run9_mount_record_fork_test.go`

### Umount with Finalize Semantics

- New command `juicefs umount-finalize <mountpoint>`: strict finalize semantics for badger (SkipWAL=true). It sends `SIGUSR2` to the mount daemon and returns success only after receiving a success finalize ack written by the mount daemon.
  - A short force-umount observation is done in parallel, but it does not affect correctness (ack is the source of truth).

- Mount daemon handles `SIGUSR2` by quiescing FUSE, flushing VFS, draining writeback uploads, closing metadata session, shutting down metadata/object storage, writing finalize ack atomically, then exiting with `0` on success or `meta.UmountCode` on failure.

- `umount-finalize` writes a request file alongside the ack path to pass `--finalize-timeout` to the mount daemon. The mount daemon uses it to bound `WaitForUploadDrain(ctx)` and always writes a terminal ack (error/panic) before the caller times out (small headroom is reserved).

- `cachedStore.WaitForUploadDrain(ctx)` to wait for pending writeback uploads and empty `rawstaging` across all cache dirs.

- During `WaitForUploadDrain(ctx)`, finalize actively re-queues current pending items that are not already uploading, so drain proof does not depend on the background delayed-upload scanner to make progress.

- When a pending block still exists in bookkeeping but its `rawstaging` path is gone, both `uploadStagingFile()` and `cachedStore.WaitForUploadDrain(ctx)` recover only if the keyed cache copy can still be fully read and verified with the configured checksum, or if the remote object can already be fully read and decompressed to the original block size. Missing or corrupt local copies stay fatal unless the remote object proves that the pending upload has already completed, so finalize keeps exposing real data-loss evidence instead of timing out or silently degrading.

- Disk-cache recovery for such pending blocks opens the keyed cache hardlink directly and does not trust the in-memory cache key index. The index is an accelerator and may miss a still-existing hardlink during cache scan races; pending upload recovery must use the filesystem copy as the authoritative recovery source.

- Chunk unit tests avoid `mockey` (assembly-based monkey patching; Go version/arch sensitive). Instead, tests override small package-level function variables like `diskUsageFn` / `statPathForUploadDrain`.

Related files:

- `cmd/main.go`
- `cmd/fork_finalize_shared_fork.go`
- `cmd/umount_finalize_fork.go`
- `cmd/umount_finalize_fork_windows.go`
- `cmd/mount_unix.go`
- `cmd/mount_finalize_fork_linux.go`
- `cmd/mount_finalize_fork_nonlinux.go`
- `pkg/chunk/cached_store.go`
- `pkg/chunk/cached_store_finalize_fork.go`
- `pkg/chunk/cached_store_test.go`
- `cmd/umount_finalize_fork_test.go`
- `cmd/umount_finalize_fork_linux_test.go`
- `cmd/mount_finalize_fork_linux_test.go`
- `pkg/chunk/cached_store_finalize_fork_test.go`

### Deferred File Fsync Flush

- When `JFS_DEFER_FSYNC_FLUSH=1` is set, file `fsync` returns without forcing the current file handle to flush its writer. This is only valid for callers that use a stronger publish fence, such as `umount-finalize`, before the data becomes forkable or externally visible. The option keeps block-disk workloads from fragmenting JuiceFS writeback into tiny per-guest-fsync uploads.
- New command `juicefs flush-drain <mountpoint>` sends `SIGUSR1` to a live mount daemon and waits for an ack after `FlushAll("")` plus writeback upload drain. Unlike `umount-finalize`, it keeps FUSE, metadata, object storage, and the mount daemon alive. It exists for mounted volumes that use deferred fsync but need an explicit mid-life publish fence. The daemon serializes it against terminal finalize and never abandons a timed-out `FlushAll` goroutine while the mount stays alive.

Related files:

- `cmd/mount.go`
- `cmd/main.go`
- `cmd/flush_drain_fork.go`
- `cmd/flush_drain_fork_other.go`
- `cmd/mount_flush_drain_fork_linux.go`
- `cmd/mount_flush_drain_fork_linux_test.go`
- `pkg/vfs/vfs.go`
- `pkg/vfs/vfs_test.go`

### Run9 Deleted Snap Object GC

- Hidden internal commands `list-live-slices`, `describe-format`, and `gc-slice-ranges` expose the minimal metadata and object-store operations needed by run9rt deleted snap object GC.
- `list-live-slices` reports the format name, object block layout, and slice ids/sizes from one metadata DB; run9rt passes `--scan-pending` when materializing manifests from metadata state instead of only the live view.
- `describe-format` reports the persisted object storage descriptor and object layout so runtime can operate after candidate metadata has been removed, and also seeds a prepared writable epoch through the `?nextchunk=` badger path.
- `gc-slice-ranges` lists only the loaded format prefix, parses `slice_id` from object keys, and deletes only keys whose `slice_id` falls within the requested ranges. It may stop early at `max_delete_objects` and return `has_more=true`.
- When the loaded storage is sharded, range deletion reuses bulk delete and fans out independent shard groups in parallel so GC does not serialize shard-local deletes.

Related files:

- `cmd/main.go`
- `cmd/run9_gc.go`
- `cmd/run9_gc_test.go`

## Important: Isolating Divergences

Prefer to use a new file to isolate changed logic from upstream JuiceFS, and keep the original file as a subset of the upstream's file, if the changed logic is significant. In this way, we can easily learn what has changed from upstream, and reduce merge conflicts when syncing from upstream.

Divergences should be committed using `run9: ...` as the commit message prefix, so that they can be easily identified in the commit history.

## Divergences Engineering Rules

This project requires extremely high code quality and maintainability. Best engineering practices must be followed at all times.

The rules below are some typical principles, applied to divergences. They are not exhaustive, and you must always use your best judgment to **write the cleanest code possible**. You must always clean up and refactor immediately when you see opportunities or any violations of these engineering rules.

For non-divergent code, we should keep as it is in upstream as much as possible, even if it may not be the best code.

### Core Principles: Simplicity & Readability

- Boring Code - Obvious, self-explanatory > clever, minimize cognitive load
- Single Responsibility - One function, one job
- Explicit over Implicit - Clear is better than concise
- Meaningful Abstractions - Only when they reduce cognitive load
- Keep DRY - Only if it does not conflict with the above principles

### Better Maintainability

- Keep the public API surface minimal; prefer reusing existing methods
- Don't treat "looks similar" as "equivalent"
- Abstractions must be meaningful
- Keep certainty, single source of truth - e.g. don't introduce "optional" unless absolutely necessary
- Always add clear, concise and explicit doc comments for public APIs, complex logic, and non-obvious code

### Naming Symbols

- Choosing the name that needs the least explanation, consider: verb clarity, noun specificity, context
- Name tests by behavior and expectation (e.g. `test_restore_keyspace_with_failed_store`).
