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

- A customized Badger dependency to support SkipWAL.

Related files:

- `pkg/meta/tkv_badger.go`
- `pkg/meta/tkv_badger_nextchunk_test.go`
- `go.mod`

### Meta tests hygiene

- Badger-related unit tests use `t.TempDir()` instead of fixed relative paths to avoid leaving large badger files under `pkg/meta/` after running `go test`.

Related files:

- `pkg/meta/tkv_test.go`
- `pkg/meta/load_dump_test.go`

### Usage reporting disabled by default

- Usage reporting is always disabled by default.

Related files:

- `cmd/flags.go`

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

### Faster Mount

- JFS_SKIP_FAST_FAIL is introduced. When it is set, mount process will not load metadata in stage 0, and only load it in stage 3. This should make mount to be faster.

Related files:

- `cmd/mount.go`

### Umount with Finalize Semantics

- New command `juicefs umount-finalize <mountpoint>`: strict finalize semantics for badger (SkipWAL=true). It sends `SIGUSR2` to the mount daemon and returns success only after receiving a success finalize ack written by the mount daemon.
  - A short force-umount observation is done in parallel, but it does not affect correctness (ack is the source of truth).

- Mount daemon handles `SIGUSR2` by quiescing FUSE, flushing VFS, draining writeback uploads, closing metadata session, shutting down metadata/object storage, writing finalize ack atomically, then exiting with `0` on success or `meta.UmountCode` on failure.

- `umount-finalize` writes a request file alongside the ack path to pass `--finalize-timeout` to the mount daemon. The mount daemon uses it to bound `WaitForUploadDrain(ctx)` and always writes a terminal ack (error/panic) before the caller times out (small headroom is reserved).

- `cachedStore.WaitForUploadDrain(ctx)` to wait for pending writeback uploads and empty `rawstaging` across all cache dirs.

- When a pending block still exists in bookkeeping but its `rawstaging` path is gone, both `uploadStagingFile()` and `cachedStore.WaitForUploadDrain(ctx)` recover only if the keyed cache copy can still be fully read and verified with the configured checksum. Missing or corrupt cache copies stay fatal so finalize keeps exposing real data-loss evidence instead of timing out or silently degrading.

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
