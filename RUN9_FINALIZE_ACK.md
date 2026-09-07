# Run9 finalize acknowledgement publication

The fork-only `umount-finalize` protocol distinguishes progress notifications
from terminal results. A `pending` acknowledgement identifies the live daemon
and its current phase; it can never prove successful persistence.

All acknowledgements use a complete temporary file, checked close and atomic
rename. Pending notifications do not fsync the temporary file or directory:
their crash durability is unnecessary, and repeated host filesystem journal
flushes can otherwise dominate concurrent Box Stop latency.

Terminal acknowledgements retain the existing file fsync before publication
and best-effort directory fsync afterward. Actual FUSE quiescence, buffered
data flush, writeback upload drain, size measurement, metadata session close
and checked metadata shutdown are unchanged. Only a validated terminal success
acknowledgement permits the caller to report successful finalize.

The implementation and publication tests live in
`cmd/mount_finalize_fork_linux.go` and its test file. Caller validation and
timeout tests live in `cmd/umount_finalize_fork*_test.go`.
