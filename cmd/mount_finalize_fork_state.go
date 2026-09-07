package cmd

import (
	"sync"
	"sync/atomic"
)

// forkFinalizeInProgress records an accepted terminal finalize request.
// It must be defined on all platforms because other code paths may reference it.
//
// On Linux, SIGUSR2 sets it under forkMountLifecycle's write lock. It remains
// true until the mount daemon exits after finalizing on the main mount goroutine.
var forkFinalizeInProgress atomic.Bool

// forkFlushDrainInProgress indicates whether the fork-only flush-drain handler is running.
// It is intentionally separate from finalize: flush-drain keeps the mount daemon alive.
var forkFlushDrainInProgress atomic.Bool

// forkMountLifecycle serializes the persistent flush-drain operation with the
// terminal finalize operation. Finalize takes the write lock before unmounting;
// flush-drain holds a read lock until FlushAll and upload drain really finish.
var forkMountLifecycle sync.RWMutex
