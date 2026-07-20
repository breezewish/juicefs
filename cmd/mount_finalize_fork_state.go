package cmd

import (
	"sync"
	"sync/atomic"
)

// forkFinalizeInProgress indicates whether the fork-only finalize handler is running.
// It must be defined on all platforms because other code paths may reference it.
//
// On Linux, it is set to true when receiving SIGUSR2 and remains true until the
// mount daemon exits (os.Exit in the finalize handler).
var forkFinalizeInProgress atomic.Bool

// forkFlushDrainInProgress indicates whether the fork-only flush-drain handler is running.
// It is intentionally separate from finalize: flush-drain keeps the mount daemon alive.
var forkFlushDrainInProgress atomic.Bool

// forkMountLifecycle serializes the persistent flush-drain operation with the
// terminal finalize operation. Finalize takes the write lock before unmounting;
// flush-drain holds a read lock until FlushAll and upload drain really finish.
var forkMountLifecycle sync.RWMutex
