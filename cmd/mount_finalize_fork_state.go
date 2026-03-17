package cmd

import "sync/atomic"

// forkFinalizeInProgress indicates whether the fork-only finalize handler is running.
// It must be defined on all platforms because other code paths may reference it.
//
// On Linux, it is set to true when receiving SIGUSR2 and remains true until the
// mount daemon exits (os.Exit in the finalize handler).
var forkFinalizeInProgress atomic.Bool
