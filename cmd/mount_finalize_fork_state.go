package cmd

import "sync"

// forkFinalizeInProgress records terminal finalize or normal mount teardown.
// It must be defined on all platforms because other code paths may reference it.
//
// Every access is protected by forkMountLifecycle. Once accepted, it remains
// true until the mount daemon exits.
var forkFinalizeInProgress bool

// forkMountPublish serializes flush fences and online metadata capture. A channel
// allows flush callers to wait with their deadline, rather than drop requests.
var forkMountPublish = make(chan struct{}, 1)

// forkMountLifecycle serializes the persistent flush-drain operation with the
// terminal finalize operation. Finalize takes the write lock before unmounting;
// flush-drain holds a read lock until FlushAll and upload drain really finish.
var forkMountLifecycle sync.RWMutex
