package meta

import (
	"context"
	"errors"
	"syscall"
)

// Run9PreparedFork is the private copy's usage once inherited state is gone,
// in the same terms as `juicefs status`.
type Run9PreparedFork struct {
	UsedSpace  uint64 `json:"used_space"`
	UsedInodes uint64 `json:"used_inodes"`
}

// Run9PrepareFork removes inherited process-owned state from a private copy.
// It never opens the source database or allocates user data. The runtime applies
// the child's independently reserved writable epoch before its first mount.
// Measuring usage here spares the runtime another open of the same copy.
func Run9PrepareFork(ctx context.Context, directory string) (out Run9PreparedFork, resultErr error) {
	conf := DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	child, err := newKVMeta("badger", directory, conf)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, child.Shutdown()) }()
	if _, err := child.Load(true); err != nil {
		return out, err
	}
	sessions, err := child.ListSessions()
	if err != nil {
		return out, err
	}
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if err := child.(*kvMeta).doCleanStaleSession(session.Sid); err != nil {
			return out, err
		}
	}
	// Session cleanup changes usage only in memory, and Shutdown does not
	// persist it. Persist it so the reported usage is what the child keeps.
	child.(*kvMeta).doFlushStats()
	var total, available, availableInodes uint64
	if st := child.StatFS(Background(), RootInode, &total, &available, &out.UsedInodes, &availableInodes); st != 0 {
		return out, syscall.Errno(st)
	}
	out.UsedSpace = total - available
	return out, ctx.Err()
}
