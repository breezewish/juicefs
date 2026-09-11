package meta

import (
	"context"
	"errors"
)

// Run9PrepareFork removes inherited process-owned state from a private copy.
// It never opens the source database or allocates user data. The runtime applies
// the child's independently reserved writable epoch before its first mount.
func Run9PrepareFork(ctx context.Context, directory string) (resultErr error) {
	conf := DefaultConf()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	child, err := newKVMeta("badger", directory, conf)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, child.Shutdown()) }()
	sessions, err := child.ListSessions()
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := child.(*kvMeta).doCleanStaleSession(session.Sid); err != nil {
			return err
		}
	}
	return ctx.Err()
}
