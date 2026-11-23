package meta

import (
	"context"
	"errors"
	"syscall"
)

// Run9Usage is a database's used space and inodes, in the terms of
// `juicefs status`.
type Run9Usage struct {
	UsedSpace  uint64 `json:"used_space"`
	UsedInodes uint64 `json:"used_inodes"`
}

// Run9PrepareFork removes inherited process-owned state from a private copy
// captured through the legacy capture protocol, and reports its usage. It
// never opens the source database or allocates user data. The runtime applies
// the child's independently reserved writable epoch before its first mount.
func Run9PrepareFork(ctx context.Context, directory string) (out Run9Usage, resultErr error) {
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
	if _, err := child.(*kvMeta).Run9CleanInheritedSessions(); err != nil {
		return out, err
	}
	var total, available, availableInodes uint64
	if st := child.StatFS(Background(), RootInode, &total, &available, &out.UsedInodes, &availableInodes); st != 0 {
		return out, syscall.Errno(st)
	}
	out.UsedSpace = total - available
	return out, ctx.Err()
}

// Run9CleanInheritedSessions removes every session recorded in the database
// before this process creates its own. A writable run9 Badger has exactly one
// client, so a recorded session belongs to a process that no longer serves it:
// the source mount of an online Fork copy, or a crashed mount. Cleaning
// releases that session's locks and deletes the files it kept open after
// unlink. Session cleanup changes usage only in memory, so it is persisted
// here: the usage that follows is what the database keeps.
func (m *kvMeta) Run9CleanInheritedSessions() (int, error) {
	sessions, err := m.ListSessions()
	if err != nil {
		return 0, err
	}
	for _, session := range sessions {
		if err := m.doCleanStaleSession(session.Sid); err != nil {
			return 0, err
		}
	}
	if len(sessions) > 0 {
		m.doFlushStats()
	}
	return len(sessions), nil
}
