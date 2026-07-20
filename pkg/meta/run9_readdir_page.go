package meta

import "syscall"

// ReaddirPage returns at most limit directory entries ordered by name and
// strictly after after. It is a run9 extension used by immutable Badger readers
// so pagination does not materialize or rescan the complete directory.
//
// The method intentionally lives on kvMeta instead of Meta: only run9's Badger
// read path needs this contract, while adding it to Meta would impose a new
// public requirement on every upstream metadata backend.
func (m *kvMeta) ReaddirPage(ctx Context, inode Ino, after string, limit int, entries *[]*Entry) (more bool, st syscall.Errno) {
	if limit <= 0 {
		return false, syscall.EINVAL
	}
	if after != "" {
		if st := checkInodeName(after); st != 0 {
			return false, st
		}
	}

	inode = m.checkRoot(inode)
	var attr Attr
	if st := m.GetAttr(ctx, inode, &attr); st != 0 {
		return false, st
	}
	if st := m.Access(ctx, inode, MODE_MASK_R|MODE_MASK_X, &attr); st != 0 {
		return false, st
	}

	var cursor interface{}
	if after != "" {
		cursor = []byte(after)
	}
	_, page, err := m.getDirFetcher()(ctx, inode, cursor, 0, limit+1, true)
	if err != nil {
		return false, errno(err)
	}
	if len(page) > limit {
		page = page[:limit]
		more = true
	}
	*entries = page
	return more, 0
}
