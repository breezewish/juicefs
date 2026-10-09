package vfs

// renameDirHandle updates a same-directory rename under each handle's existing
// readdir mutex. Splitting delete and insert lets a reader observe false EOF
// when the renamed entry is its last unread item (notably a lineage member).
func (v *VFS) renameDirHandle(parent Ino, name, newname string, inode Ino, attr *Attr) {
	v.hanleM.Lock()
	hs := append([]*handle(nil), v.handles[parent]...)
	v.hanleM.Unlock()
	for _, h := range hs {
		h.Lock()
		if h.dirHandler != nil {
			h.dirHandler.Delete(name)
			h.dirHandler.Delete(newname)
			h.dirHandler.Insert(inode, newname, attr)
		}
		h.Unlock()
	}
}
