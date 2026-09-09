package meta

import (
	"fmt"
	"math"
	"time"
)

// Run9RootPermissions is guest metadata for the empty rootfs directory. Host
// inode ownership stays with the runtime, including when guest mode is 0000.
type Run9RootPermissions struct {
	UID, GID uint32
	Mode     uint32
}

// InitRun9EmptyFilesystem initializes run9's rootfs directory and allocation
// fence in a freshly formatted, exclusively owned Badger filesystem. It is not
// a general mkdir or an existing-volume repair operation. Native encoders keep
// the on-disk schema identical to Mknod, while one transaction persists the
// directory, counters and directory statistics without a mounted session's
// deferred accounting. The caller must successfully Shutdown before publishing
// this filesystem; Badger SkipWAL does not make transaction commit a disk fence.
func InitRun9EmptyFilesystem(client Meta, epoch uint64, uid, gid uint32, guest *Run9RootPermissions) error {
	if guest != nil && (guest.UID == math.MaxUint32 || guest.GID == math.MaxUint32 || guest.Mode > 07777) {
		return fmt.Errorf("invalid initial guest rootfs permissions")
	}
	m, ok := client.(*kvMeta)
	if !ok || m.client.name() != "badger" || m.conf.ReadOnly {
		return fmt.Errorf("empty filesystem initialization requires writable Badger metadata")
	}
	if epoch == 0 || epoch > uint64(math.MaxInt64>>32) {
		return fmt.Errorf("invalid writable epoch %d", epoch)
	}
	start := int64(epoch << 32)
	limit := int64(math.MaxInt64)
	if start <= math.MaxInt64-(1<<32) {
		limit = start + (1 << 32)
	}
	return m.txn(Background(), func(tx *kvTxn) error {
		if tx.get(m.fmtKey("setting")) == nil ||
			parseCounter(tx.get(m.counterKey("nextInode"))) != 2 ||
			parseCounter(tx.get(m.counterKey("nextChunk"))) != 1 ||
			parseCounter(tx.get(m.counterKey(usedSpace))) != 0 ||
			parseCounter(tx.get(m.counterKey(totalInodes))) != 0 ||
			tx.get(m.entryKey(RootInode, "rootfs")) != nil {
			return fmt.Errorf("empty filesystem initialization requires fresh metadata")
		}
		var parent Attr
		root := tx.get(m.inodeKey(RootInode))
		if root == nil {
			return fmt.Errorf("formatted filesystem root is missing")
		}
		m.parseAttr(root, &parent)
		if parent.Typ != TypeDirectory || parent.Nlink != 2 || parent.DefaultACL != 0 || parent.Mode != 0777 {
			return fmt.Errorf("formatted filesystem root was modified")
		}
		now := time.Now()
		attr := Attr{
			Typ: TypeDirectory, Mode: 0755, Uid: uid, Gid: gid,
			Nlink: 2, Length: 4096, Parent: RootInode,
			Atime: now.Unix(), Atimensec: uint32(now.Nanosecond()),
			Mtime: now.Unix(), Mtimensec: uint32(now.Nanosecond()),
			Ctime: now.Unix(), Ctimensec: uint32(now.Nanosecond()),
		}
		parent.Nlink++
		parent.Mtime, parent.Mtimensec = attr.Mtime, attr.Mtimensec
		parent.Ctime, parent.Ctimensec = attr.Ctime, attr.Ctimensec
		const inode = Ino(2)
		tx.set(m.inodeKey(RootInode), m.marshal(&parent))
		tx.set(m.inodeKey(inode), m.marshal(&attr))
		if guest != nil {
			// Same encoding as image import and guest chmod/chown. Do not write
			// guest IDs into host attributes: rootless virtiofs must retain access.
			tx.set(m.xattrKey(inode, "user.containers.override_stat"), []byte(fmt.Sprintf("%d:%d:0%o", guest.UID, guest.GID, guest.Mode)))
		}
		tx.set(m.entryKey(RootInode, "rootfs"), m.packEntry(TypeDirectory, inode))
		tx.set(m.dirStatKey(inode), m.packDirStat(&dirStat{}))
		// Directory length is not included in the parent's logical file bytes,
		// but its 4 KiB allocation and one inode are included, matching Mknod.
		tx.set(m.dirStatKey(RootInode), m.packDirStat(&dirStat{space: 4096, inodes: 1}))
		tx.set(m.counterKey("nextInode"), packCounter(3))
		tx.set(m.counterKey(usedSpace), packCounter(4096))
		tx.set(m.counterKey(totalInodes), packCounter(1))
		tx.set(m.counterKey("nextChunk"), packCounter(start))
		tx.set(m.counterKey("run9NextChunkLimit"), packCounter(limit))
		return nil
	})
}
