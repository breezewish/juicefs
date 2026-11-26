package cmd

import (
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

// Paths in this interface are already resolved in the Box namespace. In
// particular, symlinks are resolved before selecting Root or Data.
type run9ReadFilesystem interface {
	Lstat(meta.Context, string) (*fs.FileStat, syscall.Errno)
	Readlink(meta.Context, string) ([]byte, syscall.Errno)
	OpenRead(meta.Context, string) (io.ReadSeekCloser, syscall.Errno)
	ReadDirPage(meta.Context, string, int, string) ([]*fs.FileStat, string, syscall.Errno)
}

// run9MountedReadFilesystem composes immutable read views without FUSE.
// Missing mount ancestors are visible even before the first VM start creates
// their physical placeholders in Root.
type run9MountedReadFilesystem struct {
	root    run9ReadFilesystem
	volumes []run9ReadMount
}

type run9ReadMount struct {
	data     run9ReadFilesystem
	mount    string
	dataRoot *fs.FileStat
}

func newRun9MountedReadFilesystem(ctx meta.Context, root run9ReadFilesystem, volumes []run9ReadMount) (*run9MountedReadFilesystem, error) {
	for i := range volumes {
		volume := &volumes[i]
		mountPath := volume.mount
		if mountPath == "/" || !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath || strings.ContainsRune(mountPath, 0) {
			return nil, fmt.Errorf("data mount must be a canonical absolute path other than root")
		}
		for _, previous := range volumes[:i] {
			mount := run9ReadViewRoot + mountPath
			if mount == previous.mount || strings.HasPrefix(mount, previous.mount+"/") || strings.HasPrefix(previous.mount, mount+"/") {
				return nil, fmt.Errorf("data mount paths overlap")
			}
		}
		if err := checkReadViewMountTarget(ctx, root, mountPath); err != nil {
			return nil, err
		}
		stat, errno := volume.data.Lstat(ctx, run9ReadViewRoot)
		if errno != 0 {
			return nil, errno
		}
		if !stat.IsDir() {
			return nil, fmt.Errorf("data filesystem root is not a directory")
		}
		volume.mount, volume.dataRoot = run9ReadViewRoot+mountPath, stat
	}
	return &run9MountedReadFilesystem{root: root, volumes: volumes}, nil
}

// backing selects storage only after the caller resolves guest symlinks.
func (m *run9MountedReadFilesystem) backing(name string) (run9ReadFilesystem, string) {
	for _, volume := range m.volumes {
		if name == volume.mount || strings.HasPrefix(name, volume.mount+"/") {
			return volume.data, run9ReadViewRoot + strings.TrimPrefix(name, volume.mount)
		}
	}
	return m.root, name
}

func (m *run9MountedReadFilesystem) Lstat(ctx meta.Context, name string) (*fs.FileStat, syscall.Errno) {
	backing, physical := m.backing(name)
	stat, errno := backing.Lstat(ctx, physical)
	for _, volume := range m.volumes {
		if errno == syscall.ENOENT && strings.HasPrefix(volume.mount, name+"/") {
			return volume.dataRoot.WithName(path.Base(name)), 0
		}
	}
	return stat, errno
}

// metadataBacking returns the inode's reader and physical path. A synthetic
// mount ancestor is a renamed copy of the exact Volume root stat from Lstat.
func (m *run9MountedReadFilesystem) metadataBacking(name string, stat *fs.FileStat) (run9ReadFilesystem, string) {
	for _, volume := range m.volumes {
		if stat.Attr() == volume.dataRoot.Attr() {
			return volume.data, run9ReadViewRoot
		}
	}
	return m.backing(name)
}

func (m *run9MountedReadFilesystem) Readlink(ctx meta.Context, name string) ([]byte, syscall.Errno) {
	backing, physical := m.backing(name)
	return backing.Readlink(ctx, physical)
}

func (m *run9MountedReadFilesystem) OpenRead(ctx meta.Context, name string) (io.ReadSeekCloser, syscall.Errno) {
	backing, physical := m.backing(name)
	return backing.OpenRead(ctx, physical)
}

func (m *run9MountedReadFilesystem) ReadDirPage(ctx meta.Context, name string, limit int, cursor string) ([]*fs.FileStat, string, syscall.Errno) {
	backing, physical := m.backing(name)
	entries, next, errno := backing.ReadDirPage(ctx, physical, limit, cursor)
	children := make(map[string]bool)
	for _, volume := range m.volumes {
		if strings.HasPrefix(volume.mount, name+"/") {
			child := strings.SplitN(strings.TrimPrefix(volume.mount, name+"/"), "/", 2)[0]
			children[child] = true
		}
	}
	if len(children) == 0 {
		return entries, next, errno
	}
	if errno != 0 && errno != syscall.ENOENT {
		return nil, "", errno
	}
	for child := range children {
		if child > cursor {
			stat, errno := m.Lstat(ctx, path.Join(name, child))
			if errno != 0 {
				return nil, "", errno
			}
			// Replace an existing empty placeholder, or insert the virtual entry
			// into the native name-ordered page before choosing its next cursor.
			found := false
			for i, entry := range entries {
				if entry.Name() == child {
					entries[i], found = stat.WithName(child), true
					break
				}
			}
			if !found {
				entries = append(entries, stat.WithName(child))
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	more := next != "" || len(entries) > limit
	if len(entries) > limit {
		entries = entries[:limit]
	}
	next = ""
	if more && len(entries) != 0 {
		next = entries[len(entries)-1].Name()
	}
	return entries, next, 0
}

func (h *run9ReadViewHandler) fileETag(name string, stat *fs.FileStat) string {
	etag := run9ReadViewETag(h.generation, stat)
	// Inode numbers are only unique inside one filesystem. Preserve the
	// existing Root-only ETag and add a partition for composed views.
	if i := h.dataMountIndex(name); i >= 0 {
		return strings.TrimSuffix(etag, "\"") + fmt.Sprintf("-data%d\"", i)
	}
	return etag
}

func checkReadViewMountTarget(ctx meta.Context, root run9ReadFilesystem, target string) error {
	name := run9ReadViewRoot
	rootStat, errno := root.Lstat(ctx, name)
	if errno != 0 {
		return errno
	}
	if !rootStat.IsDir() {
		return fmt.Errorf("filesystem root is not a directory")
	}
	for _, part := range strings.Split(strings.TrimPrefix(target, "/"), "/") {
		name = path.Join(name, part)
		info, errno := root.Lstat(ctx, name)
		if errno == syscall.ENOENT {
			return nil
		}
		if errno != 0 {
			return errno
		}
		if !info.IsDir() {
			return fmt.Errorf("mount path %q has a non-directory or symlink component", target)
		}
	}
	page, _, errno := root.ReadDirPage(ctx, name, 1, "")
	if errno != 0 {
		return errno
	}
	if len(page) > 0 {
		return fmt.Errorf("mount path %q is not empty", target)
	}
	return nil
}

// dataMountIndex returns which Volume of a composed view holds the resolved
// name, or -1 for Root. Together with the inode it identifies one file.
func (h *run9ReadViewHandler) dataMountIndex(name string) int {
	for i, mount := range h.dataMounts {
		if name == mount || strings.HasPrefix(name, mount+"/") {
			return i
		}
	}
	return -1
}
