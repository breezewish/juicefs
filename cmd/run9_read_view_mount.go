package cmd

import (
	"fmt"
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
	Open(meta.Context, string, uint32) (*fs.File, syscall.Errno)
	ReadDirPage(meta.Context, string, int, string) ([]*fs.FileStat, string, syscall.Errno)
}

// run9MountedReadFilesystem composes two immutable read views without FUSE.
// Missing mount ancestors are visible even before the first VM start creates
// their physical placeholders in Root.
type run9MountedReadFilesystem struct {
	root, data *fs.FileSystem
	mount      string
	dataRoot   *fs.FileStat
}

func newRun9MountedReadFilesystem(ctx meta.Context, root, data *fs.FileSystem, mountPath string) (*run9MountedReadFilesystem, error) {
	if mountPath == "/" || !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath || strings.ContainsRune(mountPath, 0) {
		return nil, fmt.Errorf("data mount must be a canonical absolute path other than root")
	}
	mount := run9ReadViewRoot + mountPath
	current := ""
	for _, name := range strings.Split(strings.TrimPrefix(mount, "/"), "/") {
		current += "/" + name
		stat, errno := root.Lstat(ctx, current)
		if errno == syscall.ENOENT {
			break
		}
		if errno != 0 {
			return nil, errno
		}
		if !stat.IsDir() {
			return nil, fmt.Errorf("data mount has a non-directory or symlink ancestor: %s", current)
		}
		if current == mount {
			entries, _, errno := root.ReadDirPage(ctx, current, 1, "")
			if errno != 0 {
				return nil, errno
			}
			if len(entries) != 0 {
				return nil, fmt.Errorf("data mount is not empty: %s", mountPath)
			}
		}
	}
	stat, errno := data.Lstat(ctx, run9ReadViewRoot)
	if errno != 0 {
		return nil, errno
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("data filesystem root is not a directory")
	}
	return &run9MountedReadFilesystem{root: root, data: data, mount: mount, dataRoot: stat}, nil
}

func (m *run9MountedReadFilesystem) Lstat(ctx meta.Context, name string) (*fs.FileStat, syscall.Errno) {
	if name == m.mount || strings.HasPrefix(name, m.mount+"/") {
		return m.data.Lstat(ctx, run9ReadViewRoot+strings.TrimPrefix(name, m.mount))
	}
	stat, errno := m.root.Lstat(ctx, name)
	if errno == syscall.ENOENT && strings.HasPrefix(m.mount, name+"/") {
		return m.dataRoot.WithName(path.Base(name)), 0
	}
	return stat, errno
}

func (m *run9MountedReadFilesystem) Readlink(ctx meta.Context, name string) ([]byte, syscall.Errno) {
	if name == m.mount || strings.HasPrefix(name, m.mount+"/") {
		return m.data.Readlink(ctx, run9ReadViewRoot+strings.TrimPrefix(name, m.mount))
	}
	return m.root.Readlink(ctx, name)
}

func (m *run9MountedReadFilesystem) Open(ctx meta.Context, name string, flags uint32) (*fs.File, syscall.Errno) {
	if name == m.mount || strings.HasPrefix(name, m.mount+"/") {
		return m.data.Open(ctx, run9ReadViewRoot+strings.TrimPrefix(name, m.mount), flags)
	}
	return m.root.Open(ctx, name, flags)
}

func (m *run9MountedReadFilesystem) ReadDirPage(ctx meta.Context, name string, limit int, cursor string) ([]*fs.FileStat, string, syscall.Errno) {
	if name == m.mount || strings.HasPrefix(name, m.mount+"/") {
		return m.data.ReadDirPage(ctx, run9ReadViewRoot+strings.TrimPrefix(name, m.mount), limit, cursor)
	}
	entries, next, errno := m.root.ReadDirPage(ctx, name, limit, cursor)
	if !strings.HasPrefix(m.mount, name+"/") {
		return entries, next, errno
	}
	if errno != 0 && errno != syscall.ENOENT {
		return nil, "", errno
	}
	child := strings.SplitN(strings.TrimPrefix(m.mount, name+"/"), "/", 2)[0]
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
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	}
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
	if h.dataMount != "" && (name == h.dataMount || strings.HasPrefix(name, h.dataMount+"/")) {
		return strings.TrimSuffix(etag, "\"") + "-data\""
	}
	return etag
}
