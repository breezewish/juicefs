package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

func cmdRun9CheckEmptyDirectory() *cli.Command {
	return &cli.Command{
		Name:      "check-empty-dir",
		Hidden:    true,
		Usage:     "run9 internal: check an absent or empty directory without following symlinks",
		ArgsUsage: "READONLY-META-URL ABSOLUTE-PATH [ABSOLUTE-PATH...]",
		Action: func(c *cli.Context) error {
			setup0(c, 2, 0)
			return checkRun9EmptyDirectory(c.Context, c.Args().Get(0), c.Args().Slice()[1:]...)
		},
	}
}

func checkRun9EmptyDirectory(ctx context.Context, metaURL string, directories ...string) (retErr error) {
	parsed, err := url.Parse(metaURL)
	if err != nil || parsed.Scheme != "badger" || parsed.Query().Get("readonly") != "1" {
		return fmt.Errorf("empty directory check requires native read-only Badger metadata")
	}
	if len(directories) == 0 {
		return fmt.Errorf("at least one directory is required")
	}
	for _, directory := range directories {
		if directory == "/" || !path.IsAbs(directory) || path.Clean(directory) != directory || strings.ContainsRune(directory, 0) {
			return fmt.Errorf("directory must be a canonical absolute path other than root")
		}
	}
	conf := meta.DefaultConf()
	conf.ReadOnly, conf.NoBGJob = true, true
	client := meta.NewClient(metaURL, conf)
	defer func() { retErr = errors.Join(retErr, client.Shutdown()) }()
	if _, err := client.Load(true); err != nil {
		return err
	}
	for _, directory := range directories {
		if err := checkRun9EmptyDirectoryMetadata(run9ReadViewContext(ctx), client, directory); err != nil {
			return err
		}
	}
	return nil
}

func checkRun9EmptyDirectoryMetadata(ctx meta.Context, client meta.Meta, directory string) error {
	inode := meta.RootInode
	// Inspect every component, including rootfs: accepting an ancestor symlink
	// would allow a future mount to obscure unrelated data.
	for _, name := range strings.Split("rootfs"+directory, "/") {
		var child meta.Ino
		var attr meta.Attr
		errno := client.Lookup(ctx, inode, name, &child, &attr, false)
		if errno == syscall.ENOENT {
			return nil
		}
		if errno != 0 {
			return errno
		}
		if attr.Typ != meta.TypeDirectory {
			return fmt.Errorf("mount path %q has a non-directory or symlink component", directory)
		}
		inode = child
	}
	paged, ok := client.(interface {
		ReaddirPage(meta.Context, meta.Ino, string, int, *[]*meta.Entry) (bool, syscall.Errno)
	})
	if !ok {
		return fmt.Errorf("metadata does not support bounded directory inspection")
	}
	var entries []*meta.Entry
	if _, errno := paged.ReaddirPage(ctx, inode, "", 1, &entries); errno != 0 {
		return errno
	}
	if len(entries) != 0 {
		return fmt.Errorf("mount path %q is not empty", directory)
	}
	return nil
}
