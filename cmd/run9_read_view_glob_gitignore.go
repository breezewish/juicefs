package cmd

import (
	"errors"
	"fmt"
	"io"
	"path"
	"syscall"

	ignore "github.com/Sriram-PR/go-ignore"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

const (
	run9ReadViewGlobMaximumGitIgnoreFileBytes = 1 << 20
	run9ReadViewGlobMaximumGitIgnoreBytes     = 4 << 20
	run9ReadViewGlobMaximumGitIgnorePatterns  = 100_000
)

type run9ReadViewGlobIgnoreRules struct {
	matcher         *ignore.Matcher
	bytes           int
	patternOverflow bool
}

func newRun9ReadViewGlobIgnoreRules() *run9ReadViewGlobIgnoreRules {
	rules := &run9ReadViewGlobIgnoreRules{}
	rules.matcher = ignore.NewWithOptions(ignore.MatcherOptions{
		MaxPatterns:      run9ReadViewGlobMaximumGitIgnorePatterns,
		MaxPatternLength: run9ReadViewGlobMaximumGitIgnoreFileBytes,
		WarningHandler: func(warning ignore.ParseWarning) {
			if warning.Line == 0 {
				rules.patternOverflow = true
			}
		},
	})
	return rules
}

func (rules *run9ReadViewGlobIgnoreRules) add(basePath string, content []byte) error {
	if len(content) > run9ReadViewGlobMaximumGitIgnoreFileBytes || rules.bytes > run9ReadViewGlobMaximumGitIgnoreBytes-len(content) {
		return errors.New("gitignore data exceeds file glob limits")
	}
	rules.bytes += len(content)
	rules.matcher.AddPatterns(basePath, content)
	if rules.patternOverflow {
		return errors.New("gitignore pattern count exceeds file glob limits")
	}
	return nil
}

func (rules *run9ReadViewGlobIgnoreRules) ignores(relativePath string, directory bool) bool {
	return rules != nil && rules.matcher.Match(relativePath, directory)
}

func loadRun9ReadViewGlobIgnore(jfs *fs.FileSystem, ctx meta.Context, fsDirectory string, relativeDirectory string, rules *run9ReadViewGlobIgnoreRules) error {
	if rules == nil {
		return nil
	}
	ignorePath := path.Join(fsDirectory, ".gitignore")
	stat, errno := jfs.Lstat(ctx, ignorePath)
	if errno == syscall.ENOENT || errno == syscall.ENOTDIR {
		return nil
	}
	if errno != 0 {
		return errno
	}
	if stat.Attr().Typ != meta.TypeFile || stat.IsSymlink() {
		return nil
	}
	file, errno := jfs.Open(ctx, ignorePath, vfs.MODE_MASK_R)
	if errno != 0 {
		return errno
	}
	reader := &run9ReadViewFile{file: file, ctx: ctx}
	content, err := io.ReadAll(io.LimitReader(reader, run9ReadViewGlobMaximumGitIgnoreFileBytes+1))
	closeErr := reader.Close()
	if err != nil {
		return fmt.Errorf("read .gitignore: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close .gitignore: %w", closeErr)
	}
	if err := rules.add(relativeDirectory, content); err != nil {
		return fmt.Errorf("parse .gitignore: %w", err)
	}
	return nil
}
