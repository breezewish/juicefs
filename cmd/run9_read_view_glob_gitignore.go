package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"syscall"

	ignore "github.com/Sriram-PR/go-ignore"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

const (
	run9ReadViewGlobMaximumGitIgnoreFileBytes       = 1 << 20
	run9ReadViewGlobMaximumGitIgnoreBytes           = 4 << 20
	run9ReadViewGlobMaximumGitIgnorePatternBytes    = 4096
	run9ReadViewGlobMaximumGitIgnorePatterns        = 100_000
	run9ReadViewGlobMaximumGitIgnoreRuleEvaluations = 20_000_000
)

type run9ReadViewGlobIgnoreBudget struct {
	bytes           int
	patterns        int
	ruleEvaluations int
}

// run9ReadViewGlobIgnoreRules is an immutable directory scope. A child gets a
// new matcher while all scopes share scan-wide parsing and matching budgets.
type run9ReadViewGlobIgnoreRules struct {
	matchers  []*ignore.Matcher
	ruleCount int
	budget    *run9ReadViewGlobIgnoreBudget
}

func newRun9ReadViewGlobIgnoreRules() *run9ReadViewGlobIgnoreRules {
	return &run9ReadViewGlobIgnoreRules{budget: &run9ReadViewGlobIgnoreBudget{}}
}

func (rules *run9ReadViewGlobIgnoreRules) add(basePath string, content []byte) (*run9ReadViewGlobIgnoreRules, error) {
	if len(content) > run9ReadViewGlobMaximumGitIgnoreFileBytes || rules.budget.bytes > run9ReadViewGlobMaximumGitIgnoreBytes-len(content) {
		return nil, errors.New("gitignore data exceeds file glob limits")
	}
	lineStart := 0
	if bytes.HasPrefix(content, []byte{0xef, 0xbb, 0xbf}) {
		lineStart = 3
	}
	for lineEnd := lineStart; lineEnd < len(content); lineEnd++ {
		if content[lineEnd] != '\r' && content[lineEnd] != '\n' {
			continue
		}
		if lineEnd-lineStart > run9ReadViewGlobMaximumGitIgnorePatternBytes {
			return nil, errors.New("gitignore pattern exceeds file glob limits")
		}
		if content[lineEnd] == '\r' && lineEnd+1 < len(content) && content[lineEnd+1] == '\n' {
			lineEnd++
		}
		lineStart = lineEnd + 1
	}
	if len(content)-lineStart > run9ReadViewGlobMaximumGitIgnorePatternBytes {
		return nil, errors.New("gitignore pattern exceeds file glob limits")
	}
	matcher := ignore.NewWithOptions(ignore.MatcherOptions{
		MaxPatterns:      run9ReadViewGlobMaximumGitIgnorePatterns + 1,
		MaxPatternLength: run9ReadViewGlobMaximumGitIgnorePatternBytes,
	})
	matcher.AddPatterns(basePath, content)
	patternCount := matcher.RuleCount()
	if patternCount > run9ReadViewGlobMaximumGitIgnorePatterns || rules.budget.patterns > run9ReadViewGlobMaximumGitIgnorePatterns-patternCount {
		return nil, errors.New("gitignore pattern count exceeds file glob limits")
	}
	rules.budget.bytes += len(content)
	rules.budget.patterns += patternCount
	if patternCount == 0 {
		return rules, nil
	}
	matchers := make([]*ignore.Matcher, len(rules.matchers)+1)
	copy(matchers, rules.matchers)
	matchers[len(rules.matchers)] = matcher
	return &run9ReadViewGlobIgnoreRules{
		matchers:  matchers,
		ruleCount: rules.ruleCount + patternCount,
		budget:    rules.budget,
	}, nil
}

func (rules *run9ReadViewGlobIgnoreRules) ignores(relativePath string, directory bool) (bool, error) {
	if rules == nil || rules.ruleCount == 0 {
		return false, nil
	}
	ruleEvaluations := rules.ruleCount * (strings.Count(relativePath, "/") + 1)
	if rules.budget.ruleEvaluations > run9ReadViewGlobMaximumGitIgnoreRuleEvaluations-ruleEvaluations {
		return false, errors.New("gitignore matching work exceeds file glob limits")
	}
	rules.budget.ruleEvaluations += ruleEvaluations
	for index := len(rules.matchers) - 1; index >= 0; index-- {
		result := rules.matchers[index].MatchWithReason(relativePath, directory)
		if result.Matched {
			return result.Ignored, nil
		}
	}
	return false, nil
}

func loadRun9ReadViewGlobIgnore(jfs *fs.FileSystem, ctx meta.Context, fsDirectory string, relativeDirectory string, rules *run9ReadViewGlobIgnoreRules) (*run9ReadViewGlobIgnoreRules, error) {
	if rules == nil {
		return nil, nil
	}
	ignorePath := path.Join(relativeDirectory, ".gitignore")
	fsIgnorePath := path.Join(fsDirectory, ".gitignore")
	stat, errno := jfs.Lstat(ctx, fsIgnorePath)
	if errno == syscall.ENOENT || errno == syscall.ENOTDIR {
		return rules, nil
	}
	if errno != 0 {
		return nil, fmt.Errorf("inspect %s: %w", ignorePath, errno)
	}
	if stat.Attr().Typ != meta.TypeFile || stat.IsSymlink() {
		return rules, nil
	}
	file, errno := jfs.Open(ctx, fsIgnorePath, vfs.MODE_MASK_R)
	if errno != 0 {
		return nil, fmt.Errorf("open %s: %w", ignorePath, errno)
	}
	reader := &run9ReadViewFile{file: file, ctx: ctx}
	content, err := io.ReadAll(io.LimitReader(reader, run9ReadViewGlobMaximumGitIgnoreFileBytes+1))
	closeErr := reader.Close()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ignorePath, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close %s: %w", ignorePath, closeErr)
	}
	next, err := rules.add(relativeDirectory, content)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", ignorePath, err)
	}
	return next, nil
}
