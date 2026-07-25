package cmd

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathWithinCloneMountpointHintAcceptsRootAndDescendant(t *testing.T) {
	hint := filepath.Join(string(filepath.Separator), "var", "lib", "run9", "shared_meta")

	require.True(t, pathWithinCloneMountpointHint(hint, hint))
	require.True(t, pathWithinCloneMountpointHint(hint, filepath.Join(hint, "snaps", "01", "02", "snap", "meta")))
}

func TestPathWithinCloneMountpointHintRejectsParentAndPrefixSibling(t *testing.T) {
	hint := filepath.Join(string(filepath.Separator), "var", "lib", "run9", "shared_meta")

	require.False(t, pathWithinCloneMountpointHint(hint, filepath.Dir(hint)))
	require.False(t, pathWithinCloneMountpointHint(hint, hint+"-other"))
}
