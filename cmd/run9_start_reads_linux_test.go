//go:build linux

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
)

func TestStartReadsListRoundTripsAndRejectsInvalidRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "start-reads.json")
	list, err := readStartReads(path)
	require.NoError(t, err)
	require.Nil(t, list) // no previous start yet

	written := []vfs.StartRead{{Inode: 7, Ranges: [][2]uint64{{0, 4096}, {8192, 4096}}}}
	require.NoError(t, writeStartReads(path, written))
	list, err = readStartReads(path)
	require.NoError(t, err)
	require.Equal(t, written, list)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1) // no temporary file is left behind

	require.NoError(t, os.WriteFile(path, []byte(`[{"inode":7,"ranges":[[8192,4096],[0,4096]]}]`), 0o600))
	_, err = readStartReads(path)
	require.ErrorContains(t, err, "invalid range")
}
