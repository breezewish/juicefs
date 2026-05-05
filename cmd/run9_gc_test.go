package cmd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9ParseObjectBlockKeyDefaultLayout(t *testing.T) {
	layout := run9ObjectLayout{BlockSizeBytes: 4096}

	block, ok := parseRun9ObjectBlockKey("chunks/1/1234/1234567_2_4096", layout)
	require.True(t, ok)
	require.Equal(t, uint64(1234567), block.SliceID)
	require.Equal(t, uint64(2), block.BlockIndex)
	require.Equal(t, uint64(4096), block.BlockSize)

	_, ok = parseRun9ObjectBlockKey("chunks/0/1234/1234567_2_4096", layout)
	require.False(t, ok)

	_, ok = parseRun9ObjectBlockKey("chunks/1/1234/1234567_2_4097", layout)
	require.False(t, ok)
}

func TestRun9ParseObjectBlockKeyHashPrefixLayout(t *testing.T) {
	layout := run9ObjectLayout{BlockSizeBytes: 4096, HashPrefix: true}

	block, ok := parseRun9ObjectBlockKey("chunks/87/1/1234567_0_128", layout)
	require.True(t, ok)
	require.Equal(t, uint64(1234567), block.SliceID)
	require.Equal(t, uint64(0), block.BlockIndex)
	require.Equal(t, uint64(128), block.BlockSize)

	_, ok = parseRun9ObjectBlockKey("chunks/87/1234/1234567_0_128", layout)
	require.False(t, ok)

	_, ok = parseRun9ObjectBlockKey("chunks/87/1/1234567_0_0", layout)
	require.False(t, ok)
}

func TestRun9ObjectBlockProtectedByLiveSlice(t *testing.T) {
	layout := run9ObjectLayout{BlockSizeBytes: 4096}
	live := map[uint64]uint32{10: 5000}

	require.True(t, run9ObjectBlockProtected(
		run9ParsedObjectBlock{SliceID: 10, BlockIndex: 0, BlockSize: 4096},
		layout.BlockSizeBytes,
		live,
		nil,
	))
	require.True(t, run9ObjectBlockProtected(
		run9ParsedObjectBlock{SliceID: 10, BlockIndex: 1, BlockSize: 904},
		layout.BlockSizeBytes,
		live,
		nil,
	))
	require.False(t, run9ObjectBlockProtected(
		run9ParsedObjectBlock{SliceID: 10, BlockIndex: 1, BlockSize: 4096},
		layout.BlockSizeBytes,
		live,
		nil,
	))
	require.False(t, run9ObjectBlockProtected(
		run9ParsedObjectBlock{SliceID: 10, BlockIndex: 2, BlockSize: 1},
		layout.BlockSizeBytes,
		live,
		nil,
	))
}

func TestRun9EpochRangesAvoidOverflow(t *testing.T) {
	active := run9SliceEpochRange(math.MaxUint32)
	require.Equal(t, uint64(math.MaxUint32)<<32, active.Start)
	require.Equal(t, uint64(math.MaxUint64), active.EndInclusive)

	future, ok := run9FutureEpochRange(math.MaxUint32 - 1)
	require.True(t, ok)
	require.Equal(t, uint64(math.MaxUint32)<<32, future.Start)
	require.Equal(t, uint64(math.MaxUint64), future.EndInclusive)

	_, ok = run9FutureEpochRange(math.MaxUint32)
	require.False(t, ok)
}
