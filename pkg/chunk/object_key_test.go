package chunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectBlockKeyDefaultLayout(t *testing.T) {
	key := FormatObjectBlockKey(1234567, 2, 4096, false)
	require.Equal(t, "chunks/1/1234/1234567_2_4096", key)

	block, ok := ParseObjectBlockKey(key, false)
	require.True(t, ok)
	require.Equal(t, ObjectBlockKey{SliceID: 1234567, BlockIndex: 2, BlockSize: 4096}, block)

	_, ok = ParseObjectBlockKey("chunks/0/1234/1234567_2_4096", false)
	require.False(t, ok)
}

func TestObjectBlockKeyHashPrefixLayout(t *testing.T) {
	key := FormatObjectBlockKey(1234567, 0, 128, true)
	require.Equal(t, "chunks/87/1/1234567_0_128", key)

	block, ok := ParseObjectBlockKey(key, true)
	require.True(t, ok)
	require.Equal(t, ObjectBlockKey{SliceID: 1234567, BlockIndex: 0, BlockSize: 128}, block)

	_, ok = ParseObjectBlockKey("chunks/87/1234/1234567_0_128", true)
	require.False(t, ok)
}
