package cmd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestOwnedBytesStartupBudgetDoesNotFailTheWritableMount(t *testing.T) {
	m := meta.NewClient("badger://"+filepath.Join(t.TempDir(), "meta"), meta.DefaultConf())
	require.NoError(t, m.Init(&meta.Format{Name: "startup", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4}, true))
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	old := run9OwnedBytesInitBudget
	run9OwnedBytesInitBudget = -time.Second
	t.Cleanup(func() { run9OwnedBytesInitBudget = old })

	// Expiry is deterministic, including on a machine with an empty page cache.
	// It must not turn a missing optional count into mount failure or valid zero.
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32))
	counter := m.(interface{ Run9SettleOwnedBytes() *meta.Run9OwnedBytes })
	require.Nil(t, counter.Run9SettleOwnedBytes())
	var inode meta.Ino
	var attr meta.Attr
	require.Zero(t, m.Create(meta.Background(), meta.RootInode, "still-writable", 0644, 0, 0, &inode, &attr))
	require.Zero(t, m.Close(meta.Background(), inode))
	require.Nil(t, counter.Run9SettleOwnedBytes())
}

func TestOwnedBytesStartupWithinBudgetKeepsTheLiveCounter(t *testing.T) {
	m := meta.NewClient("badger://"+filepath.Join(t.TempDir(), "meta"), meta.DefaultConf())
	require.NoError(t, m.Init(&meta.Format{Name: "startup", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4}, true))
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32))
	counter := m.(interface{ Run9SettleOwnedBytes() *meta.Run9OwnedBytes })
	require.Equal(t, &meta.Run9OwnedBytes{}, counter.Run9SettleOwnedBytes())
}
