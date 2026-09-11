package chunk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestReadSurvivesAnotherReadersCancellation(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = "memory"
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	data := []byte("immutable block")
	key := FormatObjectBlockKey(100, 0, uint64(len(data)), false)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))

	// A speculative read owns the shared download. Both real readers join it
	// before it is cancelled; only one of those readers is itself cancelled.
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	started, leaderDone := make(chan struct{}), make(chan error, 1)
	go func() {
		page, err := store.group.Execute(key, func() (*Page, error) {
			close(started)
			<-leaderCtx.Done()
			return NewPage(make([]byte, len(data))), leaderCtx.Err()
		})
		page.Release()
		leaderDone <- err
	}()
	<-started
	activePage := NewPage(make([]byte, len(data)))
	defer activePage.Release()
	cancelledPage := NewPage(make([]byte, len(data)))
	defer cancelledPage.Release()
	cancelledCtx, cancelReader := context.WithCancel(context.Background())
	defer cancelReader()
	activeDone, cancelledDone := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := store.NewReader(100, len(data)).ReadAt(context.Background(), activePage, 0)
		activeDone <- err
	}()
	go func() {
		_, err := store.NewReader(100, len(data)).ReadAt(cancelledCtx, cancelledPage, 0)
		cancelledDone <- err
	}()
	require.Eventually(t, func() bool {
		store.group.Lock()
		defer store.group.Unlock()
		return store.group.rs[key].dups == 2
	}, time.Second, time.Millisecond)
	cancelReader()
	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.ErrorIs(t, <-cancelledDone, context.Canceled)
	require.NoError(t, <-activeDone)
	require.Equal(t, data, activePage.Data)
	require.EqualValues(t, 1, storage.gets.Load(), "the valid reader starts a fresh download")
}
