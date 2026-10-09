package object

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cancelListStore struct {
	ObjectStorage
	started chan struct{}
	once    sync.Once
}

func (s *cancelListStore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	if prefix == "" {
		var entries []Object
		// More directories than listing workers: cancellation must not leave the
		// walker waiting for a worker that exited before its second assignment.
		for i := 0; i < 20; i++ {
			entries = append(entries, &obj{key: fmt.Sprintf("%02d/", i), isDir: true})
		}
		return entries, false, "", nil
	}
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	// A backend may complete successfully just as cancellation arrives.
	return nil, false, "", nil
}

func TestListAllWithDelimiterCancellationClosesChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelListStore{started: make(chan struct{})}
	listed, err := ListAllWithDelimiter(ctx, store, "", "", "", false)
	require.NoError(t, err)
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("listing did not start")
	}
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-listed:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("listing channel remained open after cancellation")
		}
	}
}

type failingPageStore struct{ ObjectStorage }

func (*failingPageStore) String() string { return "failing" }
func (*failingPageStore) ListAll(context.Context, string, string, bool) (<-chan Object, error) {
	return nil, notSupported
}

func (s *failingPageStore) List(_ context.Context, _, marker, _, _ string, _ int64, _ bool) ([]Object, bool, string, error) {
	if marker != "" {
		return nil, false, "", fmt.Errorf("permanent list failure")
	}
	return []Object{&obj{key: "a"}}, true, "next", nil
}
func TestRun9ShardedListPreservesPageErrors(t *testing.T) {
	store := &sharded{stores: []ObjectStorage{&failingPageStore{}}}
	listed, err := store.ListAll(t.Context(), "", "", false)
	require.NoError(t, err)
	require.Equal(t, "a", (<-listed).Key())
	select {
	case item, ok := <-listed:
		require.True(t, ok, "scan errors must not become EOF")
		require.Nil(t, item)
	case <-time.After(time.Second):
		t.Fatal("permanent error was retried indefinitely")
	}
	_, ok := <-listed
	require.False(t, ok)
}

type waitingListStore struct{ ObjectStorage }

func (s *waitingListStore) ListAll(ctx context.Context, _, _ string, _ bool) (<-chan Object, error) {
	ch := make(chan Object, 1)
	ch <- &obj{key: "a"}
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}
func TestRun9ShardedListCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := &sharded{stores: []ObjectStorage{&waitingListStore{}}}
	listed, err := store.ListAll(ctx, "", "", false)
	require.NoError(t, err)
	require.Equal(t, "a", (<-listed).Key())
	cancel()
	select {
	case _, ok := <-listed:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("sharded list ignored cancellation")
	}
}
