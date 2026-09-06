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
