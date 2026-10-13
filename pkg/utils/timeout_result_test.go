package utils

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTimeoutResultDiscardsLateCompletion(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			<-started
			if canceled {
				cancel()
			}
		}()
		timeout := time.Millisecond
		if canceled {
			timeout = time.Second
		}
		value, err := WithTimeoutResult(ctx, func(context.Context) (string, error) {
			close(started)
			<-release
			defer close(finished)
			return "late value", errors.New("late error")
		}, timeout)
		require.Empty(t, value)
		if canceled {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.ErrorIs(t, err, ErrFuncTimeout)
		}
		close(release)
		for i := 0; i < 1000; i++ {
			require.NotContains(t, err.Error(), "late error")
		}
		<-finished
		cancel()
	}
	value, err := WithTimeoutResult(context.Background(), func(context.Context) (int, error) { return 42, nil }, time.Second)
	require.NoError(t, err)
	require.Equal(t, 42, value)
}
