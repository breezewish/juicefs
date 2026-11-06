//go:build linux

package cmd

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
)

func TestReadTraceReportsAMissingRouteAsUnsupported(t *testing.T) {
	serve := func(mux *http.ServeMux) string {
		socket := filepath.Join(t.TempDir(), "control.sock")
		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)
		server := &http.Server{Handler: mux}
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() { _ = server.Close() })
		return socket
	}
	path := "/read-trace?max_files=1&max_bytes=4096&max_list_bytes=1024"
	current := http.NewServeMux()
	routeForkPageCache(current, &vfs.VFS{})
	socket := serve(current)

	// A daemon older than the route.
	err := requestForkControl(context.Background(), serve(http.NewServeMux()), http.MethodGet, path, nil, &forkReadTraceResult{})
	require.ErrorIs(t, err, errForkControlUnsupported)

	// A current daemon whose mount does not trace is a failure, not old.
	err = requestForkControl(context.Background(), socket, http.MethodGet, path, nil, &forkReadTraceResult{})
	require.ErrorContains(t, err, "this mount does not trace reads")
	require.False(t, errors.Is(err, errForkControlUnsupported))

	forkMountLifecycle.Lock()
	err = requestForkControl(context.Background(), socket, http.MethodGet, path, nil, &forkReadTraceResult{})
	forkMountLifecycle.Unlock()
	require.ErrorContains(t, err, "mount is finalizing")
}

func TestStopForkPageCacheWarmsCancelsAndWaitsForRunningWarms(t *testing.T) {
	originalCtx, originalCancel := forkPageCacheWarmCtx, cancelForkPageCacheWarms
	forkPageCacheWarmCtx, cancelForkPageCacheWarms = context.WithCancel(context.Background())
	defer func() { forkPageCacheWarmCtx, cancelForkPageCacheWarms = originalCtx, originalCancel }()
	warmCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, err := beginForkPageCacheWarm(cancel)
	require.NoError(t, err)
	var closed atomic.Bool
	go func() {
		<-warmCtx.Done()
		time.Sleep(50 * time.Millisecond) // the warm closes its files
		closed.Store(true)
		done()
	}()

	stopForkPageCacheWarms()
	require.True(t, closed.Load())
}
