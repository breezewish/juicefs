package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestRun9StandaloneReadViewRejectsOtherSnapCursor(t *testing.T) {
	// Exercise the real command's identity setup, not manually scoped handlers.
	start := func() *http.Client {
		uri := "badger://" + t.TempDir()
		require.NoError(t, Main([]string{"juicefs", "format", "--storage", "file", "--bucket", t.TempDir(), "--trash-days", "0", "--run9-init-empty-epoch", "1", uri, "cursor-test"}))
		m := meta.NewClient(uri, meta.DefaultConf())
		_, err := m.Load(true)
		require.NoError(t, err)
		var root, child meta.Ino
		var attr meta.Attr
		require.Zero(t, m.Lookup(meta.Background(), meta.RootInode, "rootfs", &root, &attr, false))
		require.Zero(t, m.Mkdir(meta.Background(), root, "a", 0755, 0, 0, &child, &attr))
		require.Zero(t, m.Mkdir(meta.Background(), root, "b", 0755, 0, 0, &child, &attr))
		require.NoError(t, m.Shutdown())
		socket := filepath.Join(t.TempDir(), "reader.sock")
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		app := &cli.App{Commands: []*cli.Command{cmdRun9ServeReadView()}}
		cacheDir := t.TempDir()
		go func() {
			done <- app.RunContext(ctx, []string{"juicefs", "serve-read-view", "--listen", socket, "--cache-dir", cacheDir, "--generation", "1", uri + "?readonly=1"})
		}()
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		client := &http.Client{Transport: transport, Timeout: time.Second}
		t.Cleanup(func() {
			transport.CloseIdleConnections()
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("read view did not shut down")
			}
		})
		require.Eventually(t, func() bool {
			resp, err := client.Get("http://reader/?list=1")
			if err != nil {
				return false
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 10*time.Second, 10*time.Millisecond)
		return client
	}
	first, second := start(), start()
	response, err := first.Get("http://reader/?list=1&limit=1")
	require.NoError(t, err)
	var page run9ReadViewListResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&page))
	response.Body.Close()
	require.NotEmpty(t, page.Cursor)
	response, err = first.Get("http://reader/?list=1&cursor=" + page.Cursor)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	response, err = second.Get("http://reader/?list=1&cursor=" + page.Cursor)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode, string(body))
}
