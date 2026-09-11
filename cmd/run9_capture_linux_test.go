//go:build linux && !nobadger

package cmd

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun9CaptureReportsErrorBeforeClosing(t *testing.T) {
	previous := forkFlushDrainInProgress.Swap(true)
	defer forkFlushDrainInProgress.Store(previous)
	server, client := net.Pipe()
	defer client.Close()
	require.NoError(t, client.SetDeadline(time.Now().Add(time.Second)))
	finished := make(chan error, 1)
	go func() {
		finished <- serveRun9Capture(context.Background(), server, json.NewEncoder(server), nil, nil)
	}()
	require.NoError(t, json.NewEncoder(client).Encode(run9CaptureRequest{
		Directory: "/unused", DeadlineUnixMS: time.Now().Add(time.Second).UnixMilli(),
	}))
	var response run9CaptureResponse
	require.NoError(t, json.NewDecoder(client).Decode(&response))
	require.Equal(t, "error", response.Phase)
	require.Equal(t, "mount capture or flush is busy", response.Error)
	require.ErrorContains(t, <-finished, "mount capture or flush is busy")
}
