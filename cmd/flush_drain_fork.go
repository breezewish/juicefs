//go:build linux

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"
)

type forkFlushDrainResult struct {
	Ok      bool               `json:"ok"`
	Drained bool               `json:"drained"`
	Reason  string             `json:"reason,omitempty"`
	Ack     *forkFinalizeAckV1 `json:"ack,omitempty"`
}

type forkFlushDrainRequest struct {
	DeadlineUnixMS int64 `json:"deadline_unix_ms"`
}

func cmdFlushDrainFork() *cli.Command {
	return &cli.Command{
		Name:      "flush-drain",
		Action:    flushDrain,
		Category:  "SERVICE",
		Usage:     "Flush a mounted volume and wait for writeback uploads without unmounting (fork)",
		ArgsUsage: "MOUNTPOINT",
		Flags: []cli.Flag{&cli.StringFlag{
			Name:  "timeout",
			Value: "10m",
			Usage: "total timeout including queueing, VFS flush and writeback uploads",
		}},
	}
}

func flushDrain(ctx *cli.Context) error {
	setup0(ctx, 0, 0)
	result, exitCode := flushDrainRun(ctx)
	printJson(result)
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}

func flushDrainRun(ctx *cli.Context) (*forkFlushDrainResult, int) {
	result := &forkFlushDrainResult{}
	err := func() error {
		if ctx.NArg() != 1 {
			return fmt.Errorf("expected exactly 1 argument MOUNTPOINT, got %d", ctx.NArg())
		}
		timeout, err := parsePositiveDuration(ctx.String("timeout"))
		if err != nil {
			return fmt.Errorf("invalid --timeout: %w", err)
		}
		requestCtx, cancel := context.WithTimeout(ctx.Context, timeout)
		defer cancel()
		socket, err := forkControlSocketForMount(requestCtx, ctx.Args().Get(0))
		if err != nil {
			return err
		}
		result, err = requestForkFlushDrain(requestCtx, socket)
		return err
	}()
	if err != nil {
		return &forkFlushDrainResult{Reason: err.Error()}, 1
	}
	if !result.Ok || !result.Drained {
		return result, 1
	}
	return result, 0
}

// forkControlSocketForMount finds the owner-only control socket of the daemon
// serving mountpoint.
func forkControlSocketForMount(ctx context.Context, mountpoint string) (string, error) {
	conf, err := readVFSConfigWithTimeout(ctx, mountpoint, 3*time.Second)
	if err != nil {
		return "", fmt.Errorf("read mount config: %w", err)
	}
	if conf.Pid <= 0 {
		return "", fmt.Errorf("invalid mount pid %d in config", conf.Pid)
	}
	ticks, err := readProcStatStarttimeTicks(conf.Pid)
	if err != nil {
		return "", err
	}
	status, err := readProcStatusEUIDAndSignals(conf.Pid)
	if err != nil {
		return "", err
	}
	socket := forkControlSocketPath(conf.Pid, ticks)
	ready, err := finalizeAckDirReady(filepath.Dir(socket), status.EUID)
	if err != nil {
		return "", fmt.Errorf("unsafe control socket directory: %w", err)
	}
	if !ready {
		return "", fmt.Errorf("mount pid %d has no owner-only control socket; mount daemon upgrade required", conf.Pid)
	}
	return socket, nil
}

// requestForkControlForMount sends one request to the control socket of the
// daemon serving mountpoint and decodes its JSON response into result.
func requestForkControlForMount(ctx context.Context, mountpoint, method, path string, body []byte, result any) error {
	socket, err := forkControlSocketForMount(ctx, mountpoint)
	if err != nil {
		return err
	}
	return requestForkControl(ctx, socket, method, path, body, result)
}

// Each connection owns its response. Unlike SIGUSR1 and a shared ack file,
// concurrent or cancelled callers cannot delete or consume each other's proof.
func requestForkFlushDrain(ctx context.Context, socket string) (*forkFlushDrainResult, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("flush-drain requires a deadline")
	}
	body, err := json.Marshal(forkFlushDrainRequest{DeadlineUnixMS: deadline.UnixMilli()})
	if err != nil {
		return nil, err
	}
	var result forkFlushDrainResult
	if err := requestForkControl(ctx, socket, http.MethodPost, "/flush", body, &result); err != nil {
		return nil, fmt.Errorf("flush-drain %w", err)
	}
	return &result, nil
}

// requestForkControl sends one request to a daemon control socket. A daemon
// older than the requested route answers 404, reported as
// errForkControlUnsupported.
func requestForkControl(ctx context.Context, socket, method, path string, body []byte, result any) error {
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, method, "http://mount"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		err := fmt.Errorf("%s: %s", response.Status, bytes.TrimSpace(message))
		if response.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%s %w: %w", path, errForkControlUnsupported, err)
		}
		return fmt.Errorf("response: %w", err)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, forkControlMaxBody)).Decode(result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func forkControlSocketPath(pid int, ticks uint64) string {
	return filepath.Join("/tmp", fmt.Sprintf("run9-flush-%d-%d", pid, ticks), "control.sock")
}
