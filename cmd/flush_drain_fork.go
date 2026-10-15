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
		conf, err := readVFSConfigWithTimeout(requestCtx, ctx.Args().Get(0), 3*time.Second)
		if err != nil {
			return fmt.Errorf("read mount config: %w", err)
		}
		if conf.Pid <= 0 {
			return fmt.Errorf("invalid mount pid %d in config", conf.Pid)
		}
		ticks, err := readProcStatStarttimeTicks(conf.Pid)
		if err != nil {
			return err
		}
		status, err := readProcStatusEUIDAndSignals(conf.Pid)
		if err != nil {
			return err
		}
		socket := forkFlushDrainSocketPath(conf.Pid, ticks)
		ready, err := finalizeAckDirReady(filepath.Dir(socket), status.EUID)
		if err != nil {
			return fmt.Errorf("unsafe flush-drain socket directory: %w", err)
		}
		if !ready {
			return fmt.Errorf("mount pid %d has no owner-only flush-drain socket; mount daemon upgrade required", conf.Pid)
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
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://mount/flush", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("flush-drain request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("flush-drain response: %s", response.Status)
	}
	var result forkFlushDrainResult
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode flush-drain response: %w", err)
	}
	return &result, nil
}

func forkFlushDrainSocketPath(pid int, ticks uint64) string {
	return filepath.Join("/tmp", fmt.Sprintf("run9-flush-%d-%d", pid, ticks), "control.sock")
}
