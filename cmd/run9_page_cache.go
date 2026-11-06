package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

// forkControlMaxBody bounds a control request or response body; a read trace
// or page-cache list of a large workload is the biggest.
const forkControlMaxBody = 64 << 20

type forkReadTraceResult struct {
	Ok    bool                `json:"ok"`
	Files []vfs.PageCacheFile `json:"files"`
}

type forkPageCacheWarmResult struct {
	Ok bool `json:"ok"`
	vfs.PageCacheWarmResult
}

type forkControlFailure struct {
	Ok     bool   `json:"ok"`
	Reason string `json:"reason"`
	// Unsupported reports a mount daemon older than the request's route.
	Unsupported bool `json:"unsupported,omitempty"`
}

// errForkControlUnsupported marks a control request whose route the mount
// daemon does not serve.
var errForkControlUnsupported = errors.New("not served by the mount daemon")

func cmdRun9ReadTrace() *cli.Command {
	return &cli.Command{
		Name:      "read-trace",
		Action:    run9ReadTrace,
		Category:  "SERVICE",
		Usage:     "Print the file ranges a mount started with --trace-reads has read (fork)",
		ArgsUsage: "MOUNTPOINT",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "timeout",
				Value: "1m",
				Usage: "total timeout",
			},
			&cli.IntFlag{
				Name:     "max-files",
				Required: true,
				Usage:    "print at most this many files, the earliest read first",
			},
			&cli.Uint64Flag{
				Name:     "max-bytes",
				Required: true,
				Usage:    "leave out files whose ranges would take the total past this many bytes",
			},
			&cli.IntFlag{
				Name:     "max-list-bytes",
				Required: true,
				Usage:    "end the list before its JSON encoding would exceed this many bytes",
			},
		},
	}
}

func run9ReadTrace(c *cli.Context) error {
	return runForkControlCommand(c, func(timeout time.Duration) (any, error) {
		ctx, cancel := context.WithTimeout(c.Context, timeout)
		defer cancel()
		query := url.Values{}
		query.Set("max_files", strconv.Itoa(c.Int("max-files")))
		query.Set("max_bytes", strconv.FormatUint(c.Uint64("max-bytes"), 10))
		query.Set("max_list_bytes", strconv.Itoa(c.Int("max-list-bytes")))
		var result forkReadTraceResult
		if err := requestForkControlForMount(ctx, c.Args().Get(0), http.MethodGet, "/read-trace?"+query.Encode(), nil, &result); err != nil {
			return nil, err
		}
		return result, nil
	})
}

func cmdRun9WarmPageCache() *cli.Command {
	return &cli.Command{
		Name:     "warm-page-cache",
		Action:   run9WarmPageCache,
		Category: "SERVICE",
		Usage:    "Read the file ranges listed on stdin into a mount's page cache, from local caches only (fork)",
		Description: `Stdin holds a JSON array of {"inode", "path", "ranges"} as printed by read-trace.
The mount daemon reads them through its mountpoint and returns when done, out of
budget, or past the timeout.`,
		ArgsUsage: "MOUNTPOINT",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "timeout",
				Required: true,
				Usage:    "stop warming after this long",
			},
			&cli.Uint64Flag{
				Name:     "max-bytes",
				Required: true,
				Usage:    "stop warming after reading this many bytes",
			},
			&cli.IntFlag{
				Name:     "concurrency",
				Required: true,
				Usage:    "number of files read at once",
			},
		},
	}
}

func run9WarmPageCache(c *cli.Context) error {
	return runForkControlCommand(c, func(timeout time.Duration) (any, error) {
		list, err := io.ReadAll(io.LimitReader(os.Stdin, forkControlMaxBody))
		if err != nil {
			return nil, fmt.Errorf("read page-cache list: %w", err)
		}
		deadline := time.Now().Add(timeout)
		query := url.Values{}
		query.Set("deadline_unix_ms", strconv.FormatInt(deadline.UnixMilli(), 10))
		query.Set("max_bytes", strconv.FormatUint(c.Uint64("max-bytes"), 10))
		query.Set("concurrency", strconv.Itoa(c.Int("concurrency")))
		// The daemon stops at the deadline; give its response a moment longer.
		ctx, cancel := context.WithDeadline(c.Context, deadline.Add(5*time.Second))
		defer cancel()
		var result forkPageCacheWarmResult
		if err := requestForkControlForMount(ctx, c.Args().Get(0), http.MethodPost, "/warm-page-cache?"+query.Encode(), list, &result); err != nil {
			return nil, err
		}
		return result, nil
	})
}

// runForkControlCommand runs one MOUNTPOINT control command and prints its
// result, or {"ok": false, "reason", "unsupported"} with exit status 1.
func runForkControlCommand(c *cli.Context, run func(timeout time.Duration) (any, error)) error {
	setup0(c, 1, 1)
	timeout, err := parsePositiveDuration(c.String("timeout"))
	var result any
	if err != nil {
		err = fmt.Errorf("invalid --timeout: %w", err)
	} else {
		result, err = run(timeout)
	}
	if err != nil {
		printJson(forkControlFailure{Reason: err.Error(), Unsupported: errors.Is(err, errForkControlUnsupported)})
		os.Exit(1)
	}
	printJson(result)
	return nil
}
