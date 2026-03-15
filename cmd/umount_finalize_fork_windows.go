//go:build windows
// +build windows

package cmd

import (
	"fmt"
	"os"
	"runtime"

	"github.com/urfave/cli/v2"
)

func cmdUmountFinalizeFork() *cli.Command {
	return &cli.Command{
		Name:      "umount-finalize",
		Action:    umountFinalizeUnsupportedOnWindows,
		Category:  "SERVICE",
		Usage:     "Unmount a volume and wait for badger terminal finalize (fork)",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This command is a fork-only variant to provide strict finalize semantics for badger (SkipWAL=true):
it returns success only after receiving a finalize ack written by the mount daemon.

Examples:
$ juicefs umount-finalize /mnt/jfs`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "finalize-timeout",
				Value: "10m",
				Usage: "timeout waiting for mount daemon to finish correctness-critical finalize and write ack",
			},
			&cli.StringFlag{
				Name:  "umount-observe-timeout",
				Value: "3s",
				Usage: "timeout observing system-level force umount (best-effort; does not affect finalize correctness)",
			},
			&cli.StringFlag{
				Name:  "exit-observe-timeout",
				Value: "3s",
				Usage: "timeout observing mount daemon exit after receiving success ack (best-effort)",
			},
		},
	}
}

func umountFinalizeUnsupportedOnWindows(ctx *cli.Context) error {
	// Fork feature: always output structured JSON on failures.
	// Avoid calling setup(ctx, 1) which prints usage errors in plain text.
	setup0(ctx, 0, 0)

	if ctx.NArg() != 1 {
		printJson(&forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("expected exactly 1 argument MOUNTPOINT, got %d", ctx.NArg()),
		})
		os.Exit(1)
	}

	printJson(&forkUmountFinalizeResult{
		Ok:     false,
		Reason: fmt.Sprintf("umount-finalize is only supported on linux (current: %s)", runtime.GOOS),
	})
	os.Exit(1)
	return nil
}
