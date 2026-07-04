//go:build !linux
// +build !linux

package cmd

import (
	"fmt"
	"os"
	"runtime"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

type forkFlushDrainResult struct {
	Ok      bool   `json:"ok"`
	Drained bool   `json:"drained"`
	Reason  string `json:"reason,omitempty"`
}

func cmdFlushDrainFork() *cli.Command {
	return &cli.Command{
		Name:      "flush-drain",
		Action:    flushDrain,
		Category:  "SERVICE",
		Usage:     "Flush a mounted volume and wait for writeback uploads without unmounting (fork)",
		ArgsUsage: "MOUNTPOINT",
	}
}

func flushDrain(ctx *cli.Context) error {
	setup0(ctx, 0, 0)
	printJson(&forkFlushDrainResult{
		Ok:      false,
		Drained: false,
		Reason:  fmt.Sprintf("flush-drain is only supported on linux (current: %s)", runtime.GOOS),
	})
	os.Exit(1)
	return nil
}

func installForkFlushDrainHandler(v *vfs.VFS) {
	// Fork feature: linux only.
}
