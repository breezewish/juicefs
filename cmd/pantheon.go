/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"
)

func cmdPantheon() *cli.Command {
	return &cli.Command{
		Name:        "pantheon",
		Action:      pantheonHelp,
		Category:    "TOOL",
		Usage:       "Controlling PantheonFS related features",
		Description: ``,
		Subcommands: []*cli.Command{
			cmdPantheonFormat(),
			cmdPantheonMount(),
			cmdPantheonUmount(),
			cmdPantheonCheckpoint(),
		},
	}
}

func pantheonHelp(c *cli.Context) error {
	cli.ShowCommandHelp(c, "pantheon")
	return nil
}

func cmdPantheonFormat() *cli.Command {
	return &cli.Command{
		Name:      "format",
		Action:    pantheonFormat,
		Usage:     "Format a volume in PantheonFS mode",
		ArgsUsage: "META-DIR NAME",
		Description: `
Examples:
# Format a simple volume with local metadata
$ juicefs pantheon format /var/lib/juicefs/myfs myfs

# Format with custom storage options
$ juicefs pantheon format /var/lib/juicefs/myfs myfs --storage s3 --bucket https://mybucket.s3.amazonaws.com`,
		Flags: cmdFormat().Flags,
	}
}

func cmdPantheonMount() *cli.Command {
	return &cli.Command{
		Name:      "mount",
		Action:    pantheonMount,
		Usage:     "Mount a volume in PantheonFS mode",
		ArgsUsage: "META-DIR MOUNTPOINT",
		Description: `
Examples:
# Mount a pantheon volume
$ juicefs pantheon mount /var/lib/juicefs/myfs /mnt/jfs

# Mount in background
$ juicefs pantheon mount /var/lib/juicefs/myfs /mnt/jfs -d`,
		Flags: cmdMount().Flags,
	}
}

func cmdPantheonUmount() *cli.Command {
	return &cli.Command{
		Name:      "umount",
		Action:    pantheonUmount,
		Usage:     "Unmount a volume",
		ArgsUsage: "MOUNTPOINT",
		Description: `
Examples:
# Unmount a volume
$ juicefs pantheon umount /mnt/jfs

# Force unmount
$ juicefs pantheon umount /mnt/jfs -f`,
		Flags: cmdUmount().Flags,
	}
}

func cmdPantheonCheckpoint() *cli.Command {
	return &cli.Command{
		Name:      "checkpoint",
		Action:    pantheonCheckpoint,
		Usage:     "Create a checkpoint of the entire filesystem by copying metadata to a new directory",
		ArgsUsage: "OLD-META-DIR NEW-META-DIR",
		Description: `
The old metadata should not be mounted when creating a checkpoint.

Examples:
$ juicefs pantheon checkpoint /var/lib/juicefs/myfs /var/lib/juicefs/myfs-branch2`,
	}
}

// Helper function to validate absolute path and existence
func validateAbsolutePath(path string, shouldExist bool) {
	if !filepath.IsAbs(path) {
		logger.Fatalf("path must be absolute: %s", path)
	}

	_, err := os.Stat(path)
	exists := err == nil

	if shouldExist && !exists {
		logger.Fatalf("path does not exist: %s", path)
	}

	if !shouldExist && exists {
		logger.Fatalf("path already exists: %s", path)
	}
}

// Helper function to build flag arguments from CLI context
func buildFlagArgs(c *cli.Context) []string {
	var args []string

	// Iterate through command flags to avoid processing the same flag multiple times
	for _, flag := range c.Command.Flags {
		// Use the primary name (first name in the list)
		flagName := flag.Names()[0]

		if c.IsSet(flagName) {
			switch flag.(type) {
			case *cli.BoolFlag:
				if c.Bool(flagName) {
					args = append(args, "--"+flagName)
				}
			case *cli.StringFlag:
				args = append(args, "--"+flagName+"="+c.String(flagName))
			case *cli.IntFlag:
				args = append(args, fmt.Sprintf("--%s=%d", flagName, c.Int(flagName)))
			case *cli.Int64Flag:
				args = append(args, fmt.Sprintf("--%s=%d", flagName, c.Int64(flagName)))
			case *cli.Float64Flag:
				args = append(args, fmt.Sprintf("--%s=%f", flagName, c.Float64(flagName)))
			case *cli.StringSliceFlag:
				for _, value := range c.StringSlice(flagName) {
					args = append(args, "--"+flagName+"="+value)
				}
			default:
				logger.Fatalf("unsupported flag type for flag %s: %T", flagName, flag)
			}
		}
	}

	return args
}

// Helper function to execute a juicefs command with signal forwarding
func executeJuicefsCommand(args []string) error {
	executable, err := os.Executable()
	if err != nil {
		logger.Fatalf("failed to get current executable: %v", err)
	}

	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	// Forward signals to child process
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range c {
			if cmd.Process != nil {
				cmd.Process.Signal(sig)
			}
		}
	}()

	err = cmd.Run()
	signal.Stop(c)

	if exitError, ok := err.(*exec.ExitError); ok {
		os.Exit(exitError.ExitCode())
	}

	return err
}

func metaDirWithoutQuery(metaDir string) string {
	// Strip all things after '?' in metaDir
	for i, ch := range metaDir {
		if ch == '?' {
			return metaDir[:i]
		}
	}
	return metaDir
}

func pantheonFormat(c *cli.Context) error {
	setup(c, 2)

	metaDir := c.Args().Get(0)
	name := c.Args().Get(1)

	// Validate meta-dir is absolute and doesn't exist
	validateAbsolutePath(metaDirWithoutQuery(metaDir), false)

	// Build arguments for juicefs format command
	args := []string{"format", fmt.Sprintf("badger://%s", metaDir), name, "--trash-days=999"}
	args = append(args, buildFlagArgs(c)...)

	return executeJuicefsCommand(args)
}

func pantheonMount(c *cli.Context) error {
	setup(c, 2)

	metaDir := c.Args().Get(0)
	mountPoint := c.Args().Get(1)

	// Validate meta-dir is absolute and exists
	validateAbsolutePath(metaDirWithoutQuery(metaDir), true)

	// Build arguments for juicefs mount command
	args := []string{"mount", fmt.Sprintf("badger://%s", metaDir), mountPoint}
	args = append(args, buildFlagArgs(c)...)

	return executeJuicefsCommand(args)
}

func pantheonUmount(c *cli.Context) error {
	setup(c, 1)

	mountPoint := c.Args().Get(0)

	// Build arguments for juicefs umount command
	args := []string{"umount", mountPoint}
	args = append(args, buildFlagArgs(c)...)

	return executeJuicefsCommand(args)
}

func pantheonCheckpoint(c *cli.Context) error {
	setup(c, 2)

	oldMetaDir := c.Args().Get(0)
	newMetaDir := c.Args().Get(1)

	// Validate paths
	validateAbsolutePath(oldMetaDir, true)
	validateAbsolutePath(newMetaDir, false)

	// Build arguments for juicefs clone command
	args := []string{"clone", oldMetaDir, newMetaDir}

	return executeJuicefsCommand(args)
}
