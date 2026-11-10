package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

// A candidate is owned by run9rt. Running its Badger lifecycle in this short-
// lived process isolates candidate failures from the source mount process.
// Stdout carries the prepared copy's usage as one JSON object.
func cmdRun9PrepareFork() *cli.Command {
	return &cli.Command{
		Name: "prepare-fork", Hidden: true,
		Usage:     "run9 internal: remove inherited session state from private fork metadata and report its usage",
		ArgsUsage: "METADATA-DIRECTORY",
		Action: func(c *cli.Context) error {
			setup0(c, 1, 1)
			directory := c.Args().First()
			if !filepath.IsAbs(directory) {
				return fmt.Errorf("fork metadata directory must be absolute")
			}
			prepared, err := meta.Run9PrepareFork(c.Context, directory)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(prepared)
		},
	}
}
