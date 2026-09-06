package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdCleanCachePrune() *cli.Command {
	return &cli.Command{
		Name: "clean-cache-prune", Hidden: true,
		Usage: "prune a host's remote-backed clean cache without touching staging",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "cache-dir", Required: true},
			&cli.StringFlag{Name: "cache-size", Value: "100G"},
			&cli.Uint64Flag{Name: "cache-items", Value: 1000000},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("clean-cache-prune does not accept positional arguments")
			}
			result, err := chunk.PruneCleanCache(c.Context, c.String("cache-dir"), utils.ParseBytes(c, "cache-size", 'B'), c.Uint64("cache-items"))
			if err != nil {
				return err
			}
			return json.NewEncoder(c.App.Writer).Encode(result)
		},
	}
}
