package cmd

import (
	"github.com/urfave/cli"
)

var pullCommand = cli.Command{
	Name:        "pull",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Action: func(ctx *cli.Context) error {
		return nil
	},
}
