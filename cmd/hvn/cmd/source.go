package cmd

import (
	"github.com/urfave/cli"
)

var sourceCommand = cli.Command{
	Name:        "source",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Action: func(ctx *cli.Context) error {
		return nil
	},
}
