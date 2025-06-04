package cmd

import (
	"github.com/urfave/cli"
)

var sinkCommand = cli.Command{
	Name:        "sink",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Action: func(ctx *cli.Context) error {
		return nil
	},
}
