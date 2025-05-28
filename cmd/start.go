package cmd

import (
	"github.com/urfave/cli"
)

var startCommand = cli.Command{
	Name:        "start",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Action: func(context *cli.Context) error {
		return nil
	},
}
