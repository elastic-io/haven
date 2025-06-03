package cmd

import (
	"github.com/urfave/cli"
)

var stateCommand = cli.Command{
	Name:        "state",
	Usage:       "",
	ArgsUsage:   ``,
	Description: ``,
	Action: func(context *cli.Context) error {
		return nil
	},
}
