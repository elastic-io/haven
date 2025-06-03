package cmd

import (
	"github.com/urfave/cli"
)

var deleteCommand = cli.Command{
	Name:      "delete",
	Usage:     "",
	ArgsUsage: ``,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "force, f",
			Usage: "",
		},
	},
	Action: func(context *cli.Context) error {
		return nil
	},
}
