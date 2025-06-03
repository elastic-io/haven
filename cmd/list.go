package cmd

import (
	"fmt"
	"github.com/urfave/cli"
)

const formatOptions = `table or json`

var listCommand = cli.Command{
	Name:      "list",
	Usage:     "lists instances",
	ArgsUsage: ``,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "format, f",
			Value: "table",
			Usage: `select one of: ` + formatOptions,
		},
		cli.BoolFlag{
			Name:  "quiet, q",
			Usage: "",
		},
	},
	Action: func(ctx *cli.Context) error {
		fmt.Println(ctx.Args(), ctx.NArg(), ctx.Command.Names(), ctx.String("format"))
		return nil
	},
}
