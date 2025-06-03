package cmd

import (
	"fmt"
	"github.com/urfave/cli"
	"os"
)

const (
	exactArgs = iota
	minArgs
	maxArgs
)

func checkArgs(ctx *cli.Context, expected, checkType int) error {
	var err error
	cmdName := ctx.Command.Name
	switch checkType {
	case exactArgs:
		if ctx.NArg() != expected {
			err = fmt.Errorf("%s: %q requires exactly %d argument(s)", os.Args[0], cmdName, expected)
		}
	case minArgs:
		if ctx.NArg() < expected {
			err = fmt.Errorf("%s: %q requires a minimum of %d argument(s)", os.Args[0], cmdName, expected)
		}
	case maxArgs:
		if ctx.NArg() > expected {
			err = fmt.Errorf("%s: %q requires a maximum of %d argument(s)", os.Args[0], cmdName, expected)
		}
	}
	if err != nil {
		fmt.Printf("Incorrect Usage.\n\n")
		cli.ShowCommandHelp(ctx, cmdName)
		return err
	}
	return nil
}
