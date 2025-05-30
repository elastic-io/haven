package cmd

import (
	"fmt"
	"github.com/urfave/cli"
	"golang.org/x/sys/unix"
	"strconv"
	"strings"
)

var killCommand = cli.Command{
	Name:      "kill",
	Usage:     "",
	ArgsUsage: ``,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "all, a",
			Usage: "(obsoleted, do not use)",
		},
	},
	Action: func(context *cli.Context) error {
		return nil
	},
}

func ParseSignal(rawSignal string) (unix.Signal, error) {
	s, err := strconv.Atoi(rawSignal)
	if err == nil {
		return unix.Signal(s), nil
	}
	sig := strings.ToUpper(rawSignal)
	if !strings.HasPrefix(sig, "SIG") {
		sig = "SIG" + sig
	}
	signal := unix.SignalNum(sig)
	if signal == 0 {
		return -1, fmt.Errorf("unknown signal %q", rawSignal)
	}
	return signal, nil
}
