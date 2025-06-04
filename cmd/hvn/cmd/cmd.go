package cmd

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	_ "github.com/elastic-io/haven/internal"
	"github.com/elastic-io/haven/internal/log"
	"github.com/urfave/cli"
)

func Execute(name, usage, version, commit string) {
	app := cli.NewApp()
	app.Name = name
	app.Usage = usage

	v := []string{version}

	if commit != "" {
		v = append(v, "commit: "+commit)
	}
	v = append(v, "go: "+runtime.Version())
	app.Version = strings.Join(v, "\n")

	root := "/run/hvn"
	xdgDirUsed := false
	xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR")
	if xdgRuntimeDir != "" {
		root = xdgRuntimeDir + "/haven"
		xdgDirUsed = true
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "log",
			Value: "",
			Usage: "set the log file to write haven logs to (default is '/dev/stderr')",
		},
		cli.StringFlag{
			Name:  "log-level",
			Value: "debug",
			Usage: "set  the log level ('DEBUG/debug', 'INFO/info', 'WARN/warn', 'ERROR/error', 'FATAL/fatal')",
		},
		cli.StringFlag{
			Name:  "root",
			Value: root,
			Usage: "root directory for storage of hvn command state",
		},
		cli.StringFlag{
			Name:   "endpoint",
			Value:  "https://127.0.0.1:8900",
			Usage:  "set the endpoint to use for the haven server",
			EnvVar: "HAVEN_ENDPOINT",
		},
		cli.BoolFlag{
			Name:  "secure",
			Usage: "use a secure connection to the haven server",
		},
	}

	app.Commands = []cli.Command{
		specCommand,
		pushCommand,
		pullCommand,
		sourceCommand,
		sinkCommand,
	}

	app.Before = func(ctx *cli.Context) error {
		if !ctx.IsSet("root") && xdgDirUsed {
			if err := os.MkdirAll(root, 0o700); err != nil {
				_, err = fmt.Fprintln(os.Stderr, "the path in $XDG_RUNTIME_DIR must be writable by the user")
				return err
			}
			if err := os.Chmod(root, os.FileMode(0o700)|os.ModeSticky); err != nil {
				_, err = fmt.Fprintln(os.Stderr, "you should check permission of the path in $XDG_RUNTIME_DIR")
				return err
			}
		}
		log.Init(ctx.String("log"), ctx.String("log-level"))
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
