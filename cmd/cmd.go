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

	root := "/run/haven"
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
			Usage: "root directory for storage of haven state",
		},
		cli.StringFlag{
			Name:  "backend",
			Value: "bolt",
			Usage: "backend storage type",
		},
		cli.StringSliceFlag{
			Name:  "mod",
			Value: &cli.StringSlice{"registry", "s3"},
			Usage: "set the module to load",
		},
		cli.StringFlag{
			Name:  "body",
			Value: "256M",
			Usage: "set the body size",
		},
		cli.StringFlag{
			Name:  "tmp, t",
			Value: "./tmp",
			Usage: "set the tmp directory",
		},
	}

	app.Commands = []cli.Command{
		startCommand,
		runCommand,
		killCommand,
		deleteCommand,
		listCommand,
		stateCommand,
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
		os.Setenv("TMPDIR", ctx.String("tmp"))
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
