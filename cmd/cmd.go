package cmd

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"

	_ "github.com/elastic-io/haven/internal"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/utils"
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
			Name:   "endpoint, e",
			Value:  "localhost:1986",
			Usage:  "Artifactory repository server address",
			EnvVar: "REPO_ENDPOINT",
		},
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
		cli.StringFlag{
			Name:  "body",
			Value: "256M",
			Usage: "set the body size",
		},
		cli.StringFlag{
			Name:  "tmp",
			Value: "./tmp",
			Usage: "set the tmp directory",
		},
		cli.IntFlag{
			Name:  "gc-percent",
			Value: 30,
			Usage: "set the garbage collection percent",
		},
		cli.StringFlag{
			Name:  "memory-limit",
			Value: "4G",
			Usage: "set the memory limit",
		},
		cli.BoolFlag{
			Name:  "secure",
			Usage: "set the secure flag",
		},
	}

	app.Commands = []cli.Command{
		pushCommand,
		pullCommand,
		sourceCommand,
		sinkCommand,
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

		debug.SetGCPercent(ctx.Int("gc-percent"))
		debug.SetMemoryLimit(int64(utils.MustParseSize(ctx.String("memory-limit"))))
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
