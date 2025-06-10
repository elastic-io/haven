package options

import (
	"fmt"

	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/urfave/cli"
)

type Options struct {
	Ctx         *cli.Context
	RepoId      string
	DataDir     string
	Tmpdir      string
	Backend     string
	Endpoint    string
	BodySize    int
	GcPercent   int
	MemoryLimit int
	Secure      bool
}

func New(ctx *cli.Context) *Options {
	opts := Options{Ctx: ctx}
	opts.RepoId = ctx.Args().First()
	opts.Endpoint = ctx.GlobalString("endpoint")
	opts.DataDir = ctx.GlobalString("data")
	opts.Tmpdir = ctx.GlobalString("tmp")
	opts.Backend = ctx.GlobalString("backend")

	body := ctx.GlobalString("body")
	size, err := utils.ParseSize(body[0:len(body)-1], body[len(body)-1:])
	if err != nil {
		panic(err)
	}
	opts.BodySize = size

	opts.GcPercent = ctx.GlobalInt("gc-percent")
	opts.MemoryLimit = utils.MustParseSize(ctx.GlobalString("memory-limit"))
	opts.Secure = ctx.GlobalBool("secure")
	return &opts
}

func (o *Options) Validate() error {
	if len(o.RepoId) == 0 {
		return fmt.Errorf("repo_id is required")
	}
	if o.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if o.Tmpdir == "" {
		return fmt.Errorf("tmp directory is required")
	}
	if o.Backend == "" {
		log.Logger.Warn("backend is not set, using default backend")
	}
	if o.BodySize <= 0 {
		return fmt.Errorf("body size is required")
	}
	if o.GcPercent < 10 || o.GcPercent > 100 {
		return fmt.Errorf("gc-percent must be between 10 and 100")
	}
	if o.MemoryLimit <= types.GB {
		return fmt.Errorf("memory-limit must be greater than 1GB")
	}
	return nil
}
