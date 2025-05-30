package options

import (
	"fmt"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/urfave/cli"
)

type Options struct {
	RepoId   string
	DataDir  string
	Backend  string
	BodySize string
	Config   *config.Config
}

func New(ctx *cli.Context) *Options {
	opts := Options{RepoId: ctx.Args().First()}
	opts.DataDir = ctx.String("data")
	opts.Backend = ctx.GlobalString("backend")
	opts.BodySize = ctx.GlobalString("body")

	l := len(opts.BodySize)
	size, err := utils.ParseSize(opts.BodySize[0:l-1], opts.BodySize[l-1:])
	if err != nil {
		panic(err)
	}

	opts.Config = config.New(ctx)
	if size > 0 {
		opts.Config.BodyLimit = size
	}
	return &opts
}

func (o *Options) Validate() error {
	if len(o.RepoId) == 0 {
		return fmt.Errorf("repo_id is required")
	}
	if o.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if o.Backend == "" {
		log.Logger.Warn("backend is not set, using default backend")
	}
	return nil
}
