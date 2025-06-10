package config

import (
	"fmt"

	"github.com/elastic-io/haven/internal/options"
	"github.com/elastic-io/haven/internal/storage"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/urfave/cli"
)

type SourceConfig struct {
	*options.Options

	Endpoint     string
	EnableAuth   bool
	Username     string
	Password     string
	CertFile     string
	KeyFile      string
	MaxMultipart string
	ChunkLength  string
	ReadTimeout  int
	WriteTimeout int
	IdleTimeout  int
	Modules      []string

	Storage storage.Storage
}

func NewSource(opts *options.Options) *SourceConfig {
	c := &SourceConfig{Options: opts}
	c.Endpoint = opts.Ctx.String("endpoint")
	c.EnableAuth = opts.Ctx.Bool("auth")
	c.Username = opts.Ctx.String("username")
	c.Password = opts.Ctx.String("password")
	c.CertFile = opts.Ctx.String("cert")
	c.KeyFile = opts.Ctx.String("key")
	c.MaxMultipart = opts.Ctx.String("max-multipart")
	c.ChunkLength = opts.Ctx.String("chunk-length")
	c.ReadTimeout = opts.Ctx.Int("read-timeout")
	c.WriteTimeout = opts.Ctx.Int("write-timeout")
	c.IdleTimeout = opts.Ctx.Int("idle-timeout")
	c.Modules = opts.Ctx.StringSlice("mod")
	return c
}

func (c *SourceConfig) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if len(c.Modules) == 0 {
		return fmt.Errorf("at least one module is required")
	}
	if c.MaxMultipart == "" {
		return fmt.Errorf("max-multipart is required")
	}
	if c.Storage == nil {
		return fmt.Errorf("storage is required")
	}
	return nil
}

type SinkConfig struct {
	Endpoint string
}

func NewSink(ctx *cli.Context) *SinkConfig {
	return nil
}

func (c *SinkConfig) Validate() error {
	return nil
}

type PushConfig struct {
	*options.Options

	Endpoint string
	Path     string
	Tag      string
	BigFileLimit int
	ExcludeFiles []string
	ExcludeExts []string
}

func NewPush(opts *options.Options) *PushConfig {
	pc := &PushConfig{Options: opts}
	pc.Endpoint = opts.Ctx.GlobalString("endpoint")
	pc.Path = opts.Ctx.String("path")
	pc.Tag = opts.Ctx.String("tag")
	limit := opts.Ctx.String("limit")

	size, err := utils.ParseSize(limit[0:len(limit)-1], limit[len(limit)-1:])
	if err != nil {
		panic(err)
	}
	pc.BigFileLimit = size

	pc.ExcludeExts = opts.Ctx.StringSlice("exclude-exts")
	pc.ExcludeFiles = opts.Ctx.StringSlice("exclude-files")
	return nil
}

func (c *PushConfig) Validate() error {
	if 0 == len(c.Endpoint) {
		return fmt.Errorf("endpoint is required")
	}
	if 0 == len(c.Path) {
		return fmt.Errorf("path is required")
	}
	if 0 == len(c.Tag) {
		return fmt.Errorf("tag is required")
	}
	return nil
}
