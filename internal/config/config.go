package config

import (
	"fmt"

	"github.com/elastic-io/haven/internal/storage"
	"github.com/urfave/cli"
)

type Config struct {
	Endpoint   string
	EnableAuth bool
	Username   string
	Password   string
	CertFile   string
	KeyFile    string
	Storage    storage.S3
	Modules    []string
}

func New(ctx *cli.Context) *Config {
	c := &Config{}
	c.Endpoint = ctx.String("endpoint")
	c.EnableAuth = ctx.Bool("auth")
	c.Username = ctx.String("username")
	c.Password = ctx.String("password")
	c.CertFile = ctx.String("cert")
	c.KeyFile = ctx.String("key")
	c.Modules = ctx.GlobalStringSlice("mod")
	return c
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if len(c.Modules) == 0 {
		return fmt.Errorf("at least one module is required")
	}
	if c.Storage == nil {
		return  fmt.Errorf("storage is required")
	}
	return nil
}
