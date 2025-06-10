package cmd

import (
	"fmt"
	"os"

	"github.com/elastic-io/haven/app"
	"github.com/elastic-io/haven/internal/config"
	"github.com/urfave/cli"

	"path/filepath"

	"github.com/elastic-io/haven/internal/api"
	"github.com/elastic-io/haven/internal/options"
	"github.com/elastic-io/haven/internal/storage"
)

var sourceCommand = cli.Command{
	Name:        "source",
	Usage:       "run a repository to receive artifactory",
	ArgsUsage:   ``,
	Description: ``,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:   "auth, a",
			Usage:  "Enable basic authentication",
			EnvVar: "REPO_AUTH_ENABLED",
		},
		cli.StringFlag{
			Name:   "username, u",
			Value:  "",
			Usage:  "Basic auth username",
			EnvVar: "REPO_USERNAME",
		},
		cli.StringFlag{
			Name:   "password, pw",
			Value:  "",
			Usage:  "Basic auth password",
			EnvVar: "REPO_PASSWORD",
		},
		cli.StringFlag{
			Name:   "cert, c",
			Value:  "",
			Usage:  "TLS certificate file path",
			EnvVar: "REPO_CERT_FILE",
		},
		cli.StringFlag{
			Name:   "key, k",
			Value:  "",
			Usage:  "TLS private key file path",
			EnvVar: "REPO_KEY_FILE",
		},
		cli.StringSliceFlag{
			Name:  "mod",
			Value: &cli.StringSlice{"registry", "s3"},
			Usage: "set the module to load",
		},
		cli.StringFlag{
			Name:  "max-multipart, mm",
			Value: "128M",
			Usage: "Threshold for multipart upload",
		},
		cli.StringFlag{
			Name:  "chunk-length, cl",
			Value: "8M",
			Usage: "Chunk length for multipart upload",
		},
		cli.IntFlag{
			Name:  "read-timeout, rt",
			Value: 600,
			Usage: "Read timeout for HTTP requests",
		},
		cli.IntFlag{
			Name:  "write-timeout, wt",
			Value: 600,
			Usage: "Write timeout for HTTP requests",
		},
		cli.IntFlag{
			Name:  "idel-timeout, it",
			Value: 1800,
			Usage: "Chunk idle timeout for multipart upload",
		},
	},
	Action: func(ctx *cli.Context) error {
		if err := checkArgs(ctx, 1, exactArgs); err != nil {
			return fmt.Errorf("%s, please specify an id", err)
		}
		return app.Main(ctx, NewSource, "HavenSourceServer")
	},
}

type Source struct {
	config  *config.SourceConfig
	dbname  string
	storage storage.Storage
	server  *api.Server
}

func NewSource(opts *options.Options) (app.App, error) {
	src := &Source{config: config.NewSource(opts)}
	src.dbname = "repo.db"

	if err := os.MkdirAll(opts.Tmpdir, 0755); err != nil {
		return nil, fmt.Errorf(
			"failed to create temporary directory: %w", err)
	}

	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	dbPath := filepath.Join(opts.DataDir, fmt.Sprintf("%s-%s", opts.RepoId, src.dbname))

	var err error
	src.storage, err = storage.NewStorage(opts.Backend, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}
	src.config.Storage = src.storage
	return src, src.config.Validate()
}

func (s *Source) Run() error {
	server := api.New(s.config)
	if err := server.Init(); err != nil {
		return err
	}
	s.server = server
	return server.Serve()
}

func (s *Source) Stop() error {
	if s.storage != nil {
		s.storage.Close()
	}
	if s.server != nil {
		s.server.Done()
	}
	return nil
}
