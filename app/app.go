package app

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/elastic-io/haven/internal/api"
	"github.com/elastic-io/haven/internal/options"
	"github.com/elastic-io/haven/internal/storage"
)

type App struct {
	opts    *options.Options
	dbname  string
	storage storage.Storage
	server  *api.Server
}

func New(opts *options.Options) (*App, error) {
	app := &App{opts: opts}
	app.dbname = "repo.db"

	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	dbPath := filepath.Join(opts.DataDir, fmt.Sprintf("%s-%s", opts.RepoId, app.dbname))

	var err error
	app.storage, err = storage.NewStorage(opts.Backend, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}
	opts.Config.Storage = app.storage
	return app, opts.Config.Validate()
}

func (a *App) Run() error {
	server := api.New(a.opts.Config)
	if err := server.Init(); err != nil {
		return err
	}
	a.server = server
	return server.Serve()
}

func (a *App) Stop() error {
	if a.storage != nil {
		a.storage.Close()
	}
	if a.server != nil {
		a.server.Done()
	}
	return nil
}
