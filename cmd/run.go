package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/elastic-io/haven/app"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/options"
	"github.com/urfave/cli"
)

var runCommand = cli.Command{
	Name:        "run",
	Usage:       "run a artifactory repository",
	ArgsUsage:   ``,
	Description: ``,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "endpoint, e",
			Value:  "localhost:1986",
			Usage:  "Artifactory repository server address",
			EnvVar: "REPO_ENDPOINT",
		},
		cli.StringFlag{
			Name:   "data, d",
			Value:  "./data",
			Usage:  "Data directory for storage",
			EnvVar: "REPO_DATA_DIR",
		},
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
	},
	Action: func(ctx *cli.Context) error {
		if err := checkArgs(ctx, 1, exactArgs); err != nil {
			return err
		}
		return Main(ctx)
	},
}

func Main(ctx *cli.Context) error {
	opts := options.New(ctx)
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("invalid options: %w", err)
	}

	app, err := app.New(opts)
	if err != nil {
		return err
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	errCh := make(chan error, 1)

	go func() {
		if err := app.Run(); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	log.Logger.Info("Application startup successfully")

	select {
	case receivedSignal := <-signalCh:
		log.Logger.Debug("Received signal: ", receivedSignal, ", initiating graceful shutdown...")
	case err = <-errCh:
		if err != nil {
			log.Logger.Debug("Application error: ", err.Error(), ", shutting down...")
		} else {
			log.Logger.Debug("Application completed successfully, shutting down...")
		}
	}

	log.Logger.Info("Stopping application...")
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopErrCh := make(chan error, 1)
	go func() {
		stopErrCh <- app.Stop()
		close(stopErrCh)
	}()

	select {
	case stopErr := <-stopErrCh:
		if stopErr != nil {
			log.Logger.Debug("Error during shutdown: ", stopErr)
			if err == nil {
				err = stopErr
			}
		}
	case <-stopCtx.Done():
		log.Logger.Debug("Shutdown timed out")
		if err == nil {
			err = stopCtx.Err()
		}
	}
	log.Logger.Debug("Application shutdown complete")
	return err
}
