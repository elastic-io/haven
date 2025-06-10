package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/elastic-io/haven/internal"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/options"
	"github.com/elastic-io/haven/internal/utils"
	"github.com/urfave/cli"
)

type App interface {
	Run() error
	Stop() error
}

func Main(ctx *cli.Context, program func(opts *options.Options) (App, error), appname string) error {
	opts := options.New(ctx)
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("invalid options: %w", err)
	}

	app, err := program(opts)
	if err != nil {
		return err
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	errCh := make(chan error, 1)

	utils.SafeGo(func() {
		if err := app.Run(); err != nil {
			errCh <- err
		}
		close(errCh)
	})

	log.Logger.Infof("%s startup successfully", appname)

	select {
	case receivedSignal := <-signalCh:
		log.Logger.Debug("Received signal: ", receivedSignal, ", initiating graceful shutdown...")
	case err = <-errCh:
		if err != nil {
			log.Logger.Debug(appname, " error: ", err.Error(), ", shutting down...")
		} else {
			log.Logger.Debug(appname, " completed successfully, shutting down...")
		}
	}

	log.Logger.Infof("Stopping %s...", appname)
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopErrCh := make(chan error, 1)
	utils.SafeGo(func() {
		stopErrCh <- app.Stop()
		close(stopErrCh)
	})

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
	log.Logger.Debug(appname, " shutdown complete")
	return err
}
