// command fleetnorm normalizes vehicle fault events from configured adapters and
// routes them to owner defined outputs. this file is wiring; the behavior lives
// in internal.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/adapter/file"
	"github.com/ianfrushon/fleetnorm/internal/adapter/geotab"
	"github.com/ianfrushon/fleetnorm/internal/config"
	"github.com/ianfrushon/fleetnorm/internal/output"
	"github.com/ianfrushon/fleetnorm/internal/output/stdout"
	"github.com/ianfrushon/fleetnorm/internal/output/webhook"
	"github.com/ianfrushon/fleetnorm/internal/pipeline"
	"github.com/ianfrushon/fleetnorm/internal/router"
	"github.com/ianfrushon/fleetnorm/internal/server"
	"github.com/ianfrushon/fleetnorm/internal/store"
)

// stamped by the build, see the Makefile
var version = "dev"

func main() {
	configPath := flag.String("config", "fleetnorm.yaml", "path to the config file")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("fleetnorm", version)
		return
	}
	if err := run(*configPath, *logLevel); err != nil {
		slog.Error("fleetnorm stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath, logLevel string) error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	//logs go to stderr because the stdout output writes events to stdout
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	st, err := store.Open(cfg.Store.Path)
	if err != nil {
		return err
	}
	defer st.Close()

	metrics := pipeline.NewMetrics()
	sources, err := buildSources(cfg, pipeline.SkipAuditor(st, metrics))
	if err != nil {
		return err
	}
	destinations, err := buildDestinations(cfg)
	if err != nil {
		return err
	}

	p, err := pipeline.New(pipeline.Options{
		Store:           st,
		Router:          router.New(cfg.Rules),
		Metrics:         metrics,
		Sources:         sources,
		Destinations:    destinations,
		DedupeRetention: time.Duration(cfg.Store.DedupeRetention),
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := server.New(cfg.Server.Addr, st.Ping, metrics.Write)
	go func() {
		slog.Info("serving /healthz and /metrics", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("operational server stopped", "error", err)
		}
	}()

	slog.Info("fleetnorm started", "version", version, "config", configPath,
		"adapters", len(sources), "outputs", len(destinations), "rules", len(cfg.Rules))

	runErr := p.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("operational server did not shut down cleanly", "error", err)
	}
	slog.Info("fleetnorm stopped")
	return runErr
}

func buildSources(cfg *config.Config, onSkip adapter.SkipFunc) ([]pipeline.Source, error) {
	sources := make([]pipeline.Source, 0, len(cfg.Adapters))
	for _, a := range cfg.Adapters {
		var (
			built adapter.Adapter
			err   error
		)
		switch a.Type {
		case "file":
			if a.Strict != nil && !*a.Strict {
				built, err = file.NewLenient(a.Name, a.Path, onSkip)
			} else {
				built, err = file.New(a.Name, a.Path)
			}
		case "geotab":
			//always lenient: its input is a vendor feed we do not control, which
			//is the case the default adapter contract exists for
			built, err = geotab.New(geotab.Options{
				Name:         a.Name,
				Server:       a.Server,
				Database:     a.Database,
				Username:     a.Username(), //resolved and checked at config load
				Password:     a.Password(),
				SeedFrom:     a.SeedFrom.At, //config.Validate rejects a nil SeedFrom
				ResultsLimit: a.ResultsLimit,
				CacheRefresh: time.Duration(a.CacheRefresh),
				OnSkip:       onSkip,
			})
		default:
			//config.Validate rejects unknown types; this catches a new one
			//added there without a constructor here
			err = fmt.Errorf("no constructor for adapter type %q", a.Type)
		}
		if err != nil {
			return nil, err
		}
		sources = append(sources, pipeline.Source{Adapter: built, Interval: time.Duration(a.PollInterval)})
	}
	return sources, nil
}

func buildDestinations(cfg *config.Config) ([]pipeline.Destination, error) {
	destinations := make([]pipeline.Destination, 0, len(cfg.Outputs))
	for _, o := range cfg.Outputs {
		var (
			built       output.Output
			err         error
			maxAttempts = 1 //an output with no retry policy gets one attempt
		)
		switch o.Type {
		case "stdout":
			built = stdout.New(o.Name, os.Stdout)
		case "webhook":
			maxAttempts = o.Retry.MaxAttempts
			built, err = webhook.New(webhook.Options{
				Name:    o.Name,
				URL:     o.URL,
				Secret:  o.Secret(), //resolved and checked at config load //resolved and checked at config load
				Timeout: time.Duration(o.Timeout),
			})
		default:
			err = fmt.Errorf("no constructor for output type %q", o.Type)
		}
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, pipeline.Destination{
			Output:      built,
			Buffer:      o.Buffer,
			MaxAttempts: maxAttempts,
		})
	}
	return destinations, nil
}
