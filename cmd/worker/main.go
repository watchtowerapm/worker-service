package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/watchtower/worker-service/internal/config"
	"github.com/watchtower/worker-service/internal/consumer"
	"github.com/watchtower/worker-service/internal/handler"
)

// Set by Go ldflags at build time.
var (
	version   = "dev"
	buildDate = ""
	vcsRef    = ""
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	bufferAddr := config.EnvOr("REDIS_BUFFER_ADDR", "localhost:6379")
	bufferPass := config.EnvOr("REDIS_BUFFER_PASSWORD", "")
	chAddr := config.EnvOr("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := config.EnvOr("CLICKHOUSE_DB", "telemetry")
	chUser := config.EnvOr("CLICKHOUSE_USER", "watchtower")
	chPass := config.EnvOr("CLICKHOUSE_PASSWORD", "")
	healthPort := config.EnvOr("HEALTH_PORT", "3001")
	consumerName := config.EnvOr("WORKER_NAME", "worker-1")

	c, err := consumer.New(bufferAddr, bufferPass, chAddr, chDB, chUser, chPass, consumerName)
	if err != nil {
		slog.Error("failed to initialise consumer", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health(c))

	srv := &http.Server{
		Addr:         ":" + healthPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("worker health endpoint starting",
			"port", healthPort,
			"version", version,
			"build_date", buildDate,
			"vcs_ref", vcsRef,
			"worker_name", consumerName,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server error", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting stream consumer", "worker_name", consumerName)
	if err := c.Run(ctx); err != nil {
		slog.Error("consumer exited with error", "error", err)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	slog.Info("worker stopped")
}
