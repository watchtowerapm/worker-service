package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/watchtower/worker-service/internal/consumer"
	"github.com/watchtower/worker-service/internal/handler"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	bufferAddr := envOr("REDIS_BUFFER_ADDR", "localhost:6379")
	bufferPass := envOr("REDIS_BUFFER_PASSWORD", "")
	chAddr := envOr("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := envOr("CLICKHOUSE_DB", "telemetry")
	chUser := envOr("CLICKHOUSE_USER", "watchtower")
	chPass := envOr("CLICKHOUSE_PASSWORD", "")
	healthPort := envOr("HEALTH_PORT", "3001")

	c, err := consumer.New(bufferAddr, bufferPass, chAddr, chDB, chUser, chPass)
	if err != nil {
		slog.Error("failed to initialise consumer", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	// Health-check HTTP server (no business routes on worker, just /health).
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health(c))

	srv := &http.Server{
		Addr:        ":" + healthPort,
		Handler:     mux,
		ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("worker health endpoint starting", "port", healthPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server error", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting stream consumer")
	if err := c.Run(ctx); err != nil {
		slog.Error("consumer exited with error", "error", err)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	slog.Info("worker stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
