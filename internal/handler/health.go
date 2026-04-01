package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type checker interface {
	PingRedis(ctx context.Context) error
	PingClickHouse(ctx context.Context) error
}

type healthResponse struct {
	Status    string            `json:"status"`
	Checks    map[string]string `json:"checks"`
	Timestamp string            `json:"timestamp"`
}

// Health returns an HTTP handler that reports liveness of downstream dependencies.
func Health(c checker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		checks := make(map[string]string, 2)
		overall := "ok"

		if err := c.PingRedis(ctx); err != nil {
			checks["redis-buffer"] = "unreachable: " + err.Error()
			overall = "degraded"
		} else {
			checks["redis-buffer"] = "ok"
		}

		if err := c.PingClickHouse(ctx); err != nil {
			checks["clickhouse"] = "unreachable: " + err.Error()
			overall = "degraded"
		} else {
			checks["clickhouse"] = "ok"
		}

		resp := healthResponse{
			Status:    overall,
			Checks:    checks,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}

		status := http.StatusOK
		if overall != "ok" {
			status = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
