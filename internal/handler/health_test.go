package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeChecker struct {
	redisErr error
	chErr    error
}

func (f fakeChecker) PingRedis(ctx context.Context) error      { return f.redisErr }
func (f fakeChecker) PingClickHouse(ctx context.Context) error { return f.chErr }

func TestHealthOK(t *testing.T) {
	h := Health(fakeChecker{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

func TestHealthDegraded(t *testing.T) {
	h := Health(fakeChecker{redisErr: errors.New("down")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "degraded") {
		t.Fatalf("body=%s", rec.Body.String())
	}
}
