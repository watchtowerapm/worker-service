package config

import "testing"

func TestEnvOr(t *testing.T) {
	t.Setenv("WATCHTOWER_WORKER_TEST", "yes")
	if got := EnvOr("WATCHTOWER_WORKER_TEST", "no"); got != "yes" {
		t.Fatalf("got %q", got)
	}
	if got := EnvOr("WATCHTOWER_WORKER_TEST_MISSING", "no"); got != "no" {
		t.Fatalf("got %q", got)
	}
}
