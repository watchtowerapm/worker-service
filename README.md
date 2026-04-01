# worker-service

The telemetry worker service for [Watchtower APM](https://github.com/watchtowerapm). Reads batches of telemetry events from a Redis Stream, normalises them from the Nightwatch agent payload format, and bulk-inserts them into ClickHouse.

[![CI](https://github.com/watchtowerapm/worker-service/actions/workflows/ci.yml/badge.svg?branch=1.x)](https://github.com/watchtowerapm/worker-service/actions/workflows/ci.yml)
[![Release](https://github.com/watchtowerapm/worker-service/actions/workflows/release.yml/badge.svg)](https://github.com/watchtowerapm/worker-service/actions/workflows/release.yml)
[![Go Version](https://img.shields.io/badge/go-1.23-00ADD8?logo=go)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## How it works

```
redis-buffer (Redis Stream: telemetry:events)
        │
        │  XREADGROUP — batches of 1 000 messages
        ▼
  worker-service
        ├── extract   → unwrap {"records":[...]} envelope or raw array/object
        ├── normalise → decode each record, resolve duplicate JSON key conflicts
        ├── map       → route fields into typed column groups by event_type
        └── insert    → ClickHouse batch INSERT INTO telemetry_events
                │
                ▼
        clickhouse (telemetry_events table)
```

Messages are acknowledged (`XACK`) and deleted (`XDEL`) from the stream only after a successful batch send to ClickHouse. On failure, they remain in the pending-entry list for redelivery.

---

## Supported event types

| Type | Description |
|---|---|
| `request` | HTTP request lifecycle |
| `outgoing-request` | External HTTP calls made by the app |
| `query` | Database queries |
| `cache-event` | Cache hits, misses, writes |
| `exception` | Thrown exceptions |
| `log` | Application log entries |
| `queued-job` / `job-attempt` | Queue job dispatch and execution |
| `scheduled-task` | Laravel scheduled task runs |
| `command` | Artisan command execution |
| `mail` | Mail sends |
| `notification` | Notification dispatches |
| `user` | Authenticated user resolution |

---

## Health endpoint

```bash
curl http://localhost:3001/health
```

```json
{
  "status": "ok",
  "checks": {
    "redis-buffer": "ok",
    "clickhouse": "ok"
  },
  "timestamp": "2026-04-01T12:00:00Z"
}
```

Returns `200 OK` when all checks pass, `503 Service Unavailable` when any dependency is unreachable.

---

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `REDIS_BUFFER_ADDR` | `localhost:6379` | Redis Stream host |
| `CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse native protocol host |
| `CLICKHOUSE_DB` | `telemetry` | ClickHouse database name |
| `CLICKHOUSE_USER` | `watchtower` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | _(empty)_ | ClickHouse password |
| `HEALTH_PORT` | `3001` | HTTP health endpoint port |

---

## Docker

Images are published to GitHub Container Registry for `linux/amd64` and `linux/arm64`.

```bash
# Latest stable release
docker pull ghcr.io/watchtowerapm/worker-service:v1.0.0

# Always latest 1.x
docker pull ghcr.io/watchtowerapm/worker-service:1
```

---

## Development

**Prerequisites:** Go 1.23+, Docker, Make.

```bash
# Start all dependencies + worker with hot reload
make watchtower-up        # from the repo root

# Or run the worker standalone
make run                  # builds and runs locally
make test                 # run tests with race detector
make lint                 # run golangci-lint
make cover                # open HTML coverage report
```

---

## Project structure

```
worker-service/
├── cmd/worker/          # main entrypoint
├── internal/
│   ├── consumer/        # Redis Stream consumer + ClickHouse writer
│   └── handler/         # HTTP health handler
└── docker/
    └── Dockerfile       # multi-stage: dev (Air) · builder · prod (distroless)
```

---

## Releasing

```bash
git tag v1.2.3
git push origin v1.2.3
```

The [Release workflow](.github/workflows/release.yml) builds multi-arch images, pushes them to GHCR, and creates a GitHub Release automatically.

---

## License

MIT — see [LICENSE](LICENSE).
