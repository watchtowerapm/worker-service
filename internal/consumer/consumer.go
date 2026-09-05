package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
)

const (
	telemetryStream     = "telemetry:events"
	consumerGroup       = "worker-group"
	defaultConsumerName = "worker-1"
	readBatchSize       = 1000
	blockDuration       = 5 * time.Second
	retryBackoff        = 2 * time.Second
)

// Consumer reads from a Redis Stream and writes batches to ClickHouse.
type Consumer struct {
	rdb          *redis.Client
	ch           clickhouse.Conn
	consumerName string
}

func New(bufferAddr, bufferPass, chAddr, chDB, chUser, chPass, consumerName string) (*Consumer, error) {
	if consumerName == "" {
		consumerName = defaultConsumerName
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         bufferAddr,
		Password:     bufferPass,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	err := rdb.XGroupCreateMkStream(ctx, telemetryStream, consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("xgroup create: %w", err)
	}

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr:            []string{chAddr},
		Auth:            clickhouse.Auth{Database: chDB, Username: chUser, Password: chPass},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	if err := ch.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	if err := ensureTable(ctx, ch, chDB); err != nil {
		return nil, fmt.Errorf("ensure table: %w", err)
	}

	return &Consumer{rdb: rdb, ch: ch, consumerName: consumerName}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: c.consumerName,
			Streams:  []string{telemetryStream, ">"},
			Count:    readBatchSize,
			Block:    blockDuration,
		}).Result()

		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("xreadgroup error", "error", err)
			time.Sleep(retryBackoff)
			continue
		}

		for _, stream := range msgs {
			ids, err := c.processBatch(ctx, stream.Messages)
			if err != nil {
				slog.Error("batch processing error", "error", err)
				continue
			}
			if len(ids) > 0 {
				if err := c.rdb.XAck(ctx, telemetryStream, consumerGroup, ids...).Err(); err != nil {
					slog.Error("xack error", "error", err)
				}
				if err := c.rdb.XDel(ctx, telemetryStream, ids...).Err(); err != nil {
					slog.Error("xdel error", "error", err)
				}
			}
		}
	}
}

func (c *Consumer) processBatch(ctx context.Context, msgs []redis.XMessage) ([]string, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	batch, err := c.ch.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return nil, fmt.Errorf("prepare batch: %w", err)
	}

	ids := make([]string, 0, len(msgs))
	totalRows := 0

	for _, msg := range msgs {
		projectID, _ := msg.Values["project_id"].(string)
		raw, _ := msg.Values["data"].(string)
		receivedAt := time.Now().UTC()

		records, err := extractRecords(raw)
		if err != nil {
			slog.Warn("failed to parse payload, storing raw", "id", msg.ID, "error", err)
			records = []json.RawMessage{json.RawMessage(raw)}
		}

		for _, rawRec := range records {
			var r record
			if err := json.Unmarshal(rawRec, &r); err != nil {
				slog.Warn("unmarshal error, skipping", "id", msg.ID, "error", err)
				continue
			}
			hydrateUserSensorFields(rawRec, &r)
			hydrateConnectionByEventType(rawRec, &r)

			eventTime := floatToTime(r.Timestamp, receivedAt)
			v := buildTelemetryInsertFields(&r)

			if err := batch.Append(
				msg.ID,
				projectID,
				receivedAt,
				r.Type,
				parseUUID(r.TraceID),
				eventTime,
				r.Server,
				r.Deploy,
				r.Group,
				r.ExecutionSource,
				parseUUID(r.ExecutionID),
				r.ExecutionPreview,
				r.ExecutionStage,
				r.User,
				derefInt32(r.Exceptions),
				derefInt32(r.Logs),
				derefInt32(r.Queries),
				derefInt32(r.LazyLoads),
				derefInt32(r.JobsQueued),
				derefInt32(r.Mail),
				derefInt32(r.Notifications),
				derefInt32(r.OutgoingReqs),
				derefInt32(r.FilesRead),
				derefInt32(r.FilesWritten),
				derefInt32(r.CacheEvents),
				derefInt32(r.HydratedModels),
				derefInt32(r.PeakMemoryUsage),
				derefStr(r.ExceptionPreview),
				derefStr(r.Context),
				v.reqMethod,
				v.reqURL,
				v.reqRouteName,
				v.reqRouteMethods,
				v.reqRouteDomain,
				v.reqRoutePath,
				v.reqRouteAction,
				v.reqIP,
				v.reqDuration,
				v.reqStatusCode,
				v.reqRequestSize,
				v.reqResponseSize,
				v.reqBootstrap,
				v.reqBeforeMiddleware,
				v.reqAction,
				v.reqRender,
				v.reqAfterMiddleware,
				v.reqSending,
				v.reqTerminating,
				v.reqHeaders,
				v.reqPayload,
				v.qrySQL,
				v.qryFile,
				v.qryLine,
				v.qryConnection,
				v.qryConnectionType,
				v.cacheStore,
				v.cacheKey,
				v.cacheType,
				v.cacheTTL,
				v.excClass,
				v.excMessage,
				v.excCode,
				v.excTrace,
				v.excHandled,
				v.excPHPVersion,
				v.excLaravelVersion,
				v.logLevel,
				v.logExtra,
				v.outHost,
				v.outMethod,
				v.outURL,
				v.outDuration,
				v.outRequestSize,
				v.outResponseSize,
				v.outStatusCode,
				v.jobID,
				v.jobAttemptID,
				v.jobAttempt,
				v.jobName,
				v.jobQueue,
				v.jobStatus,
				v.jobConnection,
				v.jobDuration,
				v.cmdCommand,
				v.cmdExitCode,
				v.schedCron,
				v.schedTimezone,
				v.schedRepeatSeconds,
				v.schedWithoutOverlapping,
				v.schedOnOneServer,
				v.schedRunInBackground,
				v.schedEvenInMaintenanceMode,
				v.mailMailer,
				v.mailSubject,
				v.mailTo,
				v.mailCC,
				v.mailBCC,
				v.mailAttachments,
				v.mailFailed,
				v.notifChannel,
				v.userDetailID,
				v.userDetailName,
				v.userDetailUsername,
				int32(r.V),
				string(rawRec),
			); err != nil {
				slog.Warn("append error, skipping record", "id", msg.ID, "type", r.Type, "error", err)
				continue
			}
			totalRows++
		}

		ids = append(ids, msg.ID)
	}

	if err := batch.Send(); err != nil {
		return nil, fmt.Errorf("batch send: %w", err)
	}

	slog.Info("batch flushed to clickhouse", "stream_messages", len(ids), "rows", totalRows)
	return ids, nil
}

func (c *Consumer) PingRedis(ctx context.Context) error      { return c.rdb.Ping(ctx).Err() }
func (c *Consumer) PingClickHouse(ctx context.Context) error { return c.ch.Ping(ctx) }
func (c *Consumer) Close() {
	_ = c.rdb.Close()
	_ = c.ch.Close()
}
