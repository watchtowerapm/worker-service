package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
)

const (
	telemetryStream = "telemetry:events"
	consumerGroup   = "worker-group"
	consumerName    = "worker-1"
	readBatchSize   = 1000
	blockDuration   = 5 * time.Second
	retryBackoff    = 2 * time.Second
)

// agentBatch is the top-level envelope the Nightwatch agent POSTs: {"records":[...]}
type agentBatch struct {
	Records []json.RawMessage `json:"records"`
}

// record mirrors every field the Nightwatch sensors can emit.
// Pointer types let json.Unmarshal distinguish missing from zero. We dereference
// to zero-values before appending to ClickHouse (no Nullable columns).
//
// Nightwatch reuses JSON keys across event types. encoding/json does not support
// two struct fields with the same tag — values can be dropped. Overlaps handled
// after Unmarshal: hydrateUserSensorFields (name), hydrateConnectionByEventType (connection).
// buildTelemetryInsertFields maps each event_type to the correct ClickHouse column group
// so duration / HTTP / file-line / class are not duplicated across req_, out_, job_, and qry_*.
type record struct {
	// ── shared / envelope ─────────────────────────────────────────────────────
	V                int     `json:"v"`
	Type             string  `json:"t"`
	Timestamp        float64 `json:"timestamp"`
	Deploy           string  `json:"deploy"`
	Server           string  `json:"server"`
	Group            string  `json:"_group"`
	TraceID          string  `json:"trace_id"`
	ExecutionSource  string  `json:"execution_source"`
	ExecutionID      string  `json:"execution_id"`
	ExecutionPreview string  `json:"execution_preview"`
	ExecutionStage   string  `json:"execution_stage"`
	User             string  `json:"user"`

	// ── summary counters ──────────────────────────────────────────────────────
	Exceptions       *int32  `json:"exceptions"`
	Logs             *int32  `json:"logs"`
	Queries          *int32  `json:"queries"`
	LazyLoads        *int32  `json:"lazy_loads"`
	JobsQueued       *int32  `json:"jobs_queued"`
	Mail             *int32  `json:"mail"`
	Notifications    *int32  `json:"notifications"`
	OutgoingReqs     *int32  `json:"outgoing_requests"`
	FilesRead        *int32  `json:"files_read"`
	FilesWritten     *int32  `json:"files_written"`
	CacheEvents      *int32  `json:"cache_events"`
	HydratedModels   *int32  `json:"hydrated_models"`
	PeakMemoryUsage  *int32  `json:"peak_memory_usage"`
	ExceptionPreview *string `json:"exception_preview"`
	Context          *string `json:"context"`

	// ── request ───────────────────────────────────────────────────────────────
	Method           *string  `json:"method"`
	URL              *string  `json:"url"`
	RouteName        *string  `json:"route_name"`
	RouteMethods     []string `json:"route_methods"`
	RouteDomain      *string  `json:"route_domain"`
	RoutePath        *string  `json:"route_path"`
	RouteAction      *string  `json:"route_action"`
	IP               *string  `json:"ip"`
	Duration         *int64   `json:"duration"`
	StatusCode       *int32   `json:"status_code"`
	RequestSize      *int64   `json:"request_size"`
	ResponseSize     *int64   `json:"response_size"`
	Bootstrap        *int64   `json:"bootstrap"`
	BeforeMiddleware *int64   `json:"before_middleware"`
	Action           *int64   `json:"action"`
	Render           *int64   `json:"render"`
	AfterMiddleware  *int64   `json:"after_middleware"`
	Sending          *int64   `json:"sending"`
	Terminating      *int64   `json:"terminating"`
	Headers          *string  `json:"headers"`
	Payload          *string  `json:"payload"`

	// ── query ─────────────────────────────────────────────────────────────────
	SQL            *string `json:"sql"`
	File           *string `json:"file"`
	Line           *int32  `json:"line"`
	// connection is json:"-" — same key is used for queue connection on queued-job /
	// job-attempt; see hydrateConnectionByEventType.
	Connection     *string `json:"-"`
	ConnectionType *string `json:"connection_type"`

	// ── cache-event ───────────────────────────────────────────────────────────
	Store     *string `json:"store"`
	Key       *string `json:"key"`
	CacheType *string `json:"type"`
	TTL       *int32  `json:"ttl"`

	// ── exception ─────────────────────────────────────────────────────────────
	Class          *string `json:"class"`
	Message        *string `json:"message"`
	Code           *string `json:"code"`
	Trace          *string `json:"trace"`
	Handled        *bool   `json:"handled"`
	PHPVersion     *string `json:"php_version"`
	LaravelVersion *string `json:"laravel_version"`

	// ── log ───────────────────────────────────────────────────────────────────
	Level *string `json:"level"`
	Extra *string `json:"extra"`

	// ── outgoing-request ──────────────────────────────────────────────────────
	// Note: outgoing-request uses the same JSON keys as request (method, url,
	// duration, status_code, request_size, response_size) — they are already
	// covered by the fields above. Only "host" is outgoing-specific.
	Host *string `json:"host"`

	// ── queued-job / job-attempt ──────────────────────────────────────────────
	JobID         *string `json:"job_id"`
	AttemptID     *string `json:"attempt_id"`
	Attempt       *int32  `json:"attempt"`
	Name          *string `json:"name"`
	Queue         *string `json:"queue"`
	Status        *string `json:"status"`
	// json:"-" — shares JSON key "connection" with query (DB); see hydrateConnectionByEventType.
	JobConnection *string `json:"-"`
	// JobDuration reuses Duration (json:"duration") — same key in the agent payload

	// ── command ───────────────────────────────────────────────────────────────
	Command  *string `json:"command"`
	ExitCode *int32  `json:"exit_code"`

	// ── scheduled-task ────────────────────────────────────────────────────────
	Cron                  *string `json:"cron"`
	Timezone              *string `json:"timezone"`
	RepeatSeconds         *int32  `json:"repeat_seconds"`
	WithoutOverlapping    *bool   `json:"without_overlapping"`
	OnOneServer           *bool   `json:"on_one_server"`
	RunInBackground       *bool   `json:"run_in_background"`
	EvenInMaintenanceMode *bool   `json:"even_in_maintenance_mode"`

	// ── mail ──────────────────────────────────────────────────────────────────
	Mailer      *string `json:"mailer"`
	Subject     *string `json:"subject"`
	To          *int32  `json:"to"`
	CC          *int32  `json:"cc"`
	BCC         *int32  `json:"bcc"`
	Attachments *int32  `json:"attachments"`
	Failed      *bool   `json:"failed"`

	// ── notification ──────────────────────────────────────────────────────────
	Channel *string `json:"channel"`

	// ── user (UserSensor: id, name, username) ─────────────────────────────────
	// user_detail_id / user_detail_username use normal JSON keys. user_detail_name
	// MUST use json:"-" because QueuedJobSensor also emits json:"name" (job name);
	// duplicate json tags are implementation-defined in encoding/json and the
	// "name" value is often dropped entirely — see hydrateUserSensorFields.
	UserDetailID       *string `json:"id"`
	UserDetailName     *string `json:"-"`
	UserDetailUsername *string `json:"username"`
}

// Consumer reads from a Redis Stream and writes batches to ClickHouse.
type Consumer struct {
	rdb *redis.Client
	ch  clickhouse.Conn
}

func New(bufferAddr, bufferPass, chAddr, chDB, chUser, chPass string) (*Consumer, error) {
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
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{Database: chDB, Username: chUser, Password: chPass},
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

	return &Consumer{rdb: rdb, ch: ch}, nil
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
			Consumer: consumerName,
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
				// ── envelope / shared ──────────────────────────────────────
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
				// ── summary counters ───────────────────────────────────────
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
				// ── request ───────────────────────────────────────────────
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
				// ── query ─────────────────────────────────────────────────
				v.qrySQL,
				v.qryFile,
				v.qryLine,
				v.qryConnection,
				v.qryConnectionType,
				// ── cache-event ───────────────────────────────────────────
				v.cacheStore,
				v.cacheKey,
				v.cacheType,
				v.cacheTTL,
				// ── exception ─────────────────────────────────────────────
				v.excClass,
				v.excMessage,
				v.excCode,
				v.excTrace,
				v.excHandled,
				v.excPHPVersion,
				v.excLaravelVersion,
				// ── log ───────────────────────────────────────────────────
				v.logLevel,
				v.logExtra,
				// ── outgoing-request ──────────────────────────────────────
				v.outHost,
				v.outMethod,
				v.outURL,
				v.outDuration,
				v.outRequestSize,
				v.outResponseSize,
				v.outStatusCode,
				// ── queued-job / job-attempt (and scheduled-task name/status/duration)
				v.jobID,
				v.jobAttemptID,
				v.jobAttempt,
				v.jobName,
				v.jobQueue,
				v.jobStatus,
				v.jobConnection,
				v.jobDuration,
				// ── command ───────────────────────────────────────────────
				v.cmdCommand,
				v.cmdExitCode,
				// ── scheduled-task ────────────────────────────────────────
				v.schedCron,
				v.schedTimezone,
				v.schedRepeatSeconds,
				v.schedWithoutOverlapping,
				v.schedOnOneServer,
				v.schedRunInBackground,
				v.schedEvenInMaintenanceMode,
				// ── mail ──────────────────────────────────────────────────
				v.mailMailer,
				v.mailSubject,
				v.mailTo,
				v.mailCC,
				v.mailBCC,
				v.mailAttachments,
				v.mailFailed,
				// ── notification ──────────────────────────────────────────
				v.notifChannel,
				// ── user ──────────────────────────────────────────────────
				v.userDetailID,
				v.userDetailName,
				v.userDetailUsername,
				// ── schema version ────────────────────────────────────────
				int32(r.V),
				// ── raw ───────────────────────────────────────────────────
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

// ─── zero-value deref helpers ─────────────────────────────────────────────────

func derefInt32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}

func derefInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// derefBool maps nil/false → 0, true → 1 (ClickHouse UInt8).
func derefBool(b *bool) uint8 {
	if b != nil && *b {
		return 1
	}
	return 0
}

// parseUUID converts a string UUID to uuid.UUID, returning uuid.Nil on failure.
func parseUUID(s string) uuid.UUID {
	if id, err := uuid.Parse(s); err == nil {
		return id
	}
	return uuid.Nil
}

// ─── other helpers ────────────────────────────────────────────────────────────

func floatToTime(ts float64, fallback time.Time) time.Time {
	if ts <= 0 {
		return fallback
	}
	sec := int64(ts)
	nsec := int64((ts - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

// hydrateUserSensorFields fills user_detail_name for t=user records. The display
// name shares JSON key "name" with queued-job payloads; encoding/json cannot
// populate two struct fields with the same tag, so the value is unmarshalled here.
func hydrateUserSensorFields(raw []byte, r *record) {
	if r.Type != "user" {
		return
	}
	var u struct {
		Name *string `json:"name"`
	}
	if err := json.Unmarshal(raw, &u); err != nil {
		return
	}
	r.UserDetailName = u.Name
	// "name" now unmarshals only into Name (job); clear so we don't write user display name to job_name.
	r.Name = nil
}

// hydrateConnectionByEventType fills qry_connection vs job_connection. Nightwatch uses
// the same JSON key "connection" for the database connection name (query) and the
// queue connection name (queued-job, job-attempt); duplicate struct tags drop values.
func hydrateConnectionByEventType(raw []byte, r *record) {
	var payload struct {
		Connection *string `json:"connection"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return
	}
	switch r.Type {
	case "query":
		r.Connection = payload.Connection
	case "queued-job", "job-attempt":
		r.JobConnection = payload.Connection
	default:
		// leave unset; avoids writing queue names into qry_* columns on unrelated types
	}
}

// telemetryInsertFields maps a Nightwatch record into the wide telemetry_events row.
// Overlapping JSON keys (duration, method/url, file/line, class/message) are written only
// into the column group that matches event_type so request rows do not duplicate into
// outgoing columns and exception file/line do not land in qry_*.
type telemetryInsertFields struct {
	reqMethod, reqURL, reqRouteName, reqRouteMethods, reqRouteDomain, reqRoutePath, reqRouteAction, reqIP string
	reqDuration                                                                                           int64
	reqStatusCode                                                                                         int32
	reqRequestSize, reqResponseSize                                                                       int64
	reqBootstrap, reqBeforeMiddleware, reqAction, reqRender, reqAfterMiddleware, reqSending, reqTerminating int64
	reqHeaders, reqPayload                                                                                string

	qrySQL, qryFile       string
	qryLine               int32
	qryConnection         string
	qryConnectionType     string

	cacheStore, cacheKey, cacheType string
	cacheTTL                      int32

	excClass, excMessage, excCode, excTrace string
	excHandled                              uint8
	excPHPVersion, excLaravelVersion        string

	logLevel, logExtra string

	outHost, outMethod, outURL string
	outDuration                int64
	outRequestSize, outResponseSize int64
	outStatusCode              int32

	jobID, jobAttemptID, jobName, jobQueue, jobStatus, jobConnection string
	jobAttempt                                                       int32
	jobDuration                                                      int64

	cmdCommand string
	cmdExitCode int32

	schedCron, schedTimezone string
	schedRepeatSeconds       int32
	schedWithoutOverlapping, schedOnOneServer, schedRunInBackground, schedEvenInMaintenanceMode uint8

	mailMailer, mailSubject string
	mailTo, mailCC, mailBCC, mailAttachments int32
	mailFailed                             uint8

	notifChannel string

	userDetailID, userDetailName, userDetailUsername string
}

func buildTelemetryInsertFields(r *record) telemetryInsertFields {
	switch r.Type {
	case "request":
		return insertFieldsRequest(r)
	case "outgoing-request":
		return insertFieldsOutgoing(r)
	case "query":
		return insertFieldsQuery(r)
	case "cache-event":
		return insertFieldsCache(r)
	case "exception":
		return insertFieldsException(r)
	case "log":
		return insertFieldsLog(r)
	case "queued-job", "job-attempt":
		return insertFieldsJob(r)
	case "scheduled-task":
		return insertFieldsScheduledTask(r)
	case "command":
		return insertFieldsCommand(r)
	case "mail":
		return insertFieldsMail(r)
	case "notification":
		return insertFieldsNotification(r)
	case "user":
		return insertFieldsUser(r)
	default:
		return insertFieldsLegacy(r)
	}
}

func insertFieldsRequest(r *record) (f telemetryInsertFields) {
	f.reqMethod = derefStr(r.Method)
	f.reqURL = derefStr(r.URL)
	f.reqRouteName = derefStr(r.RouteName)
	f.reqRouteMethods = strings.Join(r.RouteMethods, ",")
	f.reqRouteDomain = derefStr(r.RouteDomain)
	f.reqRoutePath = derefStr(r.RoutePath)
	f.reqRouteAction = derefStr(r.RouteAction)
	f.reqIP = derefStr(r.IP)
	f.reqDuration = derefInt64(r.Duration)
	f.reqStatusCode = derefInt32(r.StatusCode)
	f.reqRequestSize = derefInt64(r.RequestSize)
	f.reqResponseSize = derefInt64(r.ResponseSize)
	f.reqBootstrap = derefInt64(r.Bootstrap)
	f.reqBeforeMiddleware = derefInt64(r.BeforeMiddleware)
	f.reqAction = derefInt64(r.Action)
	f.reqRender = derefInt64(r.Render)
	f.reqAfterMiddleware = derefInt64(r.AfterMiddleware)
	f.reqSending = derefInt64(r.Sending)
	f.reqTerminating = derefInt64(r.Terminating)
	f.reqHeaders = derefStr(r.Headers)
	f.reqPayload = derefStr(r.Payload)
	return f
}

func insertFieldsOutgoing(r *record) (f telemetryInsertFields) {
	f.outHost = derefStr(r.Host)
	f.outMethod = derefStr(r.Method)
	f.outURL = derefStr(r.URL)
	f.outDuration = derefInt64(r.Duration)
	f.outRequestSize = derefInt64(r.RequestSize)
	f.outResponseSize = derefInt64(r.ResponseSize)
	f.outStatusCode = derefInt32(r.StatusCode)
	return f
}

func insertFieldsQuery(r *record) (f telemetryInsertFields) {
	// No dedicated qry_duration column; store query execution time in req_duration (legacy convention).
	f.reqDuration = derefInt64(r.Duration)
	f.qrySQL = derefStr(r.SQL)
	f.qryFile = derefStr(r.File)
	f.qryLine = derefInt32(r.Line)
	f.qryConnection = derefStr(r.Connection)
	f.qryConnectionType = derefStr(r.ConnectionType)
	return f
}

func insertFieldsCache(r *record) (f telemetryInsertFields) {
	f.cacheStore = derefStr(r.Store)
	f.cacheKey = derefStr(r.Key)
	f.cacheType = derefStr(r.CacheType)
	f.cacheTTL = derefInt32(r.TTL)
	return f
}

func insertFieldsException(r *record) (f telemetryInsertFields) {
	f.excClass = derefStr(r.Class)
	f.excMessage = derefStr(r.Message)
	f.excCode = derefStr(r.Code)
	f.excTrace = derefStr(r.Trace)
	f.excHandled = derefBool(r.Handled)
	f.excPHPVersion = derefStr(r.PHPVersion)
	f.excLaravelVersion = derefStr(r.LaravelVersion)
	return f
}

func insertFieldsLog(r *record) (f telemetryInsertFields) {
	f.excMessage = derefStr(r.Message)
	f.logLevel = derefStr(r.Level)
	f.logExtra = derefStr(r.Extra)
	return f
}

func insertFieldsJob(r *record) (f telemetryInsertFields) {
	f.jobID = derefStr(r.JobID)
	f.jobAttemptID = derefStr(r.AttemptID)
	f.jobAttempt = derefInt32(r.Attempt)
	f.jobName = derefStr(r.Name)
	f.jobQueue = derefStr(r.Queue)
	f.jobStatus = derefStr(r.Status)
	f.jobConnection = derefStr(r.JobConnection)
	f.jobDuration = derefInt64(r.Duration)
	return f
}

func insertFieldsScheduledTask(r *record) (f telemetryInsertFields) {
	// Payload reuses name/status/duration keys with jobs; there are no sched_* name/duration columns.
	f.jobName = derefStr(r.Name)
	f.jobStatus = derefStr(r.Status)
	f.jobDuration = derefInt64(r.Duration)
	f.schedCron = derefStr(r.Cron)
	f.schedTimezone = derefStr(r.Timezone)
	f.schedRepeatSeconds = derefInt32(r.RepeatSeconds)
	f.schedWithoutOverlapping = derefBool(r.WithoutOverlapping)
	f.schedOnOneServer = derefBool(r.OnOneServer)
	f.schedRunInBackground = derefBool(r.RunInBackground)
	f.schedEvenInMaintenanceMode = derefBool(r.EvenInMaintenanceMode)
	return f
}

func insertFieldsCommand(r *record) (f telemetryInsertFields) {
	// Same convention as query/mail/notification: runtime (µs) in req_duration for aggregates / monitoring.
	f.reqDuration = derefInt64(r.Duration)
	f.cmdCommand = derefStr(r.Command)
	f.cmdExitCode = derefInt32(r.ExitCode)
	f.excClass = derefStr(r.Class)
	return f
}

func insertFieldsMail(r *record) (f telemetryInsertFields) {
	// Same convention as query events: duration (µs) lives in req_duration for aggregates.
	f.reqDuration = derefInt64(r.Duration)
	f.mailMailer = derefStr(r.Mailer)
	f.mailSubject = derefStr(r.Subject)
	f.mailTo = derefInt32(r.To)
	f.mailCC = derefInt32(r.CC)
	f.mailBCC = derefInt32(r.BCC)
	f.mailAttachments = derefInt32(r.Attachments)
	f.mailFailed = derefBool(r.Failed)
	f.excClass = derefStr(r.Class)
	return f
}

func insertFieldsNotification(r *record) (f telemetryInsertFields) {
	// Same convention as mail events: duration (µs) in req_duration for aggregates / monitoring.
	f.reqDuration = derefInt64(r.Duration)
	f.notifChannel = derefStr(r.Channel)
	f.excClass = derefStr(r.Class)
	return f
}

func insertFieldsUser(r *record) (f telemetryInsertFields) {
	f.userDetailID = derefStr(r.UserDetailID)
	f.userDetailName = derefStr(r.UserDetailName)
	f.userDetailUsername = derefStr(r.UserDetailUsername)
	return f
}

// insertFieldsLegacy mirrors the pre-type-aware worker: overlapping keys populate every bucket.
// Used for unknown event_type values so new sensors still land structured data somewhere.
func insertFieldsLegacy(r *record) (f telemetryInsertFields) {
	f = insertFieldsRequest(r)
	f.qrySQL = derefStr(r.SQL)
	f.qryFile = derefStr(r.File)
	f.qryLine = derefInt32(r.Line)
	f.qryConnection = derefStr(r.Connection)
	f.qryConnectionType = derefStr(r.ConnectionType)
	f.cacheStore = derefStr(r.Store)
	f.cacheKey = derefStr(r.Key)
	f.cacheType = derefStr(r.CacheType)
	f.cacheTTL = derefInt32(r.TTL)
	f.excClass = derefStr(r.Class)
	f.excMessage = derefStr(r.Message)
	f.excCode = derefStr(r.Code)
	f.excTrace = derefStr(r.Trace)
	f.excHandled = derefBool(r.Handled)
	f.excPHPVersion = derefStr(r.PHPVersion)
	f.excLaravelVersion = derefStr(r.LaravelVersion)
	f.logLevel = derefStr(r.Level)
	f.logExtra = derefStr(r.Extra)
	f.outHost = derefStr(r.Host)
	f.outMethod = derefStr(r.Method)
	f.outURL = derefStr(r.URL)
	f.outDuration = derefInt64(r.Duration)
	f.outRequestSize = derefInt64(r.RequestSize)
	f.outResponseSize = derefInt64(r.ResponseSize)
	f.outStatusCode = derefInt32(r.StatusCode)
	f.jobID = derefStr(r.JobID)
	f.jobAttemptID = derefStr(r.AttemptID)
	f.jobAttempt = derefInt32(r.Attempt)
	f.jobName = derefStr(r.Name)
	f.jobQueue = derefStr(r.Queue)
	f.jobStatus = derefStr(r.Status)
	f.jobConnection = derefStr(r.JobConnection)
	f.jobDuration = derefInt64(r.Duration)
	f.cmdCommand = derefStr(r.Command)
	f.cmdExitCode = derefInt32(r.ExitCode)
	f.schedCron = derefStr(r.Cron)
	f.schedTimezone = derefStr(r.Timezone)
	f.schedRepeatSeconds = derefInt32(r.RepeatSeconds)
	f.schedWithoutOverlapping = derefBool(r.WithoutOverlapping)
	f.schedOnOneServer = derefBool(r.OnOneServer)
	f.schedRunInBackground = derefBool(r.RunInBackground)
	f.schedEvenInMaintenanceMode = derefBool(r.EvenInMaintenanceMode)
	f.mailMailer = derefStr(r.Mailer)
	f.mailSubject = derefStr(r.Subject)
	f.mailTo = derefInt32(r.To)
	f.mailCC = derefInt32(r.CC)
	f.mailBCC = derefInt32(r.BCC)
	f.mailAttachments = derefInt32(r.Attachments)
	f.mailFailed = derefBool(r.Failed)
	f.notifChannel = derefStr(r.Channel)
	f.userDetailID = derefStr(r.UserDetailID)
	f.userDetailName = derefStr(r.UserDetailName)
	f.userDetailUsername = derefStr(r.UserDetailUsername)
	return f
}

func extractRecords(raw string) ([]json.RawMessage, error) {
	var envelope agentBatch
	if err := json.Unmarshal([]byte(raw), &envelope); err == nil && len(envelope.Records) > 0 {
		return envelope.Records, nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &arr); err == nil {
		return arr, nil
	}
	var obj json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err == nil {
		return []json.RawMessage{obj}, nil
	}
	return nil, fmt.Errorf("unrecognised payload format")
}

// ─── SQL ──────────────────────────────────────────────────────────────────────

const insertSQL = `INSERT INTO telemetry_events (
	stream_id, project_id, received_at,
	event_type, trace_id, event_timestamp,
	server, deploy, event_group,
	execution_source, execution_id, execution_preview, execution_stage, user_id,
	cnt_exceptions, cnt_logs, cnt_queries, cnt_lazy_loads, cnt_jobs_queued,
	cnt_mail, cnt_notifications, cnt_outgoing_requests, cnt_files_read, cnt_files_written,
	cnt_cache_events, cnt_hydrated_models, peak_memory_usage,
	exception_preview, context,
	req_method, req_url, req_route_name, req_route_methods,
	req_route_domain, req_route_path, req_route_action,
	req_ip, req_duration, req_status_code, req_request_size, req_response_size,
	req_bootstrap, req_before_middleware, req_action, req_render,
	req_after_middleware, req_sending, req_terminating,
	req_headers, req_payload,
	qry_sql, qry_file, qry_line, qry_connection, qry_connection_type,
	cache_store, cache_key, cache_type, cache_ttl,
	exc_class, exc_message, exc_code, exc_trace, exc_handled,
	exc_php_version, exc_laravel_version,
	log_level, log_extra,
	out_host, out_method, out_url, out_duration, out_request_size, out_response_size, out_status_code,
	job_id, job_attempt_id, job_attempt, job_name, job_queue, job_status, job_connection, job_duration,
	cmd_command, cmd_exit_code,
	sched_cron, sched_timezone, sched_repeat_seconds,
	sched_without_overlapping, sched_on_one_server, sched_run_in_background, sched_even_in_maintenance_mode,
	mail_mailer, mail_subject, mail_to, mail_cc, mail_bcc, mail_attachments, mail_failed,
	notif_channel,
	user_detail_id, user_detail_name, user_detail_username,
	schema_version,
	raw_payload
)`

// ensureTable creates the wide, Nullable-free telemetry_events table.
func ensureTable(ctx context.Context, ch clickhouse.Conn, db string) error {
	return ch.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.telemetry_events (

			-- envelope / shared
			stream_id          String,
			project_id         String,
			received_at        DateTime64(3, 'UTC'),
			event_type         LowCardinality(String),
			trace_id           UUID,
			event_timestamp    DateTime64(6, 'UTC'),
			server             LowCardinality(String),
			deploy             String,
			event_group        String,
			execution_source   LowCardinality(String),
			execution_id       UUID,
			execution_preview  String,
			execution_stage    LowCardinality(String),
			user_id            String,

			-- summary counters (shared by request / command / job / scheduled-task)
			cnt_exceptions        Int32   DEFAULT 0,
			cnt_logs              Int32   DEFAULT 0,
			cnt_queries           Int32   DEFAULT 0,
			cnt_lazy_loads        Int32   DEFAULT 0,
			cnt_jobs_queued       Int32   DEFAULT 0,
			cnt_mail              Int32   DEFAULT 0,
			cnt_notifications     Int32   DEFAULT 0,
			cnt_outgoing_requests Int32   DEFAULT 0,
			cnt_files_read        Int32   DEFAULT 0,
			cnt_files_written     Int32   DEFAULT 0,
			cnt_cache_events      Int32   DEFAULT 0,
			cnt_hydrated_models   Int32   DEFAULT 0,
			peak_memory_usage     Int32   DEFAULT 0,
			exception_preview     String  DEFAULT '',
			context               String  DEFAULT '',

			-- request
			req_method            LowCardinality(String) DEFAULT '',
			req_url               String  DEFAULT '',
			req_route_name        String  DEFAULT '',
			req_route_methods     String  DEFAULT '',
			req_route_domain      String  DEFAULT '',
			req_route_path        String  DEFAULT '',
			req_route_action      String  DEFAULT '',
			req_ip                String  DEFAULT '',
			req_duration          Int64   DEFAULT 0,
			req_status_code       Int32   DEFAULT 0,
			req_request_size      Int64   DEFAULT 0,
			req_response_size     Int64   DEFAULT 0,
			req_bootstrap         Int64   DEFAULT 0,
			req_before_middleware  Int64   DEFAULT 0,
			req_action            Int64   DEFAULT 0,
			req_render            Int64   DEFAULT 0,
			req_after_middleware   Int64   DEFAULT 0,
			req_sending           Int64   DEFAULT 0,
			req_terminating       Int64   DEFAULT 0,
			req_headers           String  DEFAULT '',
			req_payload           String  DEFAULT '',

			-- query
			qry_sql               String  DEFAULT '',
			qry_file              String  DEFAULT '',
			qry_line              Int32   DEFAULT 0,
			qry_connection        LowCardinality(String) DEFAULT '',
			qry_connection_type   LowCardinality(String) DEFAULT '',

			-- cache-event
			cache_store           LowCardinality(String) DEFAULT '',
			cache_key             String  DEFAULT '',
			cache_type            LowCardinality(String) DEFAULT '',
			cache_ttl             Int32   DEFAULT 0,

			-- exception
			exc_class             String  DEFAULT '',
			exc_message           String  DEFAULT '',
			exc_code              String  DEFAULT '',
			exc_trace             String  DEFAULT '',
			exc_handled           UInt8   DEFAULT 0,
			exc_php_version       LowCardinality(String) DEFAULT '',
			exc_laravel_version   LowCardinality(String) DEFAULT '',

			-- log
			log_level             LowCardinality(String) DEFAULT '',
			log_extra             String  DEFAULT '',

			-- outgoing-request
			out_host              LowCardinality(String) DEFAULT '',
			out_method            LowCardinality(String) DEFAULT '',
			out_url               String  DEFAULT '',
			out_duration          Int64   DEFAULT 0,
			out_request_size      Int64   DEFAULT 0,
			out_response_size     Int64   DEFAULT 0,
			out_status_code       Int32   DEFAULT 0,

			-- queued-job / job-attempt
			job_id                String  DEFAULT '',
			job_attempt_id        String  DEFAULT '',
			job_attempt           Int32   DEFAULT 0,
			job_name              String  DEFAULT '',
			job_queue             LowCardinality(String) DEFAULT '',
			job_status            LowCardinality(String) DEFAULT '',
			job_connection        LowCardinality(String) DEFAULT '',
			job_duration          Int64   DEFAULT 0,

			-- command
			cmd_command           String  DEFAULT '',
			cmd_exit_code         Int32   DEFAULT 0,

			-- scheduled-task
			sched_cron                     String  DEFAULT '',
			sched_timezone                 LowCardinality(String) DEFAULT '',
			sched_repeat_seconds           Int32   DEFAULT 0,
			sched_without_overlapping      UInt8   DEFAULT 0,
			sched_on_one_server            UInt8   DEFAULT 0,
			sched_run_in_background        UInt8   DEFAULT 0,
			sched_even_in_maintenance_mode UInt8   DEFAULT 0,

			-- mail
			mail_mailer           LowCardinality(String) DEFAULT '',
			mail_subject          String  DEFAULT '',
			mail_to               Int32   DEFAULT 0,
			mail_cc               Int32   DEFAULT 0,
			mail_bcc              Int32   DEFAULT 0,
			mail_attachments      Int32   DEFAULT 0,
			mail_failed           UInt8   DEFAULT 0,

			-- notification
			notif_channel         LowCardinality(String) DEFAULT '',

			-- user (UserSensor — fired when authenticated user is resolved)
			user_detail_id        String  DEFAULT '',
			user_detail_name      String  DEFAULT '',
			user_detail_username  String  DEFAULT '',

			-- schema version (v field from sensor payload)
			schema_version        Int32   DEFAULT 0,

			-- always keep the full raw record for forward-compat
			raw_payload           String

		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(event_timestamp)
		ORDER BY (project_id, event_type, event_timestamp, trace_id)
		TTL received_at + INTERVAL 90 DAY
		SETTINGS index_granularity = 8192
	`, db))
}
