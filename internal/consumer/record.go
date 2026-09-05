package consumer

import "encoding/json"

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
