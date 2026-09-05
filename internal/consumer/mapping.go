package consumer

import (
	"encoding/json"
	"fmt"
	"strings"
)

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
