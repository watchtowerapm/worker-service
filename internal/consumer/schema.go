package consumer

import (
	"context"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

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
		TTL toDateTime(received_at) + INTERVAL 90 DAY
		SETTINGS index_granularity = 8192
	`, db))
}
