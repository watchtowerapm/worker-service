package consumer

import (
	"time"

	"github.com/google/uuid"
)

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
