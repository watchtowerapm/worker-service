package consumer

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExtractRecordsEnvelope(t *testing.T) {
	raw := `{"records":[{"t":"request","v":1},{"t":"query"}]}`
	got, err := extractRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
}

func TestExtractRecordsArray(t *testing.T) {
	raw := `[{"t":"log"},{"t":"mail"}]`
	got, err := extractRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
}

func TestExtractRecordsObject(t *testing.T) {
	raw := `{"t":"exception","message":"x"}`
	got, err := extractRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
}

func TestExtractRecordsInvalid(t *testing.T) {
	if _, err := extractRecords("not-json"); err == nil {
		t.Fatal("expected error")
	}
}

func TestHydrateUserNameNotJobName(t *testing.T) {
	raw := []byte(`{"t":"user","id":"1","name":"Ada","username":"ada"}`)
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	hydrateUserSensorFields(raw, &r)
	if r.UserDetailName == nil || *r.UserDetailName != "Ada" {
		t.Fatalf("user name = %v", r.UserDetailName)
	}
	if r.Name != nil {
		t.Fatalf("job name should be cleared, got %v", r.Name)
	}
}

func TestHydrateConnectionByType(t *testing.T) {
	raw := []byte(`{"t":"query","connection":"mysql"}`)
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	hydrateConnectionByEventType(raw, &r)
	if r.Connection == nil || *r.Connection != "mysql" {
		t.Fatalf("qry connection = %v", r.Connection)
	}
	if r.JobConnection != nil {
		t.Fatalf("job connection should be unset")
	}

	raw = []byte(`{"t":"queued-job","connection":"redis"}`)
	r = record{}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	hydrateConnectionByEventType(raw, &r)
	if r.JobConnection == nil || *r.JobConnection != "redis" {
		t.Fatalf("job connection = %v", r.JobConnection)
	}
}

func TestBuildTelemetryInsertFieldsRequestDoesNotFillOutgoing(t *testing.T) {
	url := "https://app.test/users"
	method := "GET"
	r := record{Type: "request", Method: &method, URL: &url}
	f := buildTelemetryInsertFields(&r)
	if f.reqURL != url || f.outURL != "" {
		t.Fatalf("reqURL=%q outURL=%q", f.reqURL, f.outURL)
	}
}

func TestParseUUIDAndFloatToTime(t *testing.T) {
	if parseUUID("not-a-uuid") == parseUUID("00000000-0000-0000-0000-000000000000") {
		// both nil uuid is expected for invalid; just ensure it doesn't panic
	}
	fallback := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !floatToTime(0, fallback).Equal(fallback) {
		t.Fatal("zero timestamp should use fallback")
	}
	got := floatToTime(1700000000.5, fallback)
	if got.Unix() != 1700000000 {
		t.Fatalf("unix=%d", got.Unix())
	}
}

func TestDerefHelpers(t *testing.T) {
	if derefInt32(nil) != 0 || derefInt64(nil) != 0 || derefStr(nil) != "" || derefBool(nil) != 0 {
		t.Fatal("nil deref")
	}
	b := true
	if derefBool(&b) != 1 {
		t.Fatal("true should be 1")
	}
}
