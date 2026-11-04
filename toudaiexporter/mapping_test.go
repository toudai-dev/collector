package toudaiexporter

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestDoubleText(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want string
	}{
		{"zero", 0, "0.0"},
		{"negative zero", math.Copysign(0, -1), "-0.0"},
		{"integral", 1.0, "1.0"},
		{"fraction", 1.5, "1.5"},
		{"small exponent", 1e-7, "1e-07"},
		{"large exponent", 1e21, "1e+21"},
		{"nan", math.NaN(), "nan"},
		{"positive infinity", math.Inf(1), "inf"},
		{"negative infinity", math.Inf(-1), "-inf"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := doubleText(tc.in); got != tc.want {
				t.Fatalf("doubleText(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCanonicalNodeTypes(t *testing.T) {
	tests := []struct {
		name string
		in   value
		want string
	}{
		{"int", value{kind: kindInt, i: 9007199254740993}, `{"t":"int","v":"9007199254740993"}`},
		{"int min", value{kind: kindInt, i: math.MinInt64}, `{"t":"int","v":"-9223372036854775808"}`},
		{"string one", value{kind: kindString, s: "1"}, `{"t":"string","v":"1"}`},
		{"empty string", value{kind: kindString, s: ""}, `{"t":"string","v":""}`},
		{"double one", value{kind: kindDouble, d: 1}, `{"t":"double","v":"1.0"}`},
		{"bool false", value{kind: kindBool, b: false}, `{"t":"bool","v":false}`},
		{"empty", value{kind: kindEmpty}, `{"t":"empty"}`},
		{"bytes", value{kind: kindBytes, y: []byte{0, 1, 0xfe, 0xff}}, `{"t":"bytes","v":"AAH+/w=="}`},
		{"array", value{kind: kindArray, arr: []value{
			{kind: kindString, s: "a"}, {kind: kindInt, i: 1}, {kind: kindBool, b: true},
		}}, `{"t":"array","v":[{"t":"string","v":"a"},{"t":"int","v":"1"},{"t":"bool","v":true}]}`},
		{"map", value{kind: kindMap, m: map[string]value{
			"nested.bool": {kind: kindBool, b: false},
			"nested.int":  {kind: kindInt, i: 2},
		}}, `{"t":"map","v":{"nested.bool":{"t":"bool","v":false},"nested.int":{"t":"int","v":"2"}}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(canonicalNode(tc.in))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("canonicalNode = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRecordMapsRouteEachKindOnce(t *testing.T) {
	tests := []struct {
		name      string
		attr      value
		wantMap   string
		wantValue string
	}{
		{"string", value{kind: kindString, s: "1"}, "string", "1"},
		{"int", value{kind: kindInt, i: 1}, "int", "1"},
		{"double", value{kind: kindDouble, d: 1}, "double", "1"},
		{"bool", value{kind: kindBool, b: false}, "bool", "false"},
		{"bytes", value{kind: kindBytes, y: []byte{0}}, "other", "bytes"},
		{"array", value{kind: kindArray, arr: []value{{kind: kindInt, i: 1}}}, "other", "array"},
		{"map", value{kind: kindMap, m: map[string]value{}}, "other", "map"},
		{"empty", value{kind: kindEmpty}, "other", "empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			maps := newRecordMaps()
			if err := maps.place("k", tc.attr); err != nil {
				t.Fatalf("place: %v", err)
			}
			locations := 0
			if _, ok := maps.ints["k"]; ok {
				locations++
				if tc.wantMap != "int" {
					t.Fatalf("key landed in int map, want %s", tc.wantMap)
				} else if got := maps.ints["k"]; got != tc.attr.i {
					t.Fatalf("int value %d, want %d", got, tc.attr.i)
				}
			}
			if _, ok := maps.doubles["k"]; ok {
				locations++
			}
			if _, ok := maps.strs["k"]; ok {
				locations++
			}
			if _, ok := maps.bools["k"]; ok {
				locations++
			}
			if got, ok := maps.others["k"]; ok {
				locations++
				if tc.wantMap == "other" && got != tc.wantValue {
					t.Fatalf("other value %q, want %q", got, tc.wantValue)
				}
			}
			if locations != 1 {
				t.Fatalf("key present in %d maps, want exactly one", locations)
			}
		})
	}
}

// fixtureEventID is a canonical UUIDv7 standing in for the gateway-assigned ID.
const fixtureEventID = "0198a0c4-7b3e-7c11-8a52-3f6d1e9b2a40"

// fixtureLogs builds one log record with the trusted gateway scope and the
// supplied record attributes.
func fixtureLogs(t *testing.T, record func(plog.LogRecord)) plog.Logs {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	res := rl.Resource().Attributes()
	res.PutStr(datasetAttr, "fixture")
	res.PutStr("service.name", "fixture-service")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("toudai.fixture")
	sl.Scope().Attributes().PutStr("scope.attr", "scope-value")
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr(eventIDAttr, fixtureEventID)
	lr.SetTimestamp(pcommon.Timestamp(1788246000123456789))
	lr.SetObservedTimestamp(pcommon.Timestamp(1788246000123456789 + 1_000_000))
	lr.Body().SetStr("body")
	if record != nil {
		record(lr)
	}
	return ld
}

// Duplicate attribute keys cannot be built through pcommon.Map's Go API (PutStr
// upserts), so the duplicate-key rejection is exercised in the integration
// harness with real OTLP protobuf bytes carrying two entries for one key.
// decodeAttrs detects the duplicate while ranging the collection.

// A resource without a usable toudai.dataset means the gateway processor did not
// run, so the batch fails closed. The workspace attribute of the pre-Decision
// 0002 gateway no longer stands in for it.
func TestPrepareLogsRejectsMissingTrustedScope(t *testing.T) {
	tests := []struct {
		name     string
		resource func(pcommon.Map)
	}{
		{"no dataset", func(pcommon.Map) {}},
		{"empty dataset", func(m pcommon.Map) { m.PutStr(datasetAttr, "") }},
		{"non-string dataset", func(m pcommon.Map) { m.PutInt(datasetAttr, 1) }},
		{"legacy workspace scope only", func(m pcommon.Map) {
			m.PutStr("toudai.workspace_id", "11111111-1111-4111-8111-111111111111")
			m.PutStr("toudai.scope_source", "gateway")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld := fixtureLogs(t, nil)
			res := ld.ResourceLogs().At(0).Resource().Attributes()
			res.Remove(datasetAttr)
			tc.resource(res)
			prepared := prepareLogs(ld)
			if len(prepared.rows) != 0 {
				t.Fatalf("rows = %d, want 0", len(prepared.rows))
			}
			if reason, err := batchInvalidation(prepared.rejections); err == nil || reason != "missing_trusted_scope" {
				t.Fatalf("batchInvalidation = %q, %v, want missing_trusted_scope", reason, err)
			}
		})
	}
}

func TestPrepareLogsDropsProducerReservedKeys(t *testing.T) {
	ld := fixtureLogs(t, func(lr plog.LogRecord) {
		lr.Attributes().PutStr("toudai.dataset", "forged-record")
		lr.Attributes().PutStr("normal", "value")
	})
	rl := ld.ResourceLogs().At(0)
	rl.ScopeLogs().At(0).Scope().Attributes().PutStr("toudai.dataset", "forged-scope")
	// Reserved resource keys other than the dataset, such as the retired
	// workspace and scope-marker attributes, are never stored.
	rl.Resource().Attributes().PutStr("toudai.workspace_id", "99999999-9999-4999-8999-999999999999")
	rl.Resource().Attributes().PutStr("toudai.scope_source", "gateway")
	prepared := prepareLogs(ld)
	if len(prepared.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(prepared.rows))
	}
	row := prepared.rows[0]
	if row.Dataset != "fixture" || prepared.canonical[0].Dataset != "fixture" {
		t.Fatalf("dataset = %q / %q, want the gateway value", row.Dataset, prepared.canonical[0].Dataset)
	}
	if _, ok := row.RecordString["toudai.dataset"]; ok {
		t.Fatal("producer record attribute toudai.dataset must be dropped")
	}
	if _, ok := row.Scope["toudai.dataset"]; ok {
		t.Fatal("producer scope attribute toudai.dataset must be dropped")
	}
	if row.Resource["toudai.dataset"] != "fixture" {
		t.Fatal("stored resource namespace must keep the trusted dataset")
	}
	for _, k := range []string{"toudai.workspace_id", "toudai.scope_source"} {
		if _, ok := row.Resource[k]; ok {
			t.Fatalf("resource %s must not be stored", k)
		}
	}
	if !strings.Contains(row.Payload, `"toudai.dataset":{"t":"string","v":"fixture"}`) {
		t.Fatal("canonical resource namespace must carry the trusted dataset")
	}
	for _, forged := range []string{"forged-record", "forged-scope", "9999999999", "workspace", "scope_source"} {
		if strings.Contains(row.Payload, forged) {
			t.Fatalf("canonical payload must not contain %q", forged)
		}
	}
}

// A span whose timing is missing or reversed is accepted and preserved: both
// supplied timestamps are stored, the derived duration is unavailable, and the
// timing diagnostic distinguishes missing timing (either timestamp unset, which
// OTLP encodes as 0) from a reversed span and from a valid zero duration.
func TestSpanTimingStatusAndPreservedTimestamps(t *testing.T) {
	build := func(start, end uint64) preparedSpans {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		res := rs.Resource().Attributes()
		res.PutStr(datasetAttr, "fixture")
		ss := rs.ScopeSpans().AppendEmpty()
		sp := ss.Spans().AppendEmpty()
		sp.Attributes().PutStr(eventIDAttr, fixtureEventID)
		sp.SetStartTimestamp(pcommon.Timestamp(start))
		sp.SetEndTimestamp(pcommon.Timestamp(end))
		sp.SetName("span")
		return prepareSpans(td)
	}
	tests := []struct {
		name       string
		start, end uint64
		wantStatus string
		wantDur    *int64
	}{
		{"positive duration", 10, 20, spanTimingOK, ptr(int64(10))},
		{"zero duration", 10, 10, spanTimingOK, ptr(int64(0))},
		{"missing timing", 0, 0, spanTimingMissing, nil},
		// An unset timestamp is 0 in OTLP and either side being unset makes the
		// duration unobservable; missing timing takes precedence over the
		// reversed classification, so a missing start is not a duration from
		// epoch 0 and a missing end is not read as end < start.
		{"missing start", 0, 20, spanTimingMissing, nil},
		{"missing end", 10, 0, spanTimingMissing, nil},
		{"reversed timing", 20, 10, spanTimingEndBeforeStart, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := build(tc.start, tc.end)
			if len(got.rows) != 1 || len(got.rejections) != 0 {
				t.Fatalf("rows = %d, rejections = %+v; a span must be preserved, not rejected",
					len(got.rows), got.rejections)
			}
			row := got.rows[0]
			if row.TimingStatus != tc.wantStatus {
				t.Fatalf("timing status = %q, want %q", row.TimingStatus, tc.wantStatus)
			}
			if got := uint64(row.TS.UnixNano()); got != tc.start {
				t.Fatalf("stored start = %d, want %d", got, tc.start)
			}
			if got := uint64(row.EndTS.UnixNano()); got != tc.end {
				t.Fatalf("stored end = %d, want %d", got, tc.end)
			}
			switch {
			case tc.wantDur == nil && row.DurationNS != nil:
				t.Fatalf("duration = %d, want NULL", *row.DurationNS)
			case tc.wantDur != nil && row.DurationNS == nil:
				t.Fatalf("duration = NULL, want %d", *tc.wantDur)
			case tc.wantDur != nil && *row.DurationNS != *tc.wantDur:
				t.Fatalf("duration = %d, want %d", *row.DurationNS, *tc.wantDur)
			}
			// The canonical payload keeps the supplied timestamps and carries no
			// derived duration when the duration is not observable.
			var doc map[string]any
			if err := json.Unmarshal([]byte(row.Payload), &doc); err != nil {
				t.Fatalf("payload is not JSON: %v", err)
			}
			span := doc["span"].(map[string]any)
			if span["start_ns"] != strconv.FormatUint(tc.start, 10) ||
				span["end_ns"] != strconv.FormatUint(tc.end, 10) {
				t.Fatalf("canonical start/end = %v/%v, want %d/%d",
					span["start_ns"], span["end_ns"], tc.start, tc.end)
			}
			_, hasDuration := span["duration_ns"]
			if hasDuration != (tc.wantDur != nil) {
				t.Fatalf("canonical duration_ns present = %v, want %v", hasDuration, tc.wantDur != nil)
			}
		})
	}
}

// The exporter stores the ID the gateway assigned and never derives one: two
// records with byte-identical content but different assigned IDs (a producer
// retry, or two legitimate identical events) keep distinct identities, and the
// same assigned ID is reproduced exactly.
func TestEventIDIsTheAssignedOne(t *testing.T) {
	const other = "0198a0c4-7b3e-7c11-8a52-3f6d1e9b2a41"
	build := func(id string) preparedLogs {
		return prepareLogs(fixtureLogs(t, func(lr plog.LogRecord) {
			lr.Attributes().PutStr(eventIDAttr, id)
			lr.Attributes().PutInt("metric.value", 1)
		}))
	}
	a, b := build(fixtureEventID), build(other)
	if len(a.rows) != 1 || len(b.rows) != 1 {
		t.Fatalf("expected one row per build, got %d and %d", len(a.rows), len(b.rows))
	}
	if a.rows[0].EventID != fixtureEventID || a.canonical[0].EventID != fixtureEventID {
		t.Fatalf("serving/canonical event IDs = %q/%q, want the assigned %q", a.rows[0].EventID, a.canonical[0].EventID, fixtureEventID)
	}
	if b.rows[0].EventID != other {
		t.Fatalf("event ID = %q, want the assigned %q", b.rows[0].EventID, other)
	}
	// Content is identical, so only the assigned ID can explain the difference.
	if a.rows[0].PayloadSHA256 == b.rows[0].PayloadSHA256 {
		t.Fatal("payload checksum ignored the assigned event ID")
	}
	if strings.Contains(a.rows[0].Payload, `"toudai.event_id"`) {
		t.Fatal("the event ID attribute must be stored as identity only, not as a record attribute")
	}
	if _, ok := a.rows[0].RecordString[eventIDAttr]; ok {
		t.Fatal("the event ID attribute must not be stored in the record Maps")
	}
}

func TestEventIDValidation(t *testing.T) {
	tests := []struct {
		name       string
		set        func(plog.LogRecord)
		wantReason string
	}{
		{"missing", func(lr plog.LogRecord) { lr.Attributes().Remove(eventIDAttr) }, "missing_event_id"},
		{"empty", func(lr plog.LogRecord) { lr.Attributes().PutStr(eventIDAttr, "") }, "missing_event_id"},
		{"not a string", func(lr plog.LogRecord) { lr.Attributes().PutInt(eventIDAttr, 7) }, "missing_event_id"},
		{"content hash", func(lr plog.LogRecord) { lr.Attributes().PutStr(eventIDAttr, "lh1-0123456789abcdef0123456789abcdef") }, "invalid_event_id"},
		{"uuid v4", func(lr plog.LogRecord) { lr.Attributes().PutStr(eventIDAttr, "9d6b2f0e-7d1c-4a55-8f3e-1c2b3a4d5e6f") }, "invalid_event_id"},
		{"uppercase uuid v7", func(lr plog.LogRecord) { lr.Attributes().PutStr(eventIDAttr, strings.ToUpper(fixtureEventID)) }, "invalid_event_id"},
		{"unhyphenated uuid v7", func(lr plog.LogRecord) {
			lr.Attributes().PutStr(eventIDAttr, strings.ReplaceAll(fixtureEventID, "-", ""))
		}, "invalid_event_id"},
		{"urn uuid v7", func(lr plog.LogRecord) { lr.Attributes().PutStr(eventIDAttr, "urn:uuid:"+fixtureEventID) }, "invalid_event_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prepared := prepareLogs(fixtureLogs(t, tc.set))
			if len(prepared.rows) != 0 || len(prepared.canonical) != 0 {
				t.Fatalf("rows/canonical = %d/%d, want the record rejected", len(prepared.rows), len(prepared.canonical))
			}
			if len(prepared.rejections) != 1 || prepared.rejections[0].Reason != tc.wantReason {
				t.Fatalf("rejections = %+v, want one %s", prepared.rejections, tc.wantReason)
			}
			// A bad ID means the gateway processor is misconfigured, so the whole
			// export fails closed rather than writing its other records.
			if reason, err := batchInvalidation(prepared.rejections); err == nil || reason != tc.wantReason {
				t.Fatalf("batchInvalidation = %q, %v, want %s", reason, err, tc.wantReason)
			}
		})
	}
}

func TestSpansRequireAssignedEventID(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	res := rs.Resource().Attributes()
	res.PutStr(datasetAttr, "fixture")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	spans.AppendEmpty().Attributes().PutStr(eventIDAttr, fixtureEventID)
	spans.AppendEmpty() // no event ID
	prepared := prepareSpans(td)
	if len(prepared.rows) != 1 || prepared.rows[0].EventID != fixtureEventID {
		t.Fatalf("rows = %+v, want only the span with an assigned ID", prepared.rows)
	}
	if reason, err := batchInvalidation(prepared.rejections); err == nil || reason != "missing_event_id" {
		t.Fatalf("batchInvalidation = %q, %v, want missing_event_id", reason, err)
	}
}

func TestCanonicalPayloadMatchesIndependentShape(t *testing.T) {
	prepared := prepareLogs(fixtureLogs(t, func(lr plog.LogRecord) {
		lr.Attributes().PutInt("metric.value", 1)
		lr.Attributes().PutDouble("zero.float", math.Copysign(0, -1))
		lr.Attributes().PutEmptyBytes("bytes.raw").FromRaw([]byte{0, 1})
	}))
	var doc map[string]any
	if err := json.Unmarshal([]byte(prepared.rows[0].Payload), &doc); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	identity := doc["identity"].(map[string]any)
	// Decision 0002 D2: the identity block is dataset plus event ID, nothing else.
	if len(identity) != 2 || identity["dataset"] != "fixture" {
		t.Fatalf("identity = %v", identity)
	}
	if identity["event_id"] != prepared.rows[0].EventID {
		t.Fatal("payload identity.event_id does not match the row identity")
	}
	record := doc["record"].(map[string]any)
	if got := record["metric.value"].(map[string]any); got["t"] != "int" || got["v"] != "1" {
		t.Fatalf("metric.value node = %v", got)
	}
	if got := record["zero.float"].(map[string]any); got["t"] != "double" || got["v"] != "-0.0" {
		t.Fatalf("zero.float node = %v", got)
	}
	if got := record["bytes.raw"].(map[string]any); got["t"] != "bytes" || got["v"] != "AAE=" {
		t.Fatalf("bytes.raw node = %v", got)
	}
	if doc["schema"] != canonicalSchema || doc["kind"] != "log" {
		t.Fatalf("schema/kind = %v/%v", doc["schema"], doc["kind"])
	}
}

func ptr[T any](v T) *T { return &v }
