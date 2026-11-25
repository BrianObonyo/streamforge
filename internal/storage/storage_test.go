package storage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func res(startSec int, kind events.Kind, key string, count int64, sum float64) aggregation.Result {
	w := aggregation.Window{Start: t0.Add(time.Duration(startSec) * time.Second), End: t0.Add(time.Duration(startSec+10) * time.Second)}
	return aggregation.Result{Window: w, Kind: kind, Key: key,
		Stats: aggregation.Stats{Count: count, Sum: sum, Min: 1, Max: 9}, Version: uint64(count)}
}

// The core idempotency guarantee: a fuller aggregate is never overwritten by an
// emptier replay of the same window.
func TestMemStoreHighestVersionWins(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "a", 10, 100)})
	_ = s.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "a", 12, 120)}) // late refinement: must win
	_ = s.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "a", 4, 40)})   // stale replay arrives last: must lose
	rows := s.Rows()
	if len(rows) != 1 || rows[0].Stats.Count != 12 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestMemStoreSummarySeriesFilters(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.WriteResults(ctx, []aggregation.Result{
		res(0, events.KindIoT, "a", 10, 100),
		res(10, events.KindIoT, "a", 5, 50),
		res(0, events.KindMarket, "BTC", 2, 20),
	})
	sum, _ := s.Summary(ctx, Query{})
	if len(sum) != 2 || sum[0].Kind != events.KindIoT || sum[0].Events != 15 || sum[0].Avg != 10 || sum[0].Windows != 2 {
		t.Fatalf("summary=%+v", sum)
	}
	only, _ := s.Summary(ctx, Query{Kind: events.KindMarket})
	if len(only) != 1 || only[0].Events != 2 {
		t.Fatalf("kind filter: %+v", only)
	}
	ser, _ := s.Series(ctx, Query{Kind: events.KindIoT, From: t0.Add(5 * time.Second)})
	if len(ser) != 1 || !ser[0].WindowStart.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("time filter: %+v", ser)
	}
}

func TestMemStoreFailNext(t *testing.T) {
	s := NewMemStore()
	s.FailNext(1)
	if err := s.WriteResults(context.Background(), []aggregation.Result{res(0, events.KindIoT, "a", 1, 1)}); err == nil {
		t.Fatal("expected simulated outage")
	}
	if err := s.WriteResults(context.Background(), []aggregation.Result{res(0, events.KindIoT, "a", 1, 1)}); err != nil {
		t.Fatal(err)
	}
}

func TestMemStoreCheckpointKeepsHighestRevision(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	first := Checkpoint{
		Group: "group", Topic: "events", Partition: 2, Offset: 14,
		Revision: 1, Config: "cfg", State: []byte(`{"next_offset":14}`),
	}
	if err := s.SaveCheckpoint(ctx, first); err != nil {
		t.Fatal(err)
	}
	newer := first
	newer.Offset, newer.Revision, newer.State = 20, 2, []byte(`{"next_offset":20}`)
	if err := s.SaveCheckpoint(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(ctx, first); err == nil {
		t.Fatal("older checkpoint revision was accepted")
	}
	got, ok, err := s.LoadCheckpoint(ctx, "group", "events", 2)
	if err != nil || !ok {
		t.Fatalf("load checkpoint: found=%v err=%v", ok, err)
	}
	if got.Offset != 20 || got.Revision != 2 || string(got.State) != string(newer.State) {
		t.Fatalf("loaded stale checkpoint: %+v", got)
	}
	got.State[0] = 'x'
	again, _, err := s.LoadCheckpoint(ctx, "group", "events", 2)
	if err != nil || string(again.State) != string(newer.State) {
		t.Fatalf("checkpoint state was not copied on load: %+v err=%v", again, err)
	}
}

func TestMemStoreLateSummary(t *testing.T) {
	s := NewMemStore()
	_ = s.WriteLateStats(context.Background(), []LateStatRow{
		{TS: t0, OnTime: 10, Late: 2, TooLate: 1, MaxLatenessMs: 500},
		{TS: t0.Add(time.Minute), OnTime: 5, Late: 1, MaxLatenessMs: 900},
	})
	got, _ := s.LateSummary(context.Background(), Query{})
	if got.OnTime != 15 || got.Late != 3 || got.TooLate != 1 || got.MaxLatenessMs != 900 {
		t.Fatalf("%+v", got)
	}
}

// ---------------------------------------------------------------- ClickHouse

func TestClickHouseRejectsUnsafeIdentifiers(t *testing.T) {
	for _, bad := range []string{"a;DROP TABLE x", "a b", "1abc", "a-b", ""} {
		if bad == "" {
			continue // empty means "use default"
		}
		if _, err := NewClickHouse(CHConfig{URL: "http://x", Table: bad}); err == nil {
			t.Errorf("identifier %q accepted", bad)
		}
	}
}

func TestClickHouseInsertWireFormat(t *testing.T) {
	var gotQuery, gotBody, gotUser string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotQuery, gotBody, gotUser = r.URL.Query().Get("query"), string(b), r.Header.Get("X-ClickHouse-User")
	}))
	defer srv.Close()

	ch, _ := NewClickHouse(CHConfig{URL: srv.URL, User: "sf", Password: "pw"})
	err := ch.WriteResults(context.Background(), []aggregation.Result{res(0, events.KindIoT, "sensor-1", 3, 30)})
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "INSERT INTO streamforge.window_results FORMAT JSONEachRow" {
		t.Errorf("query=%q", gotQuery)
	}
	for _, want := range []string{`"kind":"iot"`, `"key":"sensor-1"`, `"event_count":3`, `"version":3`, `"window_start":"2026-11-14 10:00:00.000"`} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("body missing %s: %s", want, gotBody)
		}
	}
	if gotUser != "sf" {
		t.Errorf("auth header not sent")
	}
}

// Security property: client-supplied values must travel as query PARAMETERS and
// never appear inside the SQL text.
func TestClickHouseSchemaIncludesCheckpointTable(t *testing.T) {
	var statements []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		statements = append(statements, string(body))
	}))
	defer srv.Close()
	ch, err := NewClickHouse(CHConfig{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, statement := range statements {
		if strings.Contains(statement, "processor_checkpoints") &&
			strings.Contains(statement, "ReplacingMergeTree(revision)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("checkpoint table DDL missing: %v", statements)
	}
}

func TestClickHouseCheckpointReadWrite(t *testing.T) {
	var insertBody, selectSQL string
	var selectParams map[string][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if query := r.URL.Query().Get("query"); strings.HasPrefix(query, "INSERT") {
			body, _ := io.ReadAll(r.Body)
			insertBody = string(body)
			return
		}
		body, _ := io.ReadAll(r.Body)
		selectSQL, selectParams = string(body), r.URL.Query()
		_, _ = io.WriteString(w, `{"consumer_group":"processors","topic":"events","partition":2,"checkpoint_offset":17,"revision":3,"config":"fingerprint","state":"{\"next_offset\":17}"}`+"\n")
	}))
	defer srv.Close()
	ch, err := NewClickHouse(CHConfig{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{
		Group: "processors", Topic: "events", Partition: 2, Offset: 17,
		Revision: 3, Config: "fingerprint", State: []byte(`{"next_offset":17}`),
	}
	if err := ch.SaveCheckpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	var inserted map[string]any
	if err := json.Unmarshal([]byte(insertBody), &inserted); err != nil {
		t.Fatalf("invalid checkpoint INSERT body %q: %v", insertBody, err)
	}
	if inserted["consumer_group"] != cp.Group || inserted["state"] != string(cp.State) {
		t.Fatalf("unexpected checkpoint INSERT: %+v", inserted)
	}
	got, ok, err := ch.LoadCheckpoint(context.Background(), cp.Group, cp.Topic, cp.Partition)
	if err != nil || !ok {
		t.Fatalf("load checkpoint: found=%v err=%v", ok, err)
	}
	if got.Offset != cp.Offset || got.Revision != cp.Revision || string(got.State) != string(cp.State) {
		t.Fatalf("loaded checkpoint differs: %+v", got)
	}
	if !strings.Contains(selectSQL, "FINAL") || selectParams["param_group"][0] != cp.Group ||
		selectParams["param_topic"][0] != cp.Topic ||
		selectParams["param_partition"][0] != "2" {
		t.Fatalf("checkpoint keys were not parameterized: %+v", selectParams)
	}
}

func TestClickHouseQueriesAreParameterised(t *testing.T) {
	var sql string
	var params map[string][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sql, params = string(b), r.URL.Query()
		_, _ = io.WriteString(w, `{"kind":"iot","windows":2,"events":15,"sum":150,"min":1,"max":9,"avg":10}`+"\n")
	}))
	defer srv.Close()

	ch, _ := NewClickHouse(CHConfig{URL: srv.URL})
	evil := "x' OR 1=1; DROP TABLE streamforge.window_results --"
	rows, err := ch.Summary(context.Background(), Query{Kind: events.KindIoT, Key: evil, From: t0, To: t0.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "DROP") || strings.Contains(sql, "OR 1=1") {
		t.Fatalf("user input leaked into SQL: %s", sql)
	}
	if params["param_key"][0] != evil {
		t.Errorf("key not passed as param: %v", params)
	}
	if !strings.Contains(sql, "FINAL") {
		t.Error("reads must use FINAL to collapse unmerged duplicates")
	}
	if len(rows) != 1 || rows[0].Events != 15 || rows[0].Kind != events.KindIoT {
		t.Fatalf("decoded: %+v", rows)
	}
}

func TestClickHouseErrorSurfaced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Code: 60. Table doesn't exist", http.StatusNotFound)
	}))
	defer srv.Close()
	ch, _ := NewClickHouse(CHConfig{URL: srv.URL})
	err := ch.WriteResults(context.Background(), []aggregation.Result{res(0, events.KindIoT, "a", 1, 1)})
	if err == nil || !strings.Contains(err.Error(), "Table doesn't exist") {
		t.Fatalf("err=%v", err)
	}
}

func TestClickHouseSeriesDecodesTimes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ws":"2026-11-14 10:00:00.000","we":"2026-11-14 10:00:10.000","kind":"iot","key":"a","count":7,"avg":2.5,"min":1,"max":4}`+"\n")
	}))
	defer srv.Close()
	ch, _ := NewClickHouse(CHConfig{URL: srv.URL})
	pts, err := ch.Series(context.Background(), Query{})
	if err != nil || len(pts) != 1 || !pts[0].WindowStart.Equal(t0) || pts[0].Count != 7 {
		t.Fatalf("pts=%+v err=%v", pts, err)
	}
}

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }
