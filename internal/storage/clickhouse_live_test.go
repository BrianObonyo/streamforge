package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/reliability"
)

// Live tests run only when SF_TEST_CLICKHOUSE_URL points at a real server:
//
//	SF_TEST_CLICKHOUSE_URL=http://localhost:8123 go test ./internal/storage -run Live -v
//
// Each test uses its own uniquely named database, dropped afterwards.
func liveCH(t *testing.T) (*ClickHouse, func()) {
	t.Helper()
	url := os.Getenv("SF_TEST_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("SF_TEST_CLICKHOUSE_URL not set")
	}
	db := fmt.Sprintf("sf_test_%d", time.Now().UnixNano())
	ch, err := NewClickHouse(CHConfig{URL: url, Database: db, User: os.Getenv("SF_TEST_CLICKHOUSE_USER"), Password: os.Getenv("SF_TEST_CLICKHOUSE_PASSWORD")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ch.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema against a real server: %v", err)
	}
	return ch, func() { _, _ = ch.do(ctx, "", nil, stringsReader("DROP DATABASE IF EXISTS "+db)) }
}

func TestLiveSchemaIsIdempotent(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	if err := ch.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("second EnsureSchema: %v", err)
	}
}

func TestLiveCheckpointRoundTrip(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	checkpoint := Checkpoint{
		Group: "live-checkpoint", Topic: "events", Partition: 2,
		Offset: 17, Revision: 1, Config: "test-config", State: []byte(`{"next_offset":17}`),
	}
	if err := ch.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint.Offset, checkpoint.Revision, checkpoint.State = 23, 2, []byte(`{"next_offset":23}`)
	if err := ch.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ch.LoadCheckpoint(ctx, checkpoint.Group, checkpoint.Topic, checkpoint.Partition)
	if err != nil || !ok {
		t.Fatalf("load checkpoint: found=%v err=%v", ok, err)
	}
	if got.Offset != checkpoint.Offset || got.Revision != checkpoint.Revision ||
		string(got.State) != string(checkpoint.State) {
		t.Fatalf("latest checkpoint = %+v, want %+v", got, checkpoint)
	}
}

// The central correctness claim of ADR-0002, checked against the real engine:
// before any background merge, three versions of one window coexist on disk, yet
// a FINAL read must return only the fullest one.
func TestLiveHighestVersionWinsBeforeMerge(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	for _, count := range []int64{10, 4, 12, 7} { // separate INSERTs ⇒ separate parts ⇒ no merge yet
		if err := ch.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "s1", count, float64(count)*10)}); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := ch.Summary(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sum) != 1 || sum[0].Events != 12 || sum[0].Windows != 1 {
		t.Fatalf("want one row with 12 events (highest version), got %+v", sum)
	}
	pts, err := ch.Series(ctx, Query{Kind: events.KindIoT})
	if err != nil || len(pts) != 1 || pts[0].Count != 12 || !pts[0].WindowStart.Equal(t0) || pts[0].Avg != 10 {
		t.Fatalf("series: %+v err=%v", pts, err)
	}
}

func TestLiveFiltersAndTimeRange(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	_ = ch.WriteResults(ctx, []aggregation.Result{
		res(0, events.KindIoT, "a", 10, 100), res(10, events.KindIoT, "a", 5, 50),
		res(0, events.KindMarket, "BTC", 2, 20), res(20, events.KindClick, "u1", 3, 30),
	})
	all, _ := ch.Summary(ctx, Query{})
	if len(all) != 3 {
		t.Fatalf("categories=%+v", all)
	}
	one, _ := ch.Summary(ctx, Query{Kind: events.KindIoT})
	if len(one) != 1 || one[0].Events != 15 || one[0].Windows != 2 {
		t.Fatalf("kind filter: %+v", one)
	}
	ranged, _ := ch.Summary(ctx, Query{From: t0.Add(5 * time.Second), To: t0.Add(15 * time.Second)})
	if len(ranged) != 1 || ranged[0].Events != 5 {
		t.Fatalf("range [5s,15s) should hold only the window starting at 10s: %+v", ranged)
	}
}

// Hostile input must be inert: matched as a literal string, never executed.
func TestLiveInjectionAttemptIsInert(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	_ = ch.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "s1", 3, 30)})
	evil := "' OR 1=1; DROP TABLE " + ch.cfg.Database + "." + ch.cfg.Table + " --"
	rows, err := ch.Summary(ctx, Query{Key: evil})
	if err != nil || len(rows) != 0 {
		t.Fatalf("injection matched rows or errored: %+v err=%v", rows, err)
	}
	if rows, _ := ch.Summary(ctx, Query{}); len(rows) != 1 {
		t.Fatal("table damaged by injection attempt")
	}
}

func TestLiveLateStats(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	now := time.Now().UTC()
	if err := ch.WriteLateStats(ctx, []LateStatRow{
		{TS: now, Partition: 0, OnTime: 90, Late: 8, TooLate: 2, MaxLatenessMs: 1500},
		{TS: now, Partition: 1, OnTime: 10, Late: 1, TooLate: 0, MaxLatenessMs: 700},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := ch.LateSummary(ctx, Query{From: now.Add(-time.Minute)})
	if err != nil || got.OnTime != 100 || got.Late != 9 || got.TooLate != 2 || got.MaxLatenessMs != 1500 {
		t.Fatalf("%+v err=%v", got, err)
	}
	empty, err := ch.LateSummary(ctx, Query{From: now.Add(time.Hour)})
	if err != nil || empty != (LateSummary{}) {
		t.Fatalf("empty range: %+v err=%v", empty, err)
	}
}

// A vanished table (operator DROP, fresh volume) must not wedge the pipeline: the
// store recreates the schema and retries once.
func TestLiveSelfHealsMissingTable(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	ctx := context.Background()
	_ = ch.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "a", 1, 1)})
	if _, err := ch.do(ctx, "", nil, stringsReader("DROP TABLE "+ch.cfg.Database+"."+ch.cfg.Table)); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteResults(ctx, []aggregation.Result{res(0, events.KindIoT, "a", 5, 5)}); err != nil {
		t.Fatalf("write after DROP TABLE should self-heal: %v", err)
	}
	if sum, _ := ch.Summary(ctx, Query{}); len(sum) != 1 || sum[0].Events != 5 {
		t.Fatalf("healed table content: %+v", sum)
	}
}

func TestLiveBadRequestIsPermanent(t *testing.T) {
	ch, done := liveCH(t)
	defer done()
	_, err := ch.do(context.Background(), "", nil, stringsReader("SELEKT nonsense"))
	var he *HTTPError
	if err == nil || !errors.As(err, &he) || he.Retryable() || !reliability.IsPermanent(err) {
		t.Fatalf("a syntax error must be a permanent *HTTPError, got %T %v", err, err)
	}
	t.Logf("classified: status=%d code=%d", he.Status, he.Code)
}
