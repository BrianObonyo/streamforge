package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
)

func TestRunIsRepeatableAndRetriesInjectedSinkFailure(t *testing.T) {
	base := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	input := bytes.NewBuffer(nil)
	for _, ev := range []events.Event{
		{ID: "one", Kind: events.KindIoT, Key: "sensor-a", Value: 12, EventTime: base},
		{ID: "two", Kind: events.KindIoT, Key: "sensor-a", Value: 18, EventTime: base.Add(500 * time.Millisecond)},
		{ID: "one", Kind: events.KindIoT, Key: "sensor-a", Value: 12, EventTime: base},
		{ID: "three", Kind: events.KindIoT, Key: "sensor-a", Value: 15, EventTime: base.Add(time.Second)},
	} {
		b, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(b)
		input.WriteByte('\n')
	}

	cfg := Config{
		Partitions: 3, WindowSize: time.Second, MaxOutOfOrder: 0,
		AllowedLateness: 0, DedupMax: 100, ClockStart: base,
		ArrivalStep: time.Millisecond, SinkFailures: 1, SinkRetries: 1,
	}
	first, err := Run(context.Background(), bytes.NewReader(input.Bytes()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), bytes.NewReader(input.Bytes()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("same input and configuration produced different reports:\n%s\n%s", firstJSON, secondJSON)
	}
	if first.Records != 4 || first.Valid != 4 || first.Duplicates != 1 {
		t.Fatalf("unexpected input counters: %+v", first)
	}
	if first.SinkFailures != 1 || first.SinkAttempts != 2 {
		t.Fatalf("injected sink failure was not retried deterministically: %+v", first)
	}
	var total int64
	for _, summary := range first.Summaries {
		total += summary.Events
	}
	if total != 3 {
		t.Fatalf("stored event total = %d, want 3", total)
	}
}

func TestRunClassifiesMalformedAndInvalidInput(t *testing.T) {
	base := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	ev := events.Event{
		ID: "invalid-kind", Kind: "unsupported", Key: "key", Value: 1, EventTime: base,
	}
	encoded, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	input := append([]byte("not json\n"), encoded...)
	input = append(input, '\n')
	report, err := Run(context.Background(), bytes.NewReader(input), Config{
		Partitions: 1, WindowSize: time.Second, ArrivalStep: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 2 || report.Poison != 1 || report.Invalid != 1 || report.Valid != 0 {
		t.Fatalf("unexpected input classification: %+v", report)
	}
	if len(report.Rows) != 0 || len(report.Summaries) != 0 {
		t.Fatalf("invalid input produced aggregate rows: %+v", report.Rows)
	}
}

func TestRunRejectsUnrecoverableSinkFailure(t *testing.T) {
	base := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	ev, err := events.Encode(events.Event{
		ID: "one", Kind: events.KindIoT, Key: "sensor-a", Value: 12, EventTime: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := Run(context.Background(), bytes.NewReader(append(ev, '\n')), Config{
		Partitions: 1, WindowSize: time.Second, ArrivalStep: time.Millisecond,
		SinkFailures: 2, SinkRetries: 1,
	})
	if err == nil {
		t.Fatal("expected sink error after retries")
	}
	if report.SinkFailures != 2 || report.SinkAttempts != 2 {
		t.Fatalf("unexpected failure report: %+v", report)
	}
}
