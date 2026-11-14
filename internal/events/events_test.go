package events

import (
	"errors"
	"math"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func good() Event {
	return Event{ID: "1", Kind: KindIoT, Key: "s", Value: 1.5, EventTime: now}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Event){
		"missing id":  func(e *Event) { e.ID = "" },
		"bad kind":    func(e *Event) { e.Kind = "nope" },
		"missing key": func(e *Event) { e.Key = "" },
		"NaN":         func(e *Event) { e.Value = math.NaN() },
		"+Inf":        func(e *Event) { e.Value = math.Inf(1) },
		"zero time":   func(e *Event) { e.EventTime = time.Time{} },
		"far future":  func(e *Event) { e.EventTime = now.Add(MaxFutureSkew + time.Second) },
	}
	for name, mutate := range cases {
		e := good()
		mutate(&e)
		if err := e.Validate(now); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err=%v, want ErrInvalid", name, err)
		}
	}
	e := good()
	if err := e.Validate(now); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	e.EventTime = now.Add(MaxFutureSkew) // exactly at the limit is allowed
	if err := e.Validate(now); err != nil {
		t.Fatalf("boundary rejected: %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	in := good()
	in.Attrs = map[string]string{"unit": "c"}
	b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil || out.ID != in.ID || !out.EventTime.Equal(in.EventTime) || out.Attrs["unit"] != "c" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if _, err := Decode([]byte("{oops")); err == nil {
		t.Fatal("garbage decoded")
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 10000; i++ {
		id := NewID()
		if len(id) != 32 || seen[id] {
			t.Fatalf("bad or duplicate id %q", id)
		}
		seen[id] = true
	}
}
