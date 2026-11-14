package metrics

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("sf_events_total", "Events.", Labels{"outcome": "late"})
	c.Add(3)
	r.Counter("sf_events_total", "Events.", Labels{"outcome": "on_time"}).Inc()
	r.Gauge("sf_depth", "Depth.", nil).Set(2.5)
	r.GaugeFunc("sf_lag", "Lag.", Labels{"p": "0"}, func() float64 { return 42 })

	var buf bytes.Buffer
	if _, err := r.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"# TYPE sf_events_total counter",
		`sf_events_total{outcome="late"} 3`,
		`sf_events_total{outcome="on_time"} 1`,
		"sf_depth 2.5",
		`sf_lag{p="0"} 42`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRegistrationIsIdempotent(t *testing.T) {
	r := NewRegistry()
	a := r.Counter("x", "h", Labels{"a": "1"})
	b := r.Counter("x", "h", Labels{"a": "1"})
	if a != b {
		t.Fatal("same series registered twice returned different objects")
	}
}

func TestHistogramQuantileAndExposition(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("lat", "Latency.", nil, []float64{0.01, 0.1, 1})
	for i := 0; i < 90; i++ {
		h.Observe(0.005)
	}
	for i := 0; i < 10; i++ {
		h.Observe(0.5)
	}
	if p50 := h.Quantile(0.5); p50 <= 0 || p50 > 0.01 {
		t.Errorf("p50=%v, want within first bucket", p50)
	}
	if p99 := h.Quantile(0.99); p99 < 0.1 || p99 > 1 {
		t.Errorf("p99=%v, want within (0.1,1]", p99)
	}
	var buf bytes.Buffer
	_, _ = r.WriteTo(&buf)
	for _, want := range []string{`lat_bucket{le="0.01"} 90`, `lat_bucket{le="+Inf"} 100`, "lat_count 100"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("missing %q in:\n%s", want, buf.String())
		}
	}
	if !math.IsNaN(r.Histogram("empty", "", nil, nil).Quantile(0.5)) {
		t.Error("empty histogram quantile should be NaN")
	}
}

func TestGaugeAddConcurrent(t *testing.T) {
	var g Gauge
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				g.Add(1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if g.Value() != 8000 {
		t.Fatalf("lost updates: %v", g.Value())
	}
}

func TestUnregister(t *testing.T) {
	r := NewRegistry()
	r.Gauge("g", "h", Labels{"p": "1"})
	r.Unregister("g", Labels{"p": "1"})
	var buf bytes.Buffer
	_, _ = r.WriteTo(&buf)
	if strings.Contains(buf.String(), `p="1"`) {
		t.Fatal("series not removed")
	}
}
