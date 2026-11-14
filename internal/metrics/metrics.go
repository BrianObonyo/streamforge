// Package metrics is a small, dependency-free metrics registry that renders the
// Prometheus text exposition format (version 0.0.4).
//
// Why not just import prometheus/client_golang? StreamForge's core is
// deliberately stdlib-only so it builds anywhere with zero network access, and
// the exposition format is simple enough to implement correctly. Swapping in
// the official client later only means changing this package.
//
// Format reference: https://prometheus.io/docs/instrumenting/exposition_formats/
//
// All metric types are lock-free on the hot path (atomics). The registry mutex
// is only taken when registering metrics or rendering a scrape.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Labels is a set of label name→value pairs attached to one time series.
type Labels map[string]string

// render produces the inner label string `a="x",b="y"` with sorted keys, so two
// Labels with equal content always map to the same series identity.
func (l Labels) render() string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		sb.WriteString(escapeLabel(l[k]))
		sb.WriteByte('"')
	}
	return sb.String()
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

// series is anything that can write its sample lines.
type series interface {
	write(w io.Writer, name, labels string)
}

type family struct {
	help  string
	typ   string // counter | gauge | histogram
	items map[string]series
}

// Registry owns a set of metric families.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

// getOrCreate returns the existing series for (name, labels) or installs the one
// produced by mk. Registration is idempotent: partition tasks come and go as
// consumers rebalance, and each one re-registers its gauges.
func (r *Registry) getOrCreate(name, help, typ string, l Labels, mk func() series) series {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		f = &family{help: help, typ: typ, items: map[string]series{}}
		r.families[name] = f
	}
	key := l.render()
	if s, ok := f.items[key]; ok {
		return s
	}
	s := mk()
	f.items[key] = s
	return s
}

// Unregister removes one series (used when a partition is revoked).
func (r *Registry) Unregister(name string, l Labels) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		delete(f.items, l.render())
	}
}

// ------------------------------------------------------------------ Counter

// Counter is a monotonically increasing integer.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

func (c *Counter) write(w io.Writer, name, labels string) {
	fmt.Fprintf(w, "%s%s %d\n", name, wrap(labels), c.v.Load())
}

// Counter registers (or fetches) a counter series.
func (r *Registry) Counter(name, help string, l Labels) *Counter {
	return r.getOrCreate(name, help, "counter", l, func() series { return &Counter{} }).(*Counter)
}

// -------------------------------------------------------------------- Gauge

// Gauge is a float64 that can go up and down. The float is stored as raw bits in
// an atomic uint64 so Set/Add never take a lock.
type Gauge struct{ bits atomic.Uint64 }

func (g *Gauge) Set(v float64)  { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Add uses a compare-and-swap loop because float addition isn't a native atomic.
func (g *Gauge) Add(d float64) {
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}

func (g *Gauge) write(w io.Writer, name, labels string) {
	fmt.Fprintf(w, "%s%s %s\n", name, wrap(labels), fmtFloat(g.Value()))
}

// Gauge registers (or fetches) a gauge series.
func (r *Registry) Gauge(name, help string, l Labels) *Gauge {
	return r.getOrCreate(name, help, "gauge", l, func() series { return &Gauge{} }).(*Gauge)
}

// ---------------------------------------------------------------- GaugeFunc

// gaugeFunc samples a callback at scrape time — ideal for values that already
// live elsewhere (queue length, broker lag) so we never maintain a copy.
type gaugeFunc struct {
	mu sync.Mutex
	fn func() float64
}

func (g *gaugeFunc) write(w io.Writer, name, labels string) {
	g.mu.Lock()
	fn := g.fn
	g.mu.Unlock()
	fmt.Fprintf(w, "%s%s %s\n", name, wrap(labels), fmtFloat(fn()))
}

// GaugeFunc registers a callback gauge; re-registering replaces the callback.
func (r *Registry) GaugeFunc(name, help string, l Labels, fn func() float64) {
	g := r.getOrCreate(name, help, "gauge", l, func() series { return &gaugeFunc{fn: fn} }).(*gaugeFunc)
	g.mu.Lock()
	g.fn = fn
	g.mu.Unlock()
}

// ---------------------------------------------------------------- Histogram

// Histogram counts observations into fixed upper-bound buckets.
type Histogram struct {
	bounds  []float64       // sorted ascending upper bounds (le)
	counts  []atomic.Uint64 // per-bucket, NON-cumulative; len = len(bounds)+1 (+Inf)
	sumBits atomic.Uint64
	count   atomic.Uint64
}

// DefaultLatencyBuckets spans 0.5 ms … 10 s, suitable for stream latencies.
var DefaultLatencyBuckets = []float64{
	.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Quantile estimates the q-quantile (0<q<1) by linear interpolation inside the
// bucket that contains the target rank — the same approach as PromQL's
// histogram_quantile(). Accuracy is bounded by bucket width.
func (h *Histogram) Quantile(q float64) float64 {
	total := h.count.Load()
	if total == 0 || q <= 0 || q >= 1 {
		return math.NaN()
	}
	rank := q * float64(total)
	var cum float64
	for i := range h.counts {
		c := float64(h.counts[i].Load())
		if cum+c >= rank {
			if i == len(h.bounds) { // landed in +Inf: best we can say is "≥ last bound"
				return h.bounds[len(h.bounds)-1]
			}
			lo := 0.0
			if i > 0 {
				lo = h.bounds[i-1]
			}
			if c == 0 {
				return h.bounds[i]
			}
			return lo + (h.bounds[i]-lo)*((rank-cum)/c)
		}
		cum += c
	}
	return h.bounds[len(h.bounds)-1]
}

func (h *Histogram) write(w io.Writer, name, labels string) {
	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, wrap(join(labels, `le="`+fmtFloat(b)+`"`)), cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket%s %d\n", name, wrap(join(labels, `le="+Inf"`)), cum)
	fmt.Fprintf(w, "%s_sum%s %s\n", name, wrap(labels), fmtFloat(math.Float64frombits(h.sumBits.Load())))
	fmt.Fprintf(w, "%s_count%s %d\n", name, wrap(labels), h.count.Load())
}

// Histogram registers (or fetches) a histogram series.
func (r *Registry) Histogram(name, help string, l Labels, buckets []float64) *Histogram {
	return r.getOrCreate(name, help, "histogram", l, func() series {
		b := append([]float64(nil), buckets...)
		sort.Float64s(b)
		return &Histogram{bounds: b, counts: make([]atomic.Uint64, len(b)+1)}
	}).(*Histogram)
}

// ---------------------------------------------------------------- exposition

// WriteTo renders every family in Prometheus text format, sorted by name so
// output is stable (handy for golden tests and diffs).
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	sort.Strings(names)
	// Snapshot series under lock, render outside it so a slow scraper can't
	// block metric registration.
	type snap struct {
		name string
		f    *family
		keys []string
		ser  []series
	}
	snaps := make([]snap, 0, len(names))
	for _, n := range names {
		f := r.families[n]
		keys := make([]string, 0, len(f.items))
		for k := range f.items {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ser := make([]series, len(keys))
		for i, k := range keys {
			ser[i] = f.items[k]
		}
		snaps = append(snaps, snap{n, f, keys, ser})
	}
	r.mu.Unlock()

	cw := &countWriter{w: w}
	for _, s := range snaps {
		fmt.Fprintf(cw, "# HELP %s %s\n# TYPE %s %s\n", s.name, s.f.help, s.name, s.f.typ)
		for i, k := range s.keys {
			s.ser[i].write(cw, s.name, k)
		}
	}
	return cw.n, cw.err
}

type countWriter struct {
	w   io.Writer
	n   int64
	err error
}

func (c *countWriter) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.err = err
	return n, err
}

func wrap(labels string) string {
	if labels == "" {
		return ""
	}
	return "{" + labels + "}"
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "," + b
}

func fmtFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
