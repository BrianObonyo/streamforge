package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
)

type rowKey struct {
	start, end int64
	kind       events.Kind
	key        string
}

// MemStore is a thread-safe in-memory Store.
type MemStore struct {
	mu          sync.RWMutex
	rows        map[rowKey]aggregation.Result
	late        []LateStatRow
	checkpoints map[checkpointKey]Checkpoint

	failNext int // test hook: fail this many upcoming WriteResults calls
}

type checkpointKey struct {
	group     string
	topic     string
	partition int
}

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{
		rows:        map[rowKey]aggregation.Result{},
		checkpoints: map[checkpointKey]Checkpoint{},
	}
}

// FailNext makes the next n WriteResults calls fail (simulating a sink outage).
func (m *MemStore) FailNext(n int) {
	m.mu.Lock()
	m.failNext = n
	m.mu.Unlock()
}

// WriteResults upserts rows, keeping the highest Version per key — the same
// rule ClickHouse's ReplacingMergeTree(version) applies. Ties keep the existing
// row; equal versions describe identical content by construction.
func (m *MemStore) WriteResults(_ context.Context, rs []aggregation.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext > 0 {
		m.failNext--
		return errSimulatedOutage
	}
	for _, r := range rs {
		k := rowKey{r.Window.Start.UnixNano(), r.Window.End.UnixNano(), r.Kind, r.Key}
		if old, ok := m.rows[k]; !ok || r.Version > old.Version {
			m.rows[k] = r
		}
	}
	return nil
}

func (m *MemStore) WriteLateStats(_ context.Context, rows []LateStatRow) error {
	m.mu.Lock()
	m.late = append(m.late, rows...)
	m.mu.Unlock()
	return nil
}

// LoadCheckpoint returns a copy of the latest checkpoint for a partition.
func (m *MemStore) LoadCheckpoint(ctx context.Context, group, topic string, partition int) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp, ok := m.checkpoints[checkpointKey{group: group, topic: topic, partition: partition}]
	cp.State = append([]byte(nil), cp.State...)
	return cp, ok, nil
}

// SaveCheckpoint stores a newer checkpoint, preserving the highest revision.
func (m *MemStore) SaveCheckpoint(ctx context.Context, cp Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCheckpoint(cp); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := checkpointKey{group: cp.Group, topic: cp.Topic, partition: cp.Partition}
	old, ok := m.checkpoints[key]
	if ok && cp.Revision < old.Revision {
		return fmt.Errorf("checkpoint: revision %d is older than stored revision %d", cp.Revision, old.Revision)
	}
	if ok && cp.Revision == old.Revision && (cp.Offset != old.Offset || cp.Config != old.Config || string(cp.State) != string(old.State)) {
		return fmt.Errorf("checkpoint: conflicting data at revision %d", cp.Revision)
	}
	cp.State = append([]byte(nil), cp.State...)
	m.checkpoints[key] = cp
	return nil
}

type simErr string

func (e simErr) Error() string { return string(e) }

const errSimulatedOutage = simErr("memstore: simulated sink outage")

// Rows returns a snapshot of every stored row (tests).
func (m *MemStore) Rows() []aggregation.Result {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]aggregation.Result, 0, len(m.rows))
	for _, r := range m.rows {
		out = append(out, r)
	}
	return out
}

// match applies a Query's filters to a row.
func match(q Query, r aggregation.Result) bool {
	if q.Kind != "" && r.Kind != q.Kind {
		return false
	}
	if q.Key != "" && r.Key != q.Key {
		return false
	}
	if !q.From.IsZero() && r.Window.Start.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && !r.Window.Start.Before(q.To) {
		return false
	}
	return true
}

func (m *MemStore) Summary(_ context.Context, q Query) ([]SummaryRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	acc := map[events.Kind]*SummaryRow{}
	windows := map[events.Kind]map[int64]struct{}{}
	for _, r := range m.rows {
		if !match(q, r) {
			continue
		}
		s := acc[r.Kind]
		if s == nil {
			s = &SummaryRow{Kind: r.Kind, Min: r.Stats.Min, Max: r.Stats.Max}
			acc[r.Kind] = s
			windows[r.Kind] = map[int64]struct{}{}
		}
		s.Events += r.Stats.Count
		s.Sum += r.Stats.Sum
		if r.Stats.Min < s.Min {
			s.Min = r.Stats.Min
		}
		if r.Stats.Max > s.Max {
			s.Max = r.Stats.Max
		}
		windows[r.Kind][r.Window.Start.UnixNano()] = struct{}{}
	}
	out := make([]SummaryRow, 0, len(acc))
	for k, s := range acc {
		s.Windows = int64(len(windows[k]))
		if s.Events > 0 {
			s.Avg = s.Sum / float64(s.Events)
		}
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out, nil
}

func (m *MemStore) Series(_ context.Context, q Query) ([]SeriesPoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []SeriesPoint
	for _, r := range m.rows {
		if match(q, r) {
			out = append(out, SeriesPoint{
				WindowStart: r.Window.Start, WindowEnd: r.Window.End, Kind: r.Kind, Key: r.Key,
				Count: r.Stats.Count, Avg: r.Stats.Avg(), Min: r.Stats.Min, Max: r.Stats.Max,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].WindowStart.Before(out[j].WindowStart)
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Key < out[j].Key
	})
	limit := q.Limit
	if limit <= 0 {
		limit = 1000
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) LateSummary(_ context.Context, q Query) (LateSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var s LateSummary
	for _, r := range m.late {
		if (!q.From.IsZero() && r.TS.Before(q.From)) || (!q.To.IsZero() && !r.TS.Before(q.To)) {
			continue
		}
		s.OnTime += r.OnTime
		s.Late += r.Late
		s.TooLate += r.TooLate
		if r.MaxLatenessMs > s.MaxLatenessMs {
			s.MaxLatenessMs = r.MaxLatenessMs
		}
	}
	return s, nil
}

var _ Store = (*MemStore)(nil)
var _ = time.Second // keep time imported for Query users in this package
