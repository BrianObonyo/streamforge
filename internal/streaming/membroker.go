package streaming

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/BrianObonyo/streamforge/internal/partitioning"
)

// MemBroker is a faithful, single-process model of a Kafka-style broker:
// partitioned append-only logs, consumer groups with a coordinator that
// rebalances on membership change, and committed offsets.
//
// It exists for three reasons: (1) unit and integration tests run in
// milliseconds with no Docker; (2) cmd/demo can showcase failure recovery on a
// laptop; (3) it pins down, as executable code, the exact semantics the
// processing engine depends on.
//
// Concurrency: one mutex guards all broker state. That is a deliberate
// simplification — the broker is not the system under study, and a single lock
// makes every invariant (assignment, positions, commits) trivially consistent.
// Blocking consumers wait on a broadcast channel that is closed-and-replaced on
// every state change (append, rebalance, resume, close), the classic Go
// condition-variable-with-context idiom.
type MemBroker struct {
	mu     sync.Mutex
	topics map[string]*memTopic
	groups map[string]*memGroup
	notify chan struct{} // closed + replaced to wake all blocked consumers
	closed bool
	now    func() time.Time

	produceFault func(topic string) error // test hook; see InjectProduceFault
}

type memTopic struct {
	name  string
	parts [][]Record // parts[p][offset]
}

type memGroup struct {
	id         string
	members    map[string]*memConsumer
	committed  map[TopicPartition]int64 // next offset to read, per partition
	generation int64                    // bumped on every rebalance
}

// NewMemBroker returns an empty broker.
func NewMemBroker() *MemBroker {
	return &MemBroker{
		topics: map[string]*memTopic{},
		groups: map[string]*memGroup{},
		notify: make(chan struct{}),
		now:    time.Now,
	}
}

// broadcastLocked wakes every blocked Poll. Caller must hold b.mu.
func (b *MemBroker) broadcastLocked() {
	close(b.notify)
	b.notify = make(chan struct{})
}

// CreateTopic is idempotent for an identical partition count.
func (b *MemBroker) CreateTopic(_ context.Context, name string, partitions int) error {
	if partitions < 1 {
		return fmt.Errorf("topic %q: partitions must be >= 1", name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		if len(t.parts) != partitions {
			return fmt.Errorf("topic %q exists with %d partitions (requested %d)", name, len(t.parts), partitions)
		}
		return nil
	}
	b.topics[name] = &memTopic{name: name, parts: make([][]Record, partitions)}
	return nil
}

// Close wakes and terminates every consumer.
func (b *MemBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.broadcastLocked()
	return nil
}

// InjectProduceFault installs a hook consulted before every append; returning a
// non-nil error makes Produce fail. Used to test retry logic. Pass nil to clear.
func (b *MemBroker) InjectProduceFault(fn func(topic string) error) {
	b.mu.Lock()
	b.produceFault = fn
	b.mu.Unlock()
}

// ----------------------------------------------------------------- introspection

// EndOffsets returns the next offset to be written in each partition.
func (b *MemBroker) EndOffsets(topic string) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.topics[topic]
	if t == nil {
		return nil
	}
	out := make([]int64, len(t.parts))
	for i, p := range t.parts {
		out[i] = int64(len(p))
	}
	return out
}

// Committed returns a copy of a group's committed offsets.
func (b *MemBroker) Committed(group string) map[TopicPartition]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[TopicPartition]int64{}
	if g := b.groups[group]; g != nil {
		for k, v := range g.committed {
			out[k] = v
		}
	}
	return out
}

// Lag returns, per partition, how many records the group has yet to commit past
// (end offset − committed offset). This is the "consumer lag" SRE dashboards
// track; it rises under backpressure and after a consumer crash.
func (b *MemBroker) Lag(group, topic string) map[TopicPartition]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[TopicPartition]int64{}
	t := b.topics[topic]
	if t == nil {
		return out
	}
	var committed map[TopicPartition]int64
	if g := b.groups[group]; g != nil {
		committed = g.committed
	}
	for p, recs := range t.parts {
		tp := TopicPartition{topic, p}
		out[tp] = int64(len(recs)) - committed[tp] // missing key ⇒ 0 ⇒ full backlog
	}
	return out
}

// --------------------------------------------------------------------- producer

type memProducer struct {
	b    *MemBroker
	part partitioning.Partitioner
	rr   partitioning.RoundRobin // fallback for key-less records
}

// NewProducer returns a producer routing keyed records with p (Hash if nil).
func (b *MemBroker) NewProducer(p partitioning.Partitioner) (Producer, error) {
	if p == nil {
		p = partitioning.Hash{}
	}
	return &memProducer{b: b, part: p}, nil
}

func (p *memProducer) Produce(ctx context.Context, topic string, key, value []byte) (int, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	b := p.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, 0, ErrClosed
	}
	if b.produceFault != nil {
		if err := b.produceFault(topic); err != nil {
			return 0, 0, err
		}
	}
	t := b.topics[topic]
	if t == nil {
		return 0, 0, fmt.Errorf("%w: %q", ErrUnknownTopic, topic)
	}
	n := len(t.parts)
	var idx int
	if len(key) == 0 {
		idx = p.rr.Partition(nil, n)
	} else {
		idx = p.part.Partition(key, n)
	}
	rec := Record{
		Topic: topic, Partition: idx, Offset: int64(len(t.parts[idx])),
		// Copy so later mutation by the caller can't corrupt the "durable" log.
		Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		Timestamp: b.now(),
	}
	t.parts[idx] = append(t.parts[idx], rec)
	b.broadcastLocked()
	return idx, rec.Offset, nil
}

func (p *memProducer) Close() error { return nil }

// --------------------------------------------------------------------- consumer

type memConsumer struct {
	b      *MemBroker
	g      *memGroup
	id     string
	topics map[string]struct{}

	// All fields below are guarded by b.mu.
	assigned []TopicPartition // sorted
	pos      map[TopicPartition]int64
	paused   map[TopicPartition]bool
	seenGen  int64
	rr       int // rotating start index so one hot partition can't starve the others
	closed   bool
}

var memberSeq struct {
	sync.Mutex
	n int
}

// NewConsumer joins group as memberID (auto-generated if empty) and triggers a
// rebalance across the group.
func (b *MemBroker) NewConsumer(group, memberID string, topics ...string) (Consumer, error) {
	if group == "" || len(topics) == 0 {
		return nil, fmt.Errorf("group and at least one topic are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	for _, t := range topics {
		if b.topics[t] == nil {
			return nil, fmt.Errorf("%w: %q", ErrUnknownTopic, t)
		}
	}
	g := b.groups[group]
	if g == nil {
		g = &memGroup{id: group, members: map[string]*memConsumer{}, committed: map[TopicPartition]int64{}}
		b.groups[group] = g
	}
	if memberID == "" {
		memberSeq.Lock()
		memberSeq.n++
		memberID = fmt.Sprintf("member-%d", memberSeq.n)
		memberSeq.Unlock()
	}
	if _, dup := g.members[memberID]; dup {
		return nil, fmt.Errorf("member %q already in group %q", memberID, group)
	}
	c := &memConsumer{
		b: b, g: g, id: memberID, topics: map[string]struct{}{},
		pos: map[TopicPartition]int64{}, paused: map[TopicPartition]bool{},
	}
	for _, t := range topics {
		c.topics[t] = struct{}{}
	}
	g.members[memberID] = c
	b.rebalanceLocked(g)
	return c, nil
}

// rebalanceLocked redistributes a group's partitions across its members.
//
// Strategy: for each topic, partition p goes to eligible[p mod len(eligible)],
// where eligible is the sorted list of members subscribed to that topic. It is
// deterministic, balanced to within one partition, and — importantly — a
// partition keeps its owner whenever membership is unchanged.
//
// A consumer that RETAINS a partition across the rebalance keeps its in-memory
// read position (like Kafka's cooperative-sticky assignor); a consumer that
// GAINS a partition starts at the group's committed offset (or 0 if none), which
// is precisely the at-least-once recovery path.
func (b *MemBroker) rebalanceLocked(g *memGroup) {
	g.generation++

	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	// Every topic any member subscribes to.
	topicSet := map[string]struct{}{}
	for _, m := range g.members {
		for t := range m.topics {
			topicSet[t] = struct{}{}
		}
	}

	next := make(map[string][]TopicPartition, len(ids)) // memberID -> new assignment
	for topic := range topicSet {
		var eligible []string
		for _, id := range ids {
			if _, ok := g.members[id].topics[topic]; ok {
				eligible = append(eligible, id)
			}
		}
		for p := range b.topics[topic].parts {
			owner := eligible[p%len(eligible)]
			next[owner] = append(next[owner], TopicPartition{topic, p})
		}
	}

	for _, id := range ids {
		m := g.members[id]
		prev := make(map[TopicPartition]bool, len(m.assigned))
		for _, tp := range m.assigned {
			prev[tp] = true
		}
		assigned := next[id]
		sort.Slice(assigned, func(i, j int) bool {
			if assigned[i].Topic != assigned[j].Topic {
				return assigned[i].Topic < assigned[j].Topic
			}
			return assigned[i].Partition < assigned[j].Partition
		})
		newPos := make(map[TopicPartition]int64, len(assigned))
		newPaused := map[TopicPartition]bool{}
		for _, tp := range assigned {
			if prev[tp] {
				newPos[tp] = m.pos[tp]
				newPaused[tp] = m.paused[tp]
			} else {
				newPos[tp] = g.committed[tp] // zero value = start of log
			}
		}
		m.assigned, m.pos, m.paused = assigned, newPos, newPaused
	}
	b.broadcastLocked()
}

// fetchLocked collects up to max records round-robin across un-paused partitions.
func (c *memConsumer) fetchLocked(max int) []Record {
	var out []Record
	n := len(c.assigned)
	for i := 0; i < n && len(out) < max; i++ {
		tp := c.assigned[(c.rr+i)%n]
		if c.paused[tp] {
			continue
		}
		log := c.b.topics[tp.Topic].parts[tp.Partition]
		p := c.pos[tp]
		for p < int64(len(log)) && len(out) < max {
			out = append(out, log[p])
			p++
		}
		c.pos[tp] = p
	}
	if n > 0 {
		c.rr = (c.rr + 1) % n
	}
	return out
}

func (c *memConsumer) Poll(ctx context.Context, max int) ([]Record, error) {
	if max < 1 {
		max = 1
	}
	for {
		c.b.mu.Lock()
		if c.closed || c.b.closed {
			c.b.mu.Unlock()
			return nil, ErrClosed
		}
		// Surface rebalances promptly so the caller re-reads Assignment()
		// before it processes anything from the new generation.
		if c.seenGen != c.g.generation {
			c.seenGen = c.g.generation
			c.b.mu.Unlock()
			return nil, nil
		}
		recs := c.fetchLocked(max)
		wake := c.b.notify
		c.b.mu.Unlock()

		if len(recs) > 0 {
			return recs, nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *memConsumer) Commit(_ context.Context, offsets map[TopicPartition]int64) error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	if c.closed || c.b.closed {
		return ErrClosed
	}
	// Validate everything first so a commit is all-or-nothing. Committing a
	// partition we no longer own is the classic "zombie consumer" bug: after a
	// rebalance its offsets could overwrite the new owner's progress. Kafka
	// prevents it with generation fencing; we reject on ownership.
	for tp := range offsets {
		if _, ok := c.pos[tp]; !ok {
			return fmt.Errorf("%w: %s", ErrNotAssigned, tp)
		}
	}
	for tp, off := range offsets {
		if off > c.g.committed[tp] { // never move backwards
			c.g.committed[tp] = off
		}
	}
	return nil
}

func (c *memConsumer) Assignment() []TopicPartition {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	return append([]TopicPartition(nil), c.assigned...)
}

func (c *memConsumer) Pause(tps ...TopicPartition) {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, tp := range tps {
		if _, ok := c.pos[tp]; ok {
			c.paused[tp] = true
		}
	}
}

func (c *memConsumer) Resume(tps ...TopicPartition) {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, tp := range tps {
		delete(c.paused, tp)
	}
	c.b.broadcastLocked() // a blocked Poll must notice the partition is readable again
}

func (c *memConsumer) Close() error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	delete(c.g.members, c.id)
	if len(c.g.members) > 0 {
		c.b.rebalanceLocked(c.g)
	} else {
		c.b.broadcastLocked()
	}
	return nil
}

func init() {
	// Each Open("memory") yields an independent broker; to share one between
	// components, construct a MemBroker once and pass it around (cmd/demo does).
	Register("memory", func([]string) (Broker, error) { return NewMemBroker(), nil })
}
