//go:build redpanda

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

type liveProcessorMember struct {
	consumer streaming.Consumer
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
	crashed  bool
}

type commitObserver struct {
	streaming.Consumer
	commits chan map[streaming.TopicPartition]int64
}

func (c *commitObserver) Commit(ctx context.Context, offsets map[streaming.TopicPartition]int64) error {
	if err := c.Consumer.Commit(ctx, offsets); err != nil {
		return err
	}
	copy := make(map[streaming.TopicPartition]int64, len(offsets))
	for tp, offset := range offsets {
		copy[tp] = offset
	}
	select {
	case c.commits <- copy:
	default:
	}
	return nil
}

func TestLiveCheckpointRecoveryAcrossServices(t *testing.T) {
	brokers := os.Getenv("SF_TEST_BROKERS")
	clickhouseURL := os.Getenv("SF_TEST_CLICKHOUSE_URL")
	if brokers == "" || clickhouseURL == "" {
		t.Skip("SF_TEST_BROKERS and SF_TEST_CLICKHOUSE_URL must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerAddrs := strings.Split(brokers, ",")
	for i := range brokerAddrs {
		brokerAddrs[i] = strings.TrimSpace(brokerAddrs[i])
	}
	broker, err := streaming.Open("redpanda", brokerAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	database := fmt.Sprintf("sf_live_checkpoint_%d", time.Now().UnixNano())
	ch, err := storage.NewClickHouse(storage.CHConfig{
		URL: clickhouseURL, Database: database,
		User: os.Getenv("SF_TEST_CLICKHOUSE_USER"), Password: os.Getenv("SF_TEST_CLICKHOUSE_PASSWORD"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dropLiveClickHouseDatabase(clickhouseURL, database,
			os.Getenv("SF_TEST_CLICKHOUSE_USER"), os.Getenv("SF_TEST_CLICKHOUSE_PASSWORD")); err != nil {
			t.Errorf("drop ClickHouse test database: %v", err)
		}
	})
	if err := ch.EnsureSchema(ctx); err != nil {
		t.Fatalf("create ClickHouse test schema: %v", err)
	}

	topic := fmt.Sprintf("sf-checkpoint-live-%d", time.Now().UnixNano())
	group := fmt.Sprintf("sf-checkpoint-live-%d", time.Now().UnixNano())
	if err := broker.CreateTopic(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	producer, err := broker.NewProducer(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	key := topic
	base := time.Now().UTC().Truncate(time.Second)
	const beforeCrash = 40
	produceEvents := func(start, count int, eventTime time.Time) int64 {
		t.Helper()
		var next int64
		for i := start; i < start+count; i++ {
			payload, err := events.Encode(events.Event{
				ID: fmt.Sprintf("%s-%d", group, i), Kind: events.KindIoT, Key: key,
				Value: 1, EventTime: eventTime,
			})
			if err != nil {
				t.Fatal(err)
			}
			partition, offset, err := producer.Produce(ctx, topic, []byte(key), payload)
			if err != nil {
				t.Fatal(err)
			}
			if partition != 0 {
				t.Fatalf("single-partition test event went to partition %d", partition)
			}
			next = offset + 1
		}
		return next
	}

	initialEnd := produceEvents(0, beforeCrash, base.Add(100*time.Millisecond))
	firstConsumer, err := broker.NewConsumer(group, "before-crash", topic)
	if err != nil {
		t.Fatal(err)
	}
	firstObserver := &commitObserver{Consumer: firstConsumer, commits: make(chan map[streaming.TopicPartition]int64, 4)}
	first := startLiveCheckpointProcessor(t, firstObserver, topic, group, ch)
	t.Cleanup(func() { first.stop(t) })
	waitLiveCheckpointOffset(t, ctx, ch, group, topic, 0, initialEnd)
	saved, ok, err := ch.LoadCheckpoint(ctx, group, topic, 0)
	if err != nil || !ok {
		t.Fatalf("load checkpoint before crash: found=%v err=%v", ok, err)
	}
	var state struct {
		Aggregator aggregation.Snapshot `json:"aggregator"`
	}
	if err := json.Unmarshal(saved.State, &state); err != nil {
		t.Fatalf("decode checkpoint state: %v", err)
	}
	if state.Aggregator.Version != 1 || len(state.Aggregator.Windows) != 1 {
		t.Fatalf("checkpoint did not preserve the open aggregate window: %+v", state.Aggregator)
	}
	select {
	case committed := <-firstObserver.commits:
		t.Fatalf("first processor committed before simulated crash: %v", committed)
	default:
	}
	first.crash(t)

	secondConsumer, err := broker.NewConsumer(group, "after-crash", topic)
	if err != nil {
		t.Fatal(err)
	}
	observer := &commitObserver{
		Consumer: secondConsumer,
		commits:  make(chan map[streaming.TopicPartition]int64, 4),
	}
	second := startLiveCheckpointProcessor(t, observer, topic, group, ch)
	t.Cleanup(func() { second.stop(t) })
	checkpointTP := streaming.TopicPartition{Topic: topic, Partition: 0}
	select {
	case committed := <-observer.commits:
		if committed[checkpointTP] != initialEnd {
			t.Fatalf("restored offset commit=%d, want checkpoint offset %d", committed[checkpointTP], initialEnd)
		}
	case <-ctx.Done():
		t.Fatal("successor did not commit the restored checkpoint offset")
	case <-time.After(30 * time.Second):
		t.Fatal("successor did not commit the restored checkpoint offset")
	}

	produceEvents(beforeCrash, 30, base.Add(20*time.Second))
	produceEvents(beforeCrash+30, 1, base.Add(22*time.Second))
	eventually(t, 30*time.Second, "real ClickHouse to contain the restored exact total", func() (bool, string) {
		rows, err := ch.Summary(ctx, storage.Query{Kind: events.KindIoT, Key: key})
		if err != nil {
			return false, err.Error()
		}
		if len(rows) != 1 {
			return false, fmt.Sprintf("got %d aggregate rows", len(rows))
		}
		return rows[0].Events == beforeCrash+30,
			fmt.Sprintf("stored=%d want=%d", rows[0].Events, beforeCrash+30)
	})
	second.stop(t)
}

func startLiveCheckpointProcessor(t *testing.T, consumer streaming.Consumer, topic, group string, ch *storage.ClickHouse) *liveProcessorMember {
	t.Helper()
	cfg := processor.Config{
		Topic: topic, Group: group, MemberID: fmt.Sprintf("live-%d", time.Now().UnixNano()),
		WindowSize: time.Second, MaxOutOfOrder: 500 * time.Millisecond, // nonzero avoids the 2s default; the final event closes the prior window
		AllowedLateness: time.Minute,
		IdleTimeout:     0, TickInterval: 20 * time.Millisecond, CommitInterval: time.Hour,
		CheckpointInterval: 25 * time.Millisecond,
		Retry:              reliability.RetryPolicy{MaxAttempts: 1},
	}
	p, err := processor.New(cfg, processor.Deps{
		Consumer: consumer, Sink: ch, Checkpoints: ch,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	member := &liveProcessorMember{consumer: consumer, cancel: cancel, done: make(chan struct{})}
	go func() {
		member.err = p.Run(ctx)
		close(member.done)
	}()
	return member
}

func (m *liveProcessorMember) crash(t *testing.T) {
	t.Helper()
	if err := m.consumer.Close(); err != nil {
		t.Fatalf("close broker consumer to simulate crash: %v", err)
	}
	select {
	case <-m.done:
		if !errors.Is(m.err, streaming.ErrClosed) {
			t.Fatalf("crashed processor returned %v, want ErrClosed", m.err)
		}
		m.crashed = true
	case <-time.After(15 * time.Second):
		t.Fatal("crashed processor did not stop")
	}
	m.cancel()
}

func (m *liveProcessorMember) stop(t *testing.T) {
	t.Helper()
	select {
	case <-m.done:
		if !m.crashed && m.err != nil {
			t.Errorf("processor exited before graceful stop: %v", m.err)
		}
		_ = m.consumer.Close()
		return
	default:
	}
	m.cancel()
	select {
	case <-m.done:
		if m.err != nil {
			t.Errorf("graceful processor stop returned %v", m.err)
		}
	case <-time.After(15 * time.Second):
		_ = m.consumer.Close()
		t.Fatal("processor did not stop")
	}
	_ = m.consumer.Close()
}

func waitLiveCheckpointOffset(t *testing.T, ctx context.Context, ch *storage.ClickHouse, group, topic string, partition int, want int64) {
	t.Helper()
	eventually(t, 30*time.Second, "ClickHouse checkpoint to cover published records", func() (bool, string) {
		checkpoint, ok, err := ch.LoadCheckpoint(ctx, group, topic, partition)
		if err != nil {
			return false, err.Error()
		}
		if !ok {
			return false, "checkpoint not written yet"
		}
		return checkpoint.Offset == want, fmt.Sprintf("checkpoint offset=%d want=%d", checkpoint.Offset, want)
	})
}

func dropLiveClickHouseDatabase(baseURL, database, user, password string) error {
	u, err := url.Parse(baseURL)
	if err != nil {
		return err
	}
	query := u.Query()
	query.Set("query", "DROP DATABASE IF EXISTS "+database)
	u.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return err
	}
	if user != "" {
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", password)
	}
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("ClickHouse returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
