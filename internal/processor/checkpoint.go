package processor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
)

type partitionCheckpointState struct {
	Version    int                         `json:"version"`
	NextOffset int64                       `json:"next_offset"`
	Aggregator aggregation.Snapshot        `json:"aggregator"`
	Deduper    reliability.DeduperSnapshot `json:"deduper"`
	LastLate   aggregation.LateStats       `json:"last_late"`
	LastLateAt time.Time                   `json:"last_late_at"`
}

func checkpointConfigID(cfg Config) string {
	config := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%d",
		cfg.Group, cfg.Topic, cfg.Assigner.Name(), cfg.MaxOutOfOrder,
		cfg.AllowedLateness, cfg.IdleTimeout, cfg.DedupTTL, cfg.DedupMax)
	sum := sha256.Sum256([]byte(config))
	return hex.EncodeToString(sum[:])
}

func (t *partitionTask) encodeCheckpoint() ([]byte, error) {
	if t.next < 0 {
		return nil, fmt.Errorf("processor: cannot checkpoint before processing an offset")
	}
	return json.Marshal(partitionCheckpointState{
		Version: 1, NextOffset: t.next, Aggregator: t.agg.Snapshot(),
		Deduper: t.dedup.Snapshot(), LastLate: t.lastLate, LastLateAt: t.lastLateAt,
	})
}

func (t *partitionTask) restoreCheckpoint(cpOffset int64, state []byte) error {
	var saved partitionCheckpointState
	if err := json.Unmarshal(state, &saved); err != nil {
		return fmt.Errorf("processor: decode partition checkpoint: %w", err)
	}
	if saved.Version != 1 {
		return fmt.Errorf("processor: unsupported partition checkpoint version %d", saved.Version)
	}
	if saved.NextOffset != cpOffset || saved.NextOffset < 0 {
		return fmt.Errorf("processor: checkpoint offset mismatch: metadata=%d state=%d", cpOffset, saved.NextOffset)
	}
	if saved.LastLate.OnTime < 0 || saved.LastLate.Late < 0 ||
		saved.LastLate.TooLate < 0 || saved.LastLate.MaxLateness < 0 {
		return fmt.Errorf("processor: invalid late statistics in checkpoint")
	}
	if err := t.agg.Restore(saved.Aggregator); err != nil {
		return fmt.Errorf("processor: restore aggregator: %w", err)
	}
	if err := t.dedup.Restore(saved.Deduper); err != nil {
		return fmt.Errorf("processor: restore deduper: %w", err)
	}
	t.next, t.lastLate, t.lastLateAt = saved.NextOffset, saved.LastLate, saved.LastLateAt
	t.safe.Store(saved.NextOffset)
	t.consumed.Store(saved.NextOffset)
	t.resumeOffset.Store(saved.NextOffset)
	return nil
}

func (t *partitionTask) persistCheckpoint(ctx context.Context, now time.Time) error {
	if t.next < 0 || (!t.checkpointDirty && now.Sub(t.lastCheckpointAt) < t.p.cfg.CheckpointInterval) {
		return nil
	}
	if t.checkpointRevision == ^uint64(0) {
		return fmt.Errorf("processor: checkpoint revision exhausted for %s", t.tp)
	}
	state, err := t.encodeCheckpoint()
	if err != nil {
		return err
	}
	revision := t.checkpointRevision + 1
	cp := storage.Checkpoint{
		Group: t.p.cfg.Group, Topic: t.tp.Topic, Partition: t.tp.Partition,
		Offset: t.next, Revision: revision, Config: checkpointConfigID(t.p.cfg), State: state,
	}
	if err := t.p.d.Checkpoints.SaveCheckpoint(ctx, cp); err != nil {
		return err
	}
	t.checkpointRevision = revision
	t.lastCheckpointAt = now
	t.checkpointDirty = false
	t.safe.Store(t.next)
	t.p.m.checkpoints.Inc()
	return nil
}
