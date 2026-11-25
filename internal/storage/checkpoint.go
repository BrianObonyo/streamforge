package storage

import (
	"context"
	"fmt"
)

// Checkpoint identifies one processor group's durable state for a partition.
// Offset is the next broker offset to consume after restoring State.
type Checkpoint struct {
	Group     string
	Topic     string
	Partition int
	Offset    int64
	Revision  uint64
	Config    string
	State     []byte
}

// CheckpointStore persists partition snapshots independently of analytics data.
type CheckpointStore interface {
	LoadCheckpoint(ctx context.Context, group, topic string, partition int) (Checkpoint, bool, error)
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
}

func validateCheckpoint(cp Checkpoint) error {
	if cp.Group == "" || cp.Topic == "" || cp.Partition < 0 ||
		uint64(cp.Partition) > uint64(^uint32(0)) || cp.Offset < 0 ||
		cp.Revision == 0 || cp.Config == "" || len(cp.State) == 0 {
		return fmt.Errorf("checkpoint: invalid checkpoint metadata or empty state")
	}
	return nil
}
