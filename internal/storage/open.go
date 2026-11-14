package storage

import (
	"context"
	"fmt"
	"time"
)

// Open builds a Store by name: "memory" or "clickhouse". For ClickHouse it also
// creates the schema, retrying for a while because in Docker Compose the
// database often accepts connections a few seconds after the app container starts.
func Open(ctx context.Context, kind string, cfg CHConfig) (Store, error) {
	switch kind {
	case "memory":
		return NewMemStore(), nil
	case "clickhouse":
		ch, err := NewClickHouse(cfg)
		if err != nil {
			return nil, err
		}
		var last error
		for i := 0; i < 30; i++ {
			if last = ch.EnsureSchema(ctx); last == nil {
				return ch, nil
			}
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, fmt.Errorf("clickhouse not ready after 30s: %w", last)
	}
	return nil, fmt.Errorf("unknown store %q (want memory|clickhouse)", kind)
}
