package processor

import (
	"github.com/BrianObonyo/streamforge/internal/metrics"
)

// pm bundles the processor's metrics so hot-path code touches plain pointers
// (no map lookups, no locks).
type pm struct {
	reg *metrics.Registry

	onTime, late, tooLate, duplicate, invalid, poison *metrics.Counter

	emitted, emittedUpdates *metrics.Counter
	sinkErrors              *metrics.Counter
	commits, commitErrors   *metrics.Counter
	rebalances              *metrics.Counter
	dlqSent, dlqErrors      *metrics.Counter

	latency     *metrics.Histogram // broker append → processed
	sinkLatency *metrics.Histogram
}

func newMetrics(reg *metrics.Registry) *pm {
	if reg == nil {
		reg = metrics.NewRegistry()
	}
	const evName, evHelp = "sf_events_processed_total", "Events handled by the processor, by outcome."
	b := metrics.DefaultLatencyBuckets
	return &pm{
		reg:       reg,
		onTime:    reg.Counter(evName, evHelp, metrics.Labels{"outcome": "on_time"}),
		late:      reg.Counter(evName, evHelp, metrics.Labels{"outcome": "late"}),
		tooLate:   reg.Counter(evName, evHelp, metrics.Labels{"outcome": "too_late"}),
		duplicate: reg.Counter(evName, evHelp, metrics.Labels{"outcome": "duplicate"}),
		invalid:   reg.Counter(evName, evHelp, metrics.Labels{"outcome": "invalid"}),
		poison:    reg.Counter(evName, evHelp, metrics.Labels{"outcome": "poison"}),

		emitted:        reg.Counter("sf_window_results_total", "Aggregate rows written to the sink.", metrics.Labels{"kind": "first"}),
		emittedUpdates: reg.Counter("sf_window_results_total", "Aggregate rows written to the sink.", metrics.Labels{"kind": "late_update"}),
		sinkErrors:     reg.Counter("sf_sink_errors_total", "Sink writes that failed after retries.", nil),
		commits:        reg.Counter("sf_offset_commits_total", "Successful offset commits.", nil),
		commitErrors:   reg.Counter("sf_offset_commit_errors_total", "Failed offset commits.", nil),
		rebalances:     reg.Counter("sf_rebalances_total", "Partition assignment changes observed.", nil),
		dlqSent:        reg.Counter("sf_dlq_messages_total", "Messages sent to the dead-letter queue.", nil),
		dlqErrors:      reg.Counter("sf_dlq_errors_total", "Dead-letter sends that failed.", nil),

		latency:     reg.Histogram("sf_process_latency_seconds", "Time from broker append to processing.", nil, b),
		sinkLatency: reg.Histogram("sf_sink_write_seconds", "Sink write duration (including retries).", nil, b),
	}
}
