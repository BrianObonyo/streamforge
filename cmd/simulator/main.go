// Command simulator replays caller-provided JSONL events through StreamForge's
// deterministic in-memory processing path.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BrianObonyo/streamforge/internal/simulator"
)

func main() {
	var (
		input           = flag.String("input", "", "path to newline-delimited event JSON (required)")
		partitions      = flag.Int("partitions", 6, "number of simulated partitions")
		window          = flag.Duration("window", 10*time.Second, "event-time window size")
		slide           = flag.Duration("slide", 0, "window slide (0 = tumbling)")
		maxOutOfOrder   = flag.Duration("max-ooo", 2*time.Second, "watermark slack for out-of-order events")
		allowedLateness = flag.Duration("lateness", 5*time.Second, "allowed lateness after a window fires")
		dedupTTL        = flag.Duration("dedup-ttl", 10*time.Minute, "duplicate ID retention time (0 = disabled)")
		dedupMax        = flag.Int("dedup-max", 1_000_000, "maximum IDs retained per partition (0 = disabled)")
		clockStart      = flag.String("clock-start", "", "virtual arrival clock start (RFC3339Nano); empty uses input timestamps")
		arrivalStep     = flag.Duration("arrival-step", time.Millisecond, "virtual clock step for records without increasing input timestamps")
		sinkFailures    = flag.Int("sink-failures", 0, "number of initial in-memory sink writes to fail")
		sinkRetries     = flag.Int("sink-retries", 3, "retry attempts after an injected sink failure")
	)
	flag.Parse()
	if *input == "" {
		fatal("-input is required; the simulator does not include an embedded dataset")
	}
	var start time.Time
	if *clockStart != "" {
		var err error
		start, err = time.Parse(time.RFC3339Nano, *clockStart)
		if err != nil {
			fatal("invalid -clock-start: " + err.Error())
		}
	}
	f, err := os.Open(*input)
	if err != nil {
		fatal("open input: " + err.Error())
	}
	defer f.Close()

	report, err := simulator.Run(context.Background(), f, simulator.Config{
		Partitions: *partitions, WindowSize: *window, WindowSlide: *slide,
		MaxOutOfOrder: *maxOutOfOrder, AllowedLateness: *allowedLateness,
		DedupTTL: *dedupTTL, DedupMax: *dedupMax,
		ClockStart: start, ArrivalStep: *arrivalStep,
		SinkFailures: *sinkFailures, SinkRetries: *sinkRetries,
	})
	if err != nil {
		fatal(err.Error())
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fatal("write report: " + err.Error())
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, "simulator:", message)
	os.Exit(1)
}
