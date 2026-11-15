// Command genstats replays the (deterministic) workload generator for a seed and
// prints how many UNIQUE valid events, duplicates and garbage messages the first
// N messages contain — the ground truth an end-to-end run must reproduce exactly.
//
//	genstats -seed 99 -count 30000 -disorder 0.1 -dup 0.02 -poison 0.005
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/BrianObonyo/streamforge/internal/ingestion"
)

func main() {
	seed := flag.Int64("seed", 42, "")
	count := flag.Int("count", 10000, "")
	disorder := flag.Float64("disorder", 0.1, "")
	maxDelay := flag.Duration("max-delay", 3*time.Second, "")
	dup := flag.Float64("dup", 0.02, "")
	poison := flag.Float64("poison", 0.005, "")
	flag.Parse()
	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: *seed, DisorderProb: *disorder, MaxDelay: *maxDelay, DupProb: *dup, PoisonProb: *poison})
	var unique, dups, junk int
	for i := 0; i < *count; i++ {
		switch m := g.Next(); {
		case m.Poison:
			junk++
		case m.Dup:
			dups++
		default:
			unique++
		}
	}
	fmt.Printf("unique=%d duplicates=%d garbage=%d\n", unique, dups, junk)
}
