//go:build redpanda

// Command kafkasim is a standalone, in-memory Kafka-protocol broker (franz-go's
// kfake) for LOCAL TESTING ONLY. It lets the real producer/processor/api binaries
// run as separate OS processes talking Kafka over TCP when Docker and Redpanda
// are unavailable — see scripts/e2e-local.sh.
//
// It is not Redpanda: no persistence, no replication, simplified coordinator.
// Never use it for anything but tests.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kfake"
)

func main() {
	port := flag.Int("port", 19092, "listen port")
	flag.Parse()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(*port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafkasim:", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("kafkasim listening on", c.ListenAddrs())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}
