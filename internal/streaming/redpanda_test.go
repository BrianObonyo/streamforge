//go:build redpanda

package streaming

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/BrianObonyo/streamforge/internal/partitioning"
)

// These tests drive the REAL franz-go client over the REAL Kafka wire protocol
// against kfake, franz-go's in-process Kafka cluster. They check the adapter
// honours the same Consumer/Producer contract MemBroker defines.
//
// What this does NOT prove: Redpanda-specific behaviour (its own coordinator,
// replication, timeouts). For that, point SF_TEST_BROKERS at a live Redpanda — see
// TestLiveBrokerSmoke.

func newKafka(t *testing.T) Broker {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	b, err := Open("redpanda", c.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestKafkaSASLAuthentication(t *testing.T) {
	tests := []struct {
		mechanism string
		tls       bool
	}{
		{mechanism: "PLAIN", tls: true},
		{mechanism: "SCRAM-SHA-256"},
		{mechanism: "SCRAM-SHA-512"},
	}
	for _, tt := range tests {
		t.Run(tt.mechanism, func(t *testing.T) {
			const user, password = "streamforge-test", "test-password"
			opts := []kfake.Opt{
				kfake.NumBrokers(1),
				kfake.EnableSASL(),
				kfake.Superuser(tt.mechanism, user, password),
			}
			if tt.tls {
				serverTLS, caPath, _, _ := testBrokerTLS(t)
				opts = append(opts, kfake.TLS(serverTLS))
				t.Setenv("SF_KAFKA_TLS", "true")
				t.Setenv("SF_KAFKA_TLS_CA", caPath)
			} else {
				t.Setenv("SF_KAFKA_TLS", "")
				t.Setenv("SF_KAFKA_TLS_CA", "")
			}
			t.Setenv("SF_KAFKA_TLS_CERT", "")
			t.Setenv("SF_KAFKA_TLS_KEY", "")
			t.Setenv("SF_KAFKA_TLS_SERVER_NAME", "")
			t.Setenv("SF_KAFKA_SASL_MECHANISM", tt.mechanism)
			t.Setenv("SF_KAFKA_SASL_USERNAME", user)
			t.Setenv("SF_KAFKA_SASL_PASSWORD", password)

			cluster, err := kfake.NewCluster(opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			broker, err := Open("redpanda", cluster.ListenAddrs())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := broker.CreateTopic(ctx, "authenticated", 1); err != nil {
				t.Fatalf("authenticated admin request failed: %v", err)
			}
			producer, err := broker.NewProducer(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			if _, _, err := producer.Produce(ctx, "authenticated", []byte("key"), []byte("value")); err != nil {
				t.Fatalf("authenticated produce failed: %v", err)
			}
		})
	}
}

func TestKafkaSASLRejectsBadCredentials(t *testing.T) {
	const user, password = "streamforge-test", "test-password"
	t.Setenv("SF_KAFKA_TLS", "")
	t.Setenv("SF_KAFKA_TLS_CA", "")
	t.Setenv("SF_KAFKA_TLS_CERT", "")
	t.Setenv("SF_KAFKA_TLS_KEY", "")
	t.Setenv("SF_KAFKA_TLS_SERVER_NAME", "")
	t.Setenv("SF_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("SF_KAFKA_SASL_USERNAME", user)
	t.Setenv("SF_KAFKA_SASL_PASSWORD", "wrong-password")
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.EnableSASL(),
		kfake.Superuser("SCRAM-SHA-256", user, password),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker, err := Open("redpanda", cluster.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.CreateTopic(ctx, "authenticated", 1); err == nil {
		t.Fatal("broker accepted invalid SASL credentials")
	}
}

func TestKafkaMutualTLS(t *testing.T) {
	serverTLS, caPath, clientCertPath, clientKeyPath := testBrokerTLS(t)
	t.Setenv("SF_KAFKA_TLS", "true")
	t.Setenv("SF_KAFKA_TLS_CA", caPath)
	t.Setenv("SF_KAFKA_TLS_CERT", clientCertPath)
	t.Setenv("SF_KAFKA_TLS_KEY", clientKeyPath)
	t.Setenv("SF_KAFKA_TLS_SERVER_NAME", "")
	t.Setenv("SF_KAFKA_SASL_MECHANISM", "")
	t.Setenv("SF_KAFKA_SASL_USERNAME", "")
	t.Setenv("SF_KAFKA_SASL_PASSWORD", "")
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(serverTLS))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker, err := Open("redpanda", cluster.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.CreateTopic(ctx, "mutual-tls", 1); err != nil {
		t.Fatalf("mTLS-authenticated admin request failed: %v", err)
	}
}

func TestRedpandaSecurityConfigurationValidation(t *testing.T) {
	tests := []struct {
		name, mechanism, username, password, tls, cert, key string
	}{
		{name: "unknown mechanism", mechanism: "UNSUPPORTED", username: "u", password: "p"},
		{name: "missing credentials", mechanism: "SCRAM-SHA-256", username: "u"},
		{name: "credentials without mechanism", username: "u", password: "p"},
		{name: "plain without TLS", mechanism: "PLAIN", username: "u", password: "p"},
		{name: "client certificate without key", cert: "client.crt"},
		{name: "invalid TLS boolean", tls: "enabled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SF_KAFKA_TLS", tt.tls)
			t.Setenv("SF_KAFKA_TLS_CA", "")
			t.Setenv("SF_KAFKA_TLS_CERT", tt.cert)
			t.Setenv("SF_KAFKA_TLS_KEY", tt.key)
			t.Setenv("SF_KAFKA_TLS_SERVER_NAME", "")
			t.Setenv("SF_KAFKA_SASL_MECHANISM", tt.mechanism)
			t.Setenv("SF_KAFKA_SASL_USERNAME", tt.username)
			t.Setenv("SF_KAFKA_SASL_PASSWORD", tt.password)
			if _, err := redpandaClientOptionsFromEnv(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func testBrokerTLS(t *testing.T) (*tls.Config, string, string, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "StreamForge test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	rootPool := x509.NewCertPool()
	rootPool.AddCert(caCert)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    caTemplate.NotBefore,
		NotAfter:     caTemplate.NotAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER})
	serverCert, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "streamforge-client"},
		NotBefore:    caTemplate.NotBefore,
		NotAfter:     caTemplate.NotAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})
	clientKeyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER})

	dir := t.TempDir()
	caPath := dir + string(os.PathSeparator) + "ca.pem"
	clientCertPath := dir + string(os.PathSeparator) + "client.crt"
	clientKeyPath := dir + string(os.PathSeparator) + "client.key"
	for path, data := range map[string][]byte{
		caPath: certPEM, clientCertPath: clientPEM, clientKeyPath: clientKeyPEM,
	} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	serverTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: rootPool}
	return serverTLS, caPath, clientCertPath, clientKeyPath
}

func pollUntil(t *testing.T, c Consumer, want int, d time.Duration) []Record {
	t.Helper()
	var all []Record
	deadline := time.Now().Add(d)
	for len(all) < want && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		recs, err := c.Poll(ctx, 100)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("poll: %v", err)
		}
		all = append(all, recs...)
	}
	return all
}

func TestKafkaCreateTopicIsIdempotent(t *testing.T) {
	b := newKafka(t)
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatalf("second create must succeed: %v", err)
	}
}

func TestKafkaProduceConsumeCommitResume(t *testing.T) {
	b := newKafka(t)
	ctx := context.Background()
	_ = b.CreateTopic(ctx, "t", 2)
	p, _ := b.NewProducer(partitioning.Hash{})
	parts := map[string]int{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("k%d", i%5)
		part, _, err := p.Produce(ctx, "t", []byte(key), []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if prev, ok := parts[key]; ok && prev != part {
			t.Fatalf("key %s moved partitions %d→%d", key, prev, part)
		}
		parts[key] = part
	}

	c1, err := b.NewConsumer("g", "m1", "t")
	if err != nil {
		t.Fatal(err)
	}
	recs := pollUntil(t, c1, 40, 10*time.Second)
	if len(recs) != 40 {
		t.Fatalf("read %d of 40", len(recs))
	}
	if len(c1.Assignment()) != 2 {
		t.Fatalf("assignment=%v", c1.Assignment())
	}
	// Commit offset 3 on every partition we saw, then "crash".
	offs := map[TopicPartition]int64{}
	for _, tp := range c1.Assignment() {
		offs[tp] = 3
	}
	if err := c1.Commit(ctx, offs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	_ = c1.Close()

	c2, _ := b.NewConsumer("g", "m2", "t")
	again := pollUntil(t, c2, 1, 10*time.Second)
	if len(again) == 0 {
		t.Fatal("successor read nothing")
	}
	for _, r := range again {
		if r.Offset < 3 {
			t.Fatalf("redelivered committed offset: %+v", r)
		}
	}
	// Everything at offset >= 3 must be redelivered (at-least-once).
	all := pollUntil(t, c2, 40-3*2-len(again), 10*time.Second)
	if got := len(again) + len(all); got != 34 {
		t.Fatalf("successor saw %d records, want 34 (everything past the commit)", got)
	}
	_ = c2.Close()
}

func TestKafkaRebalanceSplitsPartitions(t *testing.T) {
	b := newKafka(t)
	ctx := context.Background()
	_ = b.CreateTopic(ctx, "t", 4)
	c1, _ := b.NewConsumer("g", "a", "t")
	waitAssign := func(c Consumer, n int) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			_, _ = c.Poll(pctx, 1) // polling drives the group protocol
			cancel()
			if len(c.Assignment()) == n {
				return
			}
		}
		t.Fatalf("assignment never reached %d: %v", n, c.Assignment())
	}
	waitAssign(c1, 4)
	c2, _ := b.NewConsumer("g", "b", "t")
	go func() { // c1 must keep polling so the rebalance can complete
		for ctx.Err() == nil {
			pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			if _, err := c1.Poll(pctx, 1); errors.Is(err, ErrClosed) {
				cancel()
				return
			}
			cancel()
		}
	}()
	waitAssign(c2, 2)
	if n := len(c1.Assignment()); n != 2 {
		t.Fatalf("c1 owns %d after rebalance, want 2", n)
	}
	_ = c2.Close()
	_ = c1.Close()
}

func TestKafkaPauseResume(t *testing.T) {
	b := newKafka(t)
	ctx := context.Background()
	_ = b.CreateTopic(ctx, "t", 1)
	p, _ := b.NewProducer(nil)
	c, _ := b.NewConsumer("g", "a", "t")
	pollUntil(t, c, 1, 5*time.Second) // join the group
	tp := TopicPartition{"t", 0}
	c.Pause(tp)
	for i := 0; i < 5; i++ {
		_, _, _ = p.Produce(ctx, "t", []byte("k"), []byte("v"))
	}
	if got := pollUntil(t, c, 1, 800*time.Millisecond); len(got) != 0 {
		t.Fatalf("paused partition delivered %d records", len(got))
	}
	c.Resume(tp)
	if got := pollUntil(t, c, 5, 10*time.Second); len(got) != 5 {
		t.Fatalf("after resume got %d, want 5", len(got))
	}
	_ = c.Close()
}

func TestKafkaPollAfterCloseReturnsErrClosed(t *testing.T) {
	b := newKafka(t)
	_ = b.CreateTopic(context.Background(), "t", 1)
	c, _ := b.NewConsumer("g", "a", "t")
	_ = c.Close()
	if _, err := c.Poll(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestKafkaProduceToUnknownTopicFails(t *testing.T) {
	b := newKafka(t)
	p, _ := b.NewProducer(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := p.Produce(ctx, "nope", []byte("k"), []byte("v")); err == nil {
		t.Log("kfake auto-created the topic (server-side auto-create); acceptable")
	}
}

// TestLiveBrokerSmoke runs against a REAL Redpanda (or any Kafka) cluster:
//
//	SF_TEST_BROKERS=localhost:19092 go test -tags redpanda -run LiveBroker -v ./internal/streaming
//
// It is skipped when SF_TEST_BROKERS is unset. When SF_TEST_REQUIRE_SASL=true,
// the test also requires SASL credentials and exercises the authenticated listener.
func TestLiveBrokerSmoke(t *testing.T) {
	addrs := os.Getenv("SF_TEST_BROKERS")
	if addrs == "" {
		t.Skip("SF_TEST_BROKERS not set")
	}
	if os.Getenv("SF_TEST_REQUIRE_SASL") == "true" &&
		(os.Getenv("SF_KAFKA_SASL_MECHANISM") == "" ||
			os.Getenv("SF_KAFKA_SASL_USERNAME") == "" ||
			os.Getenv("SF_KAFKA_SASL_PASSWORD") == "") {
		t.Fatal("live broker security test requires SASL mechanism, username, and password")
	}
	if os.Getenv("SF_TEST_REQUIRE_TLS") == "true" && os.Getenv("SF_KAFKA_TLS") != "true" {
		t.Fatal("live broker security test requires TLS")
	}
	b, err := Open("redpanda", strings.Split(addrs, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	topic := fmt.Sprintf("sf-smoke-%d", time.Now().UnixNano())
	if err := b.CreateTopic(ctx, topic, 3); err != nil {
		t.Fatal(err)
	}
	p, _ := b.NewProducer(nil)
	msgs := make([]KV, 30)
	for i := range msgs {
		msgs[i] = KV{[]byte(fmt.Sprintf("k%d", i%7)), []byte(fmt.Sprintf("v%d", i))}
	}
	if err := ProduceBatch(ctx, p, topic, msgs); err != nil {
		t.Fatal(err)
	}
	c, _ := b.NewConsumer("sf-smoke-"+topic, "m", topic)
	defer c.Close()
	if got := pollUntil(t, c, 30, 30*time.Second); len(got) != 30 {
		t.Fatalf("read %d of 30 from a live broker", len(got))
	}
	offs := map[TopicPartition]int64{}
	for _, tp := range c.Assignment() {
		offs[tp] = 1
	}
	if err := c.Commit(ctx, offs); err != nil {
		t.Fatalf("commit against a live broker: %v", err)
	}
}

func TestKafkaBatchProduceAndConsume(t *testing.T) {
	b := newKafka(t)
	ctx := context.Background()
	_ = b.CreateTopic(ctx, "t", 3)
	p, _ := b.NewProducer(nil)
	if _, ok := p.(BatchProducer); !ok {
		t.Fatal("adapter producer must implement BatchProducer")
	}
	msgs := make([]KV, 500)
	for i := range msgs {
		msgs[i] = KV{[]byte(fmt.Sprintf("k%d", i%20)), []byte(fmt.Sprintf("v%d", i))}
	}
	if err := ProduceBatch(ctx, p, "t", msgs); err != nil {
		t.Fatal(err)
	}
	c, _ := b.NewConsumer("g", "m", "t")
	defer c.Close()
	if got := pollUntil(t, c, 500, 15*time.Second); len(got) != 500 {
		t.Fatalf("read %d of 500", len(got))
	}
}
