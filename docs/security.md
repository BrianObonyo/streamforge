# Kafka transport security

The Redpanda/Kafka adapter reads security settings from environment variables.
They are applied consistently to its topic-admin, producer, and consumer clients.
The in-memory broker does not use network security settings.

## TLS

```text
SF_KAFKA_TLS=true
SF_KAFKA_TLS_CA=/path/to/ca.pem                 # optional custom CA bundle
SF_KAFKA_TLS_CERT=/path/to/client.crt           # optional client certificate
SF_KAFKA_TLS_KEY=/path/to/client.key            # required with client certificate
SF_KAFKA_TLS_SERVER_NAME=kafka.example.internal # optional SNI/verification name
```

TLS enforces at least TLS 1.2 and verifies broker certificates. Without a custom
CA, Go's system trust roots are used. Supplying a custom CA adds it to the system
roots. A client certificate and key enable mutual TLS. Certificate/key file paths
must be readable by the process (including inside its container).

## SASL

Choose one supported mechanism and provide credentials:

```text
SF_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
SF_KAFKA_SASL_USERNAME=<username>
SF_KAFKA_SASL_PASSWORD=<secret>
```

Supported mechanisms are `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.
`PLAIN` is rejected unless TLS is enabled because its credentials otherwise travel
without transport encryption. SCRAM does not send the raw password, but TLS is
still recommended to protect traffic and metadata. No secret is accepted as a
command-line argument or embedded in the repository's example configuration.

Configuration errors (unsupported mechanism, partial credentials, unreadable CA,
or unmatched client certificate/key) fail broker initialization rather than
silently falling back to an unsecured connection.

## Docker Compose

The Compose stack passes these optional `SF_KAFKA_*` environment variables to
application containers. For TLS files, mount the CA/client certificate/key into
the app containers and set paths to their in-container locations. Provide secrets
through the deployment environment or a secret manager; do not commit a populated
`.env` file.

## Verification limits

Build-tagged adapter tests exercise PLAIN-over-TLS and SCRAM authentication,
invalid credentials, and configuration validation against franz-go's `kfake`.
The `redpanda-live` CI job configures an ephemeral TLS certificate and
SCRAM-SHA-256 credential on a real Redpanda listener and verifies authenticated
client operations. mTLS, production ACL policy, and deployment-specific
certificate provisioning still require verification against the target cluster.
