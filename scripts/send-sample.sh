#!/usr/bin/env bash
# Post a few hand-written events to the ingestion API (default :8081).
set -euo pipefail
T="${1:-http://localhost:8081}"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -sS -X POST "$T/v1/events" -H 'Content-Type: application/json' -d "[
  {\"kind\":\"iot\",\"key\":\"sensor-001\",\"value\":21.7,\"event_time\":\"$NOW\"},
  {\"kind\":\"market\",\"key\":\"BTC-USD\",\"value\":64123.5,\"event_time\":\"$NOW\"},
  {\"kind\":\"clickstream\",\"key\":\"user-0042\",\"value\":1834,\"event_time\":\"$NOW\",\"attrs\":{\"page\":\"/cart\"}}
]"
echo
