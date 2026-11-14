// k6 load test for the HTTP ingestion API.
//
//   k6 run -e TARGET=http://localhost:8081 tests/load/ingest.js
//
// Not yet executed against a live stack (no k6/Docker in the authoring
// environment). Record real runs in benchmarks/RESULTS.md with full context.
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'http://localhost:8081';
const BATCH = parseInt(__ENV.BATCH || '50');
const throttled = new Rate('throttled_429');
const batchLatency = new Trend('batch_latency_ms', true);

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate', startRate: 50, timeUnit: '1s',
      preAllocatedVUs: 50, maxVUs: 400,
      stages: [
        { target: 200, duration: '30s' },   // warm-up
        { target: 1000, duration: '2m' },   // sustained load: requests/s × BATCH = events/s
        { target: 0, duration: '15s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    batch_latency_ms: ['p(95)<250', 'p(99)<750'],
  },
};

const kinds = ['iot', 'clickstream', 'market'];

export default function () {
  const now = Date.now();
  const batch = [];
  for (let i = 0; i < BATCH; i++) {
    const k = kinds[Math.floor(Math.random() * 3)];
    batch.push({
      id: `${__VU}-${__ITER}-${i}-${now}`,          // unique ⇒ exercises dedup only if retried
      kind: k,
      key: `${k}-${Math.floor(Math.random() * 500)}`,
      value: Math.random() * 100,
      event_time: new Date(now - Math.floor(Math.random() * 2000)).toISOString(), // ≤2 s disorder
    });
  }
  const res = http.post(`${TARGET}/v1/events`, JSON.stringify(batch), { headers: { 'Content-Type': 'application/json' } });
  batchLatency.add(res.timings.duration);
  throttled.add(res.status === 429);
  check(res, { 'accepted or throttled': (r) => r.status === 202 || r.status === 429 });
}
