#!/usr/bin/env bash
# End-to-end verification WITHOUT Docker: runs the real producer/processor/api
# binaries as separate OS processes against real ClickHouse, a Kafka-protocol broker
# (tests/tools/kafkasim, built on franz-go's kfake), Prometheus, k6 and an
# OpenTelemetry Collector. It is the compose stack minus the containers.
#
# Prerequisites (paths overridable via env): a running ClickHouse at $CH_URL and the
# binaries listed below. Build ours with:
#     go build -tags redpanda -o "$BIN" ./cmd/... ./tests/tools/...
#
# Exit status is non-zero if any check fails. See docs/VERIFICATION.md for results.
set -uo pipefail

BIN=${BIN:-/tmp/sf2}
WORK=${WORK:-/tmp/sf-e2e}
CH_URL=${CH_URL:-http://127.0.0.1:8123}
K6=${K6:-k6}
PROM=${PROM:-prometheus}
OTEL=${OTEL:-otelcol-contrib}
DASH=${DASH:-$(dirname "$0")/../deployments/grafana/dashboards/streamforge.json}
K6_SCRIPT=${K6_SCRIPT:-$(dirname "$0")/../tests/load/ingest.js}
SEED=${SEED:-99}; COUNT=${COUNT:-30000}
FAILS=0
say(){ printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok(){ printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILS=$((FAILS+1)); }
PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 1; for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done; }
trap cleanup EXIT
spawn(){ local name=$1; shift; "$@" >"$WORK/$name.log" 2>&1 & PIDS+=($!); eval "PID_$name=$!"; }
waitfor(){ local what=$1 url=$2; for _ in $(seq 1 60); do curl -fs -m 2 "$url" >/dev/null 2>&1 && return 0; sleep 0.5; done; bad "$what never became ready ($url)"; return 1; }
jget(){ python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }
chq(){ curl -fs -m 10 "$CH_URL/" --data-binary "$1"; }

rm -rf "$WORK"; mkdir -p "$WORK"
say "0. Reset ClickHouse and create an authenticated user (exercises the auth-header path)"
chq "DROP DATABASE IF EXISTS streamforge" >/dev/null
chq "CREATE USER IF NOT EXISTS streamforge IDENTIFIED BY 'streamforge'" >/dev/null
chq "GRANT CREATE, INSERT, SELECT ON *.* TO streamforge" >/dev/null && ok "ClickHouse reachable, least-privilege user created: CREATE, INSERT, SELECT only ($(curl -fs "$CH_URL/?query=SELECT%20version()"))"

say "1. Start infrastructure: Kafka-protocol broker, OTel collector, Prometheus"
spawn kafkasim "$BIN/kafkasim" -port 19092
cat >"$WORK/otel.yaml" <<YAML
receivers: { otlp: { protocols: { http: { endpoint: "127.0.0.1:4318" } } } }
exporters: { file: { path: "$WORK/traces.jsonl" } }
service:
  telemetry: { metrics: { level: none }, logs: { level: warn } }
  pipelines: { traces: { receivers: [otlp], exporters: [file] } }
YAML
spawn otel "$OTEL" --config "$WORK/otel.yaml"
cat >"$WORK/prom.yml" <<YAML
global: { scrape_interval: 2s }
scrape_configs:
  - { job_name: processor, static_configs: [ { targets: ["127.0.0.1:9101","127.0.0.1:9102","127.0.0.1:9103"] } ] }
  - { job_name: producer,  static_configs: [ { targets: ["127.0.0.1:8081"] } ] }
  - { job_name: api,       static_configs: [ { targets: ["127.0.0.1:8080"] } ] }
YAML
spawn prom "$PROM" --config.file="$WORK/prom.yml" --storage.tsdb.path="$WORK/tsdb" --web.listen-address=127.0.0.1:9090
waitfor prometheus http://127.0.0.1:9090/-/ready && ok "Prometheus ready"
sleep 1; (exec 3<>/dev/tcp/127.0.0.1/19092) 2>/dev/null && ok "kafkasim accepting connections on :19092" || bad "kafkasim not listening"
(exec 3<>/dev/tcp/127.0.0.1/4318) 2>/dev/null && ok "OTel collector accepting OTLP/HTTP on :4318" || bad "otel collector not listening"

export SF_BROKER=redpanda SF_BROKERS=127.0.0.1:19092 SF_TOPIC=events SF_PARTITIONS=6
export SF_STORE=clickhouse SF_CLICKHOUSE_URL="$CH_URL" SF_CLICKHOUSE_USER=streamforge SF_CLICKHOUSE_PASSWORD=streamforge
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 SF_TRACE_SAMPLE=0.05 SF_KAFKA_SESSION_TIMEOUT=8s SF_KAFKA_HEARTBEAT_INTERVAL=2s
PFLAGS=(-topic events -group streamforge-processors -window 1s -max-ooo 3500ms -lateness 5s -idle 2s)

say "2. Start 2 processors + analytics API (real processes, real ClickHouse via authenticated HTTP)"
spawn p1 "$BIN/processor" "${PFLAGS[@]}" -member p1 -metrics-addr 127.0.0.1:9101
spawn p2 "$BIN/processor" "${PFLAGS[@]}" -member p2 -metrics-addr 127.0.0.1:9102
spawn api "$BIN/api" -addr 127.0.0.1:8080
waitfor api http://127.0.0.1:8080/healthz && ok "API healthy"
waitfor "api readiness (store reachable)" http://127.0.0.1:8080/readyz && ok "/readyz proves ClickHouse reachable with credentials"
waitfor p1 http://127.0.0.1:9101/healthz; waitfor p2 http://127.0.0.1:9102/healthz
chq "EXISTS TABLE streamforge.window_results" | grep -q 1 && ok "schema auto-created by the processors"

say "3. PHASE A — deterministic $COUNT-message run, with a processor kill -9 mid-stream"
EXPECT=$("$BIN/genstats" -seed "$SEED" -count "$COUNT" | sed -E 's/.*unique=([0-9]+).*/\1/')
echo "  ground truth for seed $SEED: $EXPECT unique events ($("$BIN/genstats" -seed "$SEED" -count "$COUNT"))"
"$BIN/producer" -generate -rate 3000 -count "$COUNT" -seed "$SEED" -disorder 0.1 -dup 0.02 -poison 0.005 >"$WORK/producerA.log" 2>&1 &
PA=$!
sleep 4; kill -9 "$PID_p1" && echo "  >>> kill -9 p1 (pid $PID_p1) at t+4s — no flush, no commit, no leave-group"
sleep 6; spawn p1b "$BIN/processor" "${PFLAGS[@]}" -member p1b -metrics-addr 127.0.0.1:9103; echo "  >>> started replacement p1b"
wait "$PA"; echo "  producer A finished: $(grep -o 'published=[0-9]*' "$WORK/producerA.log" | tail -1)"
GOT=-1
for i in $(seq 1 90); do
  GOT=$(curl -fs "http://127.0.0.1:8080/v1/summary?since=2h" | jget "d['total_events']" 2>/dev/null || echo -1)
  [ "$GOT" = "$EXPECT" ] && break; sleep 1
done
[ "$GOT" = "$EXPECT" ] && ok "EXACT after crash+recovery: ClickHouse holds $GOT events == $EXPECT unique produced" || bad "count mismatch: ClickHouse=$GOT expected=$EXPECT"
sleep 3
GOT2=$(curl -fs "http://127.0.0.1:8080/v1/summary?since=2h" | jget "d['total_events']")
[ "$GOT2" = "$EXPECT" ] && ok "count stable after settling (no drift / double count)" || bad "count drifted to $GOT2"
chq "SELECT count() FROM streamforge.window_results" | xargs -I{} echo "  raw rows before FINAL dedup: {}"
chq "SELECT sum(event_count) FROM streamforge.window_results FINAL" | grep -qx "$EXPECT" && ok "FINAL read agrees with API" || bad "FINAL sum differs"

say "4. PHASE B — k6 load against the HTTP ingestion API"
spawn producerB "$BIN/producer" -generate=false -serve 127.0.0.1:8081
waitfor "ingestion API" http://127.0.0.1:8081/healthz
BEFORE=$(curl -fs "http://127.0.0.1:8080/v1/summary?since=2h" | jget "d['total_events']")
"$K6" run --quiet -e TARGET=http://127.0.0.1:8081 -e SCALE="${K6_SCALE:-0.2}" -e RATE="${K6_RATE:-0.05}" --summary-trend-stats="avg,med,p(90),p(95),p(99),max" --summary-export="$WORK/k6.json" "$K6_SCRIPT" >"$WORK/k6.log" 2>&1
K6RC=$?
python3 - "$WORK/k6.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))["metrics"]
g=lambda k,f: d.get(k,{}).get(f)
print("  k6: requests=%s failed_rate=%s  batch_latency p95=%.1fms p99=%.1fms  throttled=%s" % (g("http_reqs","count"), g("http_req_failed","value"), g("batch_latency_ms","p(95)") or 0, g("batch_latency_ms","p(99)") or 0, g("throttled_429","value")))
PY
[ $K6RC -eq 0 ] && ok "k6 thresholds passed (p95<250ms, p99<750ms, <1% failed)" || bad "k6 exited $K6RC (a threshold failed — see $WORK/k6.log)"
PUB=$(curl -fs http://127.0.0.1:8081/metrics | awk '/^sf_ingest_published_total/{print $2}')
echo "  producer B published $PUB events (all unique ids from k6)"
EXPECT_B=$((EXPECT + PUB)); GOTB=-1
for i in $(seq 1 90); do
  GOTB=$(curl -fs "http://127.0.0.1:8080/v1/summary?since=2h" | jget "d['total_events']" 2>/dev/null || echo -1)
  [ "$GOTB" = "$EXPECT_B" ] && break; sleep 1
done
[ "$GOTB" = "$EXPECT_B" ] && ok "EXACT after load: $GOTB == $EXPECT (A) + $PUB (k6)" || bad "after load ClickHouse=$GOTB expected=$EXPECT_B"

say "5. Validate every Grafana dashboard query against real Prometheus"
sleep 4
python3 - "$DASH" <<'PY' || FAILS=$((FAILS+1))
import json,sys,urllib.request,urllib.parse
d=json.load(open(sys.argv[1])); bad=0
for p in d["panels"]:
    for t in p["targets"]:
        q=t["expr"]
        try:
            r=json.load(urllib.request.urlopen("http://127.0.0.1:9090/api/v1/query?"+urllib.parse.urlencode({"query":q}),timeout=10))
            n=len(r["data"]["result"]); status=r["status"]
        except Exception as e:
            print("  FAIL  [%s] %s -> %s"%(p["title"],q[:70],e)); bad+=1; continue
        tag="PASS" if status=="success" and n>0 else ("WARN" if status=="success" else "FAIL")
        if tag=="FAIL": bad+=1
        print("  %s  [%s] %d series  %s"%(tag,p["title"],n,"" if n else "(valid query, no data in this run)"))
sys.exit(1 if bad else 0)
PY

say "6. OpenTelemetry: inspect traces received by the real Collector"
kill -TERM "$PID_otel" 2>/dev/null; sleep 2
python3 - "$WORK/traces.jsonl" <<'PY' || FAILS=$((FAILS+1))
import json,sys,collections
spans=[]
for line in open(sys.argv[1]):
    d=json.loads(line)
    for rs in d.get("resourceSpans",[]):
        svc=[a["value"].get("stringValue") for a in rs["resource"].get("attributes",[]) if a["key"]=="service.name"]
        for ss in rs["scopeSpans"]:
            for s in ss["spans"]: s["_svc"]=svc[0] if svc else "?"; spans.append(s)
by={s["spanId"]:s for s in spans}
names=collections.Counter((s["_svc"],s["name"]) for s in spans)
print("  spans received by collector:",len(spans))
for (svc,n),c in sorted(names.items()): print("    %-24s %-16s %d"%(svc,n,c))
linked=[s for s in spans if s["name"]=="process events" and s.get("parentSpanId") in by and by[s["parentSpanId"]]["name"]=="publish" and by[s["parentSpanId"]]["traceId"]==s["traceId"]]
cross=[s for s in linked if by[s["parentSpanId"]]["_svc"]!=s["_svc"]]
print("  publish->process links:",len(linked),"| crossing a process boundary (producer->processor):",len(cross))
ok=len(cross)>0 and any(n=="sink.write" for _,n in names)
print("  PASS  one trace spans producer, broker and processor processes" if ok else "  FAIL  no cross-process trace found")
sys.exit(0 if ok else 1)
PY

say "7. Graceful shutdown of a live processor (SIGTERM) exits 0"
kill -TERM "$PID_p2"; wait "$PID_p2" 2>/dev/null; RC=$?
[ $RC -eq 0 ] && ok "p2 exited cleanly (status 0)" || bad "p2 exit status $RC"
grep -q 'graceful=true' "$WORK/p2.log" 2>/dev/null && ok "p2 log confirms graceful drain+commit" || echo "  (info) no graceful marker at warn log level"

say "RESULT"
if [ "$FAILS" -eq 0 ]; then printf '\033[32mALL CHECKS PASSED\033[0m\n'; else printf '\033[31m%d CHECK(S) FAILED\033[0m  (logs in %s)\n' "$FAILS" "$WORK"; fi
exit "$FAILS"
