#!/usr/bin/env python3
"""Static verification of deployments/docker-compose.yml.

This does NOT run Docker. It checks the things that can be checked without it:

  1. The YAML parses and every service referenced by depends_on exists.
  2. Each app service's `command:` flags are accepted by the REAL binary (run for a
     couple of seconds with the broker/store swapped for in-memory ones) — a typo'd
     flag exits with status 2 immediately.
  3. Every SF_* / OTEL_* variable the compose file sets is actually read somewhere in
     the Go source (catches renamed or misspelled env vars that would silently do nothing).
  4. Prometheus scrape targets name services and ports that exist in the compose file.

Usage:  scripts/check-compose.py [BIN_DIR]     (BIN_DIR defaults to ./bin; COMPOSE=path overrides the file)
"""
import os, re, subprocess, sys, pathlib, yaml

root = pathlib.Path(__file__).resolve().parent.parent
bindir = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else root / "bin")
compose = yaml.safe_load(open(os.environ.get("COMPOSE", root / "deployments/docker-compose.yml")))
prom = yaml.safe_load(open(root / "deployments/prometheus/prometheus.yml"))
services = compose["services"]
fails = 0
def ok(m):  print("  PASS", m)
def bad(m):
    global fails; fails += 1; print("  FAIL", m)

print("1. structure")
for name, svc in services.items():
    deps = svc.get("depends_on", {})
    for d in (deps if isinstance(deps, list) else deps.keys()):
        (ok if d in services else bad)(f"{name} depends_on {d}")

print("2. command flags accepted by the real binaries")
default_entry = "processor"  # Dockerfile ENTRYPOINT
for name, svc in services.items():
    if "build" not in svc and not any(k in svc for k in ("command",)) or "image" in svc and "build" not in svc:
        continue
    entry = default_entry
    if "entrypoint" in svc:
        entry = os.path.basename(svc["entrypoint"][0])
    args = [str(a) for a in svc.get("command", [])]
    binpath = bindir / entry
    if not binpath.exists():
        bad(f"{name}: binary {binpath} not built"); continue
    env = dict(os.environ, SF_BROKER="memory", SF_STORE="memory", HOSTNAME=name)
    cmd = [str(binpath)] + args
    if entry == "producer":
        cmd += ["-broker", "memory"]
    if entry == "processor":
        cmd += ["-broker", "memory", "-store", "memory", "-metrics-addr", "127.0.0.1:0"]
    if entry == "api":
        cmd += ["-store", "memory", "-addr", "127.0.0.1:0"]
    # rewrite listen addresses so parallel checks can't collide on ports
    cmd = [("127.0.0.1:0" if re.fullmatch(r":\d+", a) else a) for a in cmd]
    try:
        p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=2.5)
        # exited on its own within 2.5s => startup failed
        (bad if p.returncode in (2, 1) else ok)(f"{name}: `{entry} {' '.join(args)}` exited {p.returncode}: {p.stderr.strip()[:160]}" if p.returncode in (2,1) else f"{name}: flags accepted")
    except subprocess.TimeoutExpired:
        ok(f"{name}: `{entry} {' '.join(args)}` started and kept running")

print("3. environment variables are actually consumed")
src = "".join(p.read_text() for p in root.rglob("*.go") if "_test" not in p.name)
envs = set()
for svc in services.values():
    e = svc.get("environment", {})
    envs |= set(e.keys() if isinstance(e, dict) else [x.split("=")[0] for x in e])
anchor = compose.get("x-app", {}).get("environment", {})
envs |= set(anchor.keys())
for v in sorted(envs):
    if not v.startswith(("SF_", "OTEL_")):
        continue
    (ok if v in src else bad)(f"{v} is read by the code")

print("4. prometheus targets exist in compose")
ports = {"processor": "9100", "producer": "8081", "api": "8080"}
for sc in prom["scrape_configs"]:
    for sd in sc.get("dns_sd_configs", []):
        for n in sd["names"]:
            (ok if n in services else bad)(f"{sc['job_name']}: service {n} exists (port {sd['port']})")
    for st in sc.get("static_configs", []):
        for t in st["targets"]:
            host, port = t.split(":")
            (ok if host in services and ports.get(host) == port else bad)(f"{sc['job_name']}: {t} matches a service+port")

print("\nRESULT:", "ALL STATIC CHECKS PASSED" if not fails else f"{fails} CHECK(S) FAILED")
sys.exit(1 if fails else 0)
