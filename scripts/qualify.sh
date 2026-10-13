#!/usr/bin/env bash
# Linux qualification subset (REVAMP_PLAN §14 WP0.5 task 10): the same qual
# files and floors as scripts/qualify.ps1, run in release with
# `--ignored --test-threads=1`. Scenarios that need Windows-only features
# (WinFsp) skip themselves; FUSE scenarios need /dev/fuse.
#
# Usage:
#   CARGO_TARGET_DIR=$HOME/tkfs-target/<pkg> scripts/qualify.sh --wave 1 [--files "a b"] [--filter NAME]
#   scripts/qualify.sh --all | --baseline [--budget-s 120] [--file-timeout-min 90] [--evidence PATH] [--no-build]
#
# Evidence: test-evidence/qualification-<kind>-linux.json. Exit 0 when every
# enforced floor holds and every selected file passed (--baseline: always 0).
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)

wave=0; kind=""; files=""; filter=""; budget=120; file_timeout=90; evidence=""; build=1; baseline=0
while [ $# -gt 0 ]; do
  case "$1" in
    --wave) wave="$2"; shift 2 ;;
    --all) wave=4; kind="all"; shift ;;
    --baseline) wave=4; kind="baseline"; baseline=1; shift ;;
    --files) files="$2"; shift 2 ;;
    --filter) filter="$2"; shift 2 ;;
    --budget-s) budget="$2"; shift 2 ;;
    --file-timeout-min) file_timeout="$2"; shift 2 ;;
    --evidence) evidence="$2"; shift 2 ;;
    --no-build) build=0; shift ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
if [ "$wave" -lt 1 ] || [ "$wave" -gt 4 ]; then echo "use --wave 1..4, --all or --baseline" >&2; exit 2; fi
[ -n "$kind" ] || kind="wave-$wave"
[ -n "$evidence" ] || evidence="$root/test-evidence/qualification-$kind-linux.json"

declare -A owner_wave=(
  [qual_wp1_4_worker]=1 [qual_wp1_5_mount]=1 [qual_wp1_6_activation]=1 [qual_wp1_7_derive]=1
  [qual_wp1_8_objects]=1 [qual_wp1_9_session]=1 [qual_wp1_12_legacy]=1
  [qual_wp2_1_lifecycle]=2 [qual_wp2_3_worker]=2 [qual_wp2_4_offlock]=2 [qual_wp2_5_feed]=2
  [qual_wp3_1_transport]=3 [qual_wp3_2_objects]=3 [qual_wp3_int_e2e]=3 [qual_scale]=4
)
order="qual_wp1_4_worker qual_wp1_5_mount qual_wp1_6_activation qual_wp1_7_derive qual_wp1_8_objects
qual_wp1_9_session qual_wp1_12_legacy qual_wp2_1_lifecycle qual_wp2_3_worker qual_wp2_4_offlock
qual_wp2_5_feed qual_wp3_1_transport qual_wp3_2_objects qual_wp3_int_e2e qual_scale"
selected=()
for f in $order; do
  [ "${owner_wave[$f]}" -le "$wave" ] || continue
  if [ -n "$files" ] && [[ " $files " != *" $f "* ]]; then continue; fi
  selected+=("$f")
done
[ ${#selected[@]} -gt 0 ] || { echo "no qualification files selected" >&2; exit 2; }

run_dir="$root/test-runs/qualify-$kind-linux-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$run_dir" "$(dirname "$evidence")"
started=$(date +%s)
echo "qualify: $kind (linux), files: ${selected[*]}"
if [ "$build" = 1 ]; then
  args=(test --release --no-run)
  for f in "${selected[@]}"; do args+=(--test "$f"); done
  cargo "${args[@]}"
fi

statuses=()
for f in "${selected[@]}"; do
  echo "qualify: running $f (budget $budget s per scenario)"
  t0=$(date +%s)
  set +e
  TKFS_QUAL_OUT="$run_dir/$f.qual.jsonl" TKFS_QUAL_BUDGET_S="$budget" \
    timeout --kill-after=30 "$((file_timeout * 60))" nice -n 10 \
    cargo test --release --test "$f" -- --ignored --test-threads=1 --nocapture $filter \
    >"$run_dir/$f.log" 2>&1
  code=$?
  set -e
  if [ $code -eq 0 ]; then st=pass; elif [ $code -eq 124 ] || [ $code -eq 137 ]; then st=timeout; else st=fail; fi
  statuses+=("$f=$st=$(( $(date +%s) - t0 ))")
  echo "qualify: $f -> $st"
done

python3 - "$root" "$run_dir" "$evidence" "$kind" "$wave" "$baseline" "$budget" "$file_timeout" "$filter" "$started" "${statuses[@]}" <<'PY'
import json, os, platform, subprocess, sys, time
root, run_dir, evidence, kind, wave, baseline, budget, file_timeout, flt, started = sys.argv[1:11]
statuses = sys.argv[11:]
wave, baseline = int(wave), baseline == "1"
def git(*a):
    try: return subprocess.check_output(["git", *a], cwd=root, text=True).strip()
    except Exception: return ""
floors = json.load(open(os.path.join(root, "tests/qualification-floors.json")))["floors"]
records, files = [], []
for entry in statuses:
    name, st, secs = entry.split("=")
    recs = []
    path = os.path.join(run_dir, name + ".qual.jsonl")
    if os.path.exists(path):
        recs = [json.loads(l) for l in open(path) if l.strip()]
    records += recs
    scenarios = []
    for sc in dict.fromkeys(r["scenario"] for r in recs if r.get("status") == "started"):
        finals = [r for r in recs if r.get("scenario") == sc and r.get("status") not in (None, "started")]
        final = finals[-1] if finals else None
        scenarios.append({
            "scenario": sc,
            "status": final["status"] if final else ("killed" if st == "timeout" else "incomplete"),
            "elapsed_ms": final.get("elapsed_ms") if final else None,
            "failures": final.get("failures", []) if final else [],
            "notes": final.get("notes", []) if final else [],
            "unsupported": [r["unsupported"] for r in recs if r.get("scenario") == sc and "unsupported" in r],
            "metrics": [{"metric": r["metric"], "value": r["value"], "unit": r.get("unit")}
                        for r in recs if r.get("scenario") == sc and "metric" in r],
        })
    log = os.path.join(run_dir, name + ".log")
    result = None
    if os.path.exists(log):
        lines = [l.strip() for l in open(log, errors="replace") if l.startswith("test result:")]
        result = lines[-1] if lines else None
    files.append({"file": name, "status": st, "duration_s": int(secs), "result": result,
                  "log": log, "scenarios": scenarios})
selected = {f["file"] for f in files}
ops = {"<": lambda v, l: v < l, "<=": lambda v, l: v <= l, ">": lambda v, l: v > l,
       ">=": lambda v, l: v >= l, "==": lambda v, l: v == l}
results = []
for fl in floors:
    if fl["scenario"].split("::")[0] not in selected: continue
    ran = any(r.get("scenario") == fl["scenario"] and r.get("status") == "started" for r in records)
    metric = [r for r in records if r.get("scenario") == fl["scenario"] and r.get("metric") == fl["metric"]]
    measured = metric[-1]["value"] if metric else None
    if not ran: verdict = "not_run"
    elif not metric: verdict = "missing"
    elif measured is not None and ops[fl["op"]](measured, fl["value"]): verdict = "pass"
    else: verdict = "fail"
    enforced = (not baseline) and fl["wave"] <= wave and (ran or not flt)
    results.append({**{k: fl[k] for k in ("id", "wave", "scenario", "metric", "op", "unit", "baseline")},
                    "floor": fl["value"], "measured": measured, "verdict": verdict, "enforced": enforced})
scen = [s for f in files for s in f["scenarios"]]
by_status = {}
for s in scen: by_status[s["status"]] = by_status.get(s["status"], 0) + 1
enforced_failures = [r["id"] for r in results if r["enforced"] and r["verdict"] != "pass"]
ok = not enforced_failures and all(s["status"] in ("pass", "skipped") for s in scen) \
    and all(f["status"] == "pass" for f in files)
try: cpu = [l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")][0]
except Exception: cpu = platform.processor()
try: mem = round(int(open("/proc/meminfo").readline().split()[1]) / 1048576, 1)
except Exception: mem = None
doc = {
    "schema": 1, "kind": kind, "platform": "linux", "wave": wave,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "duration_s": int(time.time()) - int(started),
    "git": {"commit": git("rev-parse", "HEAD"), "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
            "dirty": bool(git("status", "--porcelain"))},
    "toolchain": subprocess.run(["rustc", "-V"], capture_output=True, text=True).stdout.strip(),
    "hardware": {"cpu": cpu, "threads": os.cpu_count(), "memory_gb": mem, "os": platform.platform(),
                 "fuse": os.path.exists("/dev/fuse")},
    "settings": {"budget_s": int(budget), "file_timeout_min": int(file_timeout), "filter": flt,
                 "profile": "release"},
    "summary": {"ok": None if baseline else ok, "scenarios": len(scen), "by_status": by_status,
                "floors_evaluated": len(results),
                "floors_passed": sum(r["verdict"] == "pass" for r in results),
                "floors_failed": sum(r["verdict"] == "fail" for r in results),
                "floors_missing": sum(r["verdict"] == "missing" for r in results),
                "floors_not_run": sum(r["verdict"] == "not_run" for r in results),
                "enforced_failures": enforced_failures},
    "files": files, "floors": results, "run_dir": run_dir,
}
json.dump(doc, open(evidence, "w"), indent=2)
print(f"qualify: evidence written to {evidence}")
print(f"qualify: scenarios {len(scen)}; floors pass {doc['summary']['floors_passed']}, "
      f"fail {doc['summary']['floors_failed']}, missing {doc['summary']['floors_missing']}")
sys.exit(0 if baseline or ok else 1)
PY
