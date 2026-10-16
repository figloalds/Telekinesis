#!/usr/bin/env bash
[ -n "${TKFS_QUALIFY_LF:-}" ] || exec env TKFS_QUALIFY_LF=1 bash -c "$(tr -d '\r' < "$0")" "$0" "$@" # CRLF-safe: re-run without CRs
# Linux qualification subset (REVAMP_PLAN §14 WP0.5 task 10): the same qual
# files and floors as scripts/qualify.ps1, run in release with
# `--ignored --test-threads=1`. Scenarios that need Windows-only features
# (WinFsp) skip themselves; FUSE scenarios need /dev/fuse.
#
# Run it with `bash scripts/qualify.sh ...`. Line 2 re-executes the script with
# carriage returns stripped (`tr -d '\r'`, written as an escape: never put a
# literal CR or LF between those quotes), so a Windows checkout
# (core.autocrlf=true, CRLF) still works from WSL on /mnt/c.
# tests/harness_selftest.rs::qualify_sh_runs_with_lf_and_crlf guards this.
#
# Usage:
#   CARGO_TARGET_DIR=$HOME/tkfs-target/<pkg> scripts/qualify.sh --wave 1 [--files "a b"] [--filter NAME]
#   scripts/qualify.sh --all | --baseline [--budget-s 120] [--file-timeout-min 90] [--evidence PATH] [--no-build]
#   scripts/qualify.sh --all --merge --files "a b"   # re-run some files into an existing evidence
#
# Evidence: test-evidence/qualification-<kind>-linux.json. A file passes only
# when cargo exits 0 and its last `test result:` line says `ok.`; exit code and
# result line are recorded. Exit 0 when every enforced floor holds and every
# selected file passed (--baseline: always 0).
#
# The Linux subset (WP4.2): every qual file runs; a floor whose `platforms`
# list (tests/qualification-floors.json) does not name "linux" is evaluated
# and recorded but not enforced here (a floor that depends on Windows-only
# machinery or on the Windows dev box's disk). --all gives the long scenarios
# their budget (--budget-s 2400, --file-timeout-min 150) unless given. The
# evidence records the disk of the temporary directory and the machine's load
# at the start. Scenarios run with TKFS_QUAL_ENFORCE=0 so this script judges
# the floors (a scenario whose only failures are floors not enforced here
# counts as passed, with a note); functional failures still fail it. --merge
# keeps the other files' results from the existing
# evidence file and replaces only the files run now (as qualify.ps1 -Merge).
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)

wave=0; kind=""; files=""; filter=""; budget=""; file_timeout=""; evidence=""; build=1; baseline=0; merge=0
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
    --merge) merge=1; shift ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
if [ "$wave" -lt 1 ] || [ "$wave" -gt 4 ]; then echo "use --wave 1..4, --all or --baseline" >&2; exit 2; fi
if [ "$kind" = "all" ]; then
  [ -n "$budget" ] || budget=2400
  [ -n "$file_timeout" ] || file_timeout=150
fi
[ -n "$budget" ] || budget=120
[ -n "$file_timeout" ] || file_timeout=90
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
# Git state when the run starts (edits made during a long run do not belong to it).
git -C "$root" rev-parse HEAD >"$run_dir/git-head.txt" 2>/dev/null || true
git -C "$root" rev-parse --abbrev-ref HEAD >"$run_dir/git-branch.txt" 2>/dev/null || true
git -C "$root" status --porcelain >"$run_dir/git-status.txt" 2>/dev/null || true
# What else ran when this run started (the machine may be shared).
{ cat /proc/loadavg 2>/dev/null || true; } >"$run_dir/loadavg.txt"
{ ps -eo comm= 2>/dev/null | grep -cE '^(cargo|rustc|tkfs|ld|cc)$' || true; } >"$run_dir/build-processes.txt"
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
  TKFS_QUAL_OUT="$run_dir/$f.qual.jsonl" TKFS_QUAL_BUDGET_S="$budget" TKFS_QUAL_ENFORCE=0 \
    timeout --kill-after=30 "$((file_timeout * 60))" nice -n 10 \
    cargo test --release --test "$f" -- --ignored --test-threads=1 --nocapture $filter \
    >"$run_dir/$f.log" 2>&1
  code=$?
  set -e
  if [ $code -eq 0 ]; then st=pass; elif [ $code -eq 124 ] || [ $code -eq 137 ]; then st=timeout; else st=fail; fi
  statuses+=("$f=$st=$(( $(date +%s) - t0 ))=$code")
  echo "qualify: $f -> $st (exit $code)"
done

TKFS_QUALIFY_MERGE="$merge" python3 - "$root" "$run_dir" "$evidence" "$kind" "$wave" "$baseline" "$budget" "$file_timeout" "$filter" "$started" "${statuses[@]}" <<'PY'
import json, os, platform, subprocess, sys, time
root, run_dir, evidence, kind, wave, baseline, budget, file_timeout, flt, started = sys.argv[1:11]
statuses = sys.argv[11:]
wave, baseline = int(wave), baseline == "1"
def started_git(name):
    try: return open(os.path.join(run_dir, name)).read().strip()
    except Exception: return ""
floors = json.load(open(os.path.join(root, "tests/qualification-floors.json")))["floors"]
# Floors not enforced on Linux (their `platforms` do not name it).
excluded = {fl["id"] for fl in floors if "platforms" in fl and "linux" not in fl["platforms"]}
def only_excluded_floors(failures):
    ids = [f.split()[1].rstrip(":") for f in failures if f.startswith("FLOOR ")]
    return bool(failures) and len(ids) == len(failures) and all(i in excluded for i in ids)
records, files = [], []
for entry in statuses:
    name, st, secs, code = entry.split("=")
    recs = []
    path = os.path.join(run_dir, name + ".qual.jsonl")
    if os.path.exists(path):
        recs = [json.loads(l) for l in open(path) if l.strip()]
    records += recs
    scenarios = []
    for sc in dict.fromkeys(r["scenario"] for r in recs if r.get("status") == "started"):
        finals = [r for r in recs if r.get("scenario") == sc and r.get("status") not in (None, "started")]
        final = finals[-1] if finals else None
        status = final["status"] if final else ("killed" if st == "timeout" else "incomplete")
        notes = list(final.get("notes", [])) if final else []
        if status == "fail" and only_excluded_floors(final.get("failures", [])):
            status = "pass"
            notes.append("only floors not enforced on linux failed: " + "; ".join(final["failures"]))
        scenarios.append({
            "scenario": sc,
            "status": status,
            "elapsed_ms": final.get("elapsed_ms") if final else None,
            "failures": final.get("failures", []) if final else [],
            "notes": notes,
            "unsupported": [r["unsupported"] for r in recs if r.get("scenario") == sc and "unsupported" in r],
            "metrics": [{"metric": r["metric"], "value": r["value"], "unit": r.get("unit")}
                        for r in recs if r.get("scenario") == sc and "metric" in r],
        })
    log = os.path.join(run_dir, name + ".log")
    result = None
    if os.path.exists(log):
        lines = [l.strip() for l in open(log, errors="replace") if l.startswith("test result:")]
        result = lines[-1] if lines else None
    # Cross-check: a pass needs exit 0 and an `ok.` result line (as qualify.ps1).
    if st == "pass" and not (result or "").startswith("test result: ok."): st = "fail"
    files.append({"file": name, "status": st, "exit_code": int(code), "duration_s": int(secs),
                  "result": result,
                  "log": log, "scenarios": scenarios})
merged_from = None
if os.environ.get("TKFS_QUALIFY_MERGE") == "1" and os.path.exists(evidence):
    # Carry the other files' results (and their metrics, for the floors) forward.
    old = json.load(open(evidence))
    merged_from = {k: old.get(k) for k in ("generated_at", "git", "run_dir")}
    current = {f["file"] for f in files}
    kept = [f for f in old.get("files", []) if f["file"] not in current]
    for f in kept:
        for sc in f.get("scenarios", []):
            records.append({"scenario": sc["scenario"], "status": "started"})
            for m in sc.get("metrics", []):
                records.append({"scenario": sc["scenario"], "metric": m["metric"], "value": m["value"]})
    order = ["qual_wp1_4_worker", "qual_wp1_5_mount", "qual_wp1_6_activation", "qual_wp1_7_derive",
             "qual_wp1_8_objects", "qual_wp1_9_session", "qual_wp1_12_legacy", "qual_wp2_1_lifecycle",
             "qual_wp2_3_worker", "qual_wp2_4_offlock", "qual_wp2_5_feed", "qual_wp3_1_transport",
             "qual_wp3_2_objects", "qual_wp3_int_e2e", "qual_scale"]
    files = sorted(kept + files, key=lambda f: order.index(f["file"]) if f["file"] in order else 99)
selected = {f["file"] for f in files}
ops = {"<": lambda v, l: v < l, "<=": lambda v, l: v <= l, ">": lambda v, l: v > l,
       ">=": lambda v, l: v >= l, "==": lambda v, l: v == l}
results = []
for fl in floors:
    if fl["scenario"].split("::")[0] not in selected: continue
    ran = any(r.get("scenario") == fl["scenario"] and r.get("status") == "started" for r in records)
    metric = [r for r in records if r.get("scenario") == fl["scenario"] and r.get("metric") == fl["metric"]]
    measured = metric[-1]["value"] if metric else None
    status = next((sc["status"] for f in files for sc in f["scenarios"]
                   if sc["scenario"] == fl["scenario"]), None)
    if not ran: verdict = "not_run"
    elif not metric: verdict = "missing"
    elif status in ("timeout", "killed", "incomplete"): verdict = "incomplete"
    elif measured is not None and ops[fl["op"]](measured, fl["value"]): verdict = "pass"
    else: verdict = "fail"
    platform_ok = "platforms" not in fl or "linux" in fl["platforms"]
    enforced = (not baseline) and fl["wave"] <= wave and (ran or not flt) and platform_ok
    results.append({**{k: fl[k] for k in ("id", "wave", "scenario", "metric", "op", "unit", "baseline")},
                    "platforms": fl.get("platforms"),
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
def disk(role, path):
    info = {"role": role, "path": path}
    try:
        st = os.statvfs(path)
        info["free_gb"] = round(st.f_bavail * st.f_frsize / 2**30, 1)
        info["size_gb"] = round(st.f_blocks * st.f_frsize / 2**30, 1)
        out = subprocess.run(["df", "-T", path], capture_output=True, text=True).stdout.splitlines()
        if len(out) > 1:
            fields = out[1].split()
            info["device"], info["file_system"] = fields[0], fields[1]
    except Exception as e:
        info["error"] = str(e)
    return info
import tempfile
disks = [disk("temp", tempfile.gettempdir()),
         disk("target", os.environ.get("CARGO_TARGET_DIR") or os.path.join(root, "target"))]
try: wsl = "microsoft" in open("/proc/version").read().lower()
except Exception: wsl = False
load = {"loadavg": started_git("loadavg.txt"), "build_processes": started_git("build-processes.txt")}
doc = {
    "schema": 1, "kind": kind, "platform": "linux", "wave": wave,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "duration_s": int(time.time()) - int(started),
    "git": {"commit": started_git("git-head.txt"), "branch": started_git("git-branch.txt"),
            "dirty": bool(started_git("git-status.txt")),
            "dirty_paths": started_git("git-status.txt").splitlines()[:50]},
    "toolchain": subprocess.run(["rustc", "-V"], capture_output=True, text=True).stdout.strip(),
    "hardware": {"cpu": cpu, "threads": os.cpu_count(), "memory_gb": mem, "os": platform.platform(),
                 "fuse": os.path.exists("/dev/fuse"), "wsl": wsl, "disks": disks},
    "load_at_start": load,
    "settings": {"budget_s": int(budget), "file_timeout_min": int(file_timeout), "filter": flt,
                 "profile": "release"},
    "summary": {"ok": None if baseline else ok, "scenarios": len(scen), "by_status": by_status,
                "floors_evaluated": len(results),
                "floors_passed": sum(r["verdict"] == "pass" for r in results),
                "floors_failed": sum(r["verdict"] == "fail" for r in results),
                "floors_missing": sum(r["verdict"] == "missing" for r in results),
                "floors_not_run": sum(r["verdict"] == "not_run" for r in results),
                "floors_incomplete": sum(r["verdict"] == "incomplete" for r in results),
                "enforced_failures": enforced_failures},
    "files": files, "floors": results, "run_dir": run_dir, "merged_from": merged_from,
}
json.dump(doc, open(evidence, "w"), indent=2)
print(f"qualify: evidence written to {evidence}")
print(f"qualify: scenarios {len(scen)}; floors pass {doc['summary']['floors_passed']}, "
      f"fail {doc['summary']['floors_failed']}, missing {doc['summary']['floors_missing']}")
sys.exit(0 if baseline or ok else 1)
PY
