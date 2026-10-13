#![allow(dead_code)]
#![allow(unused_imports)]
//! Qualification harness (WP0.5). Shared by `tests/harness_selftest.rs` and every
//! `tests/qual_*.rs` file through `mod support;`.
//!
//! - `node`: daemon `Node` (real `tkfs orchestrator` processes), worker processes
//!   (`WorkerProc`), dynamic JSON calls (`raw_call`, worker `rpc`).
//! - `proxy`: `FaultProxy` (latency, bandwidth, stalls, resets, blackhole, refuse).
//! - `hung`: `HungServer` and `redirect_runtime_json`.
//! - `fixtures`: seeded histories and object sets.
//! - `metrics`: stopwatch, percentiles, per-PID CPU, `QUAL {json}` lines and floors.
//! - `legacy`: temp copies of the 6298d0b fixtures under `tests/fixtures/legacy`.
//! - this file: compatibility goldens (`tests/fixtures/compat`) and `assert_same_shape`.
//!
//! Everything here talks to the product through public paths that every wave keeps
//! (`tkfs::core`, `tkfs::runtime::{rpc, Discovery}`, `tkfs::ws`) or through the
//! real binary, so qualification files compile before new API variants exist.

pub mod fixtures;
pub mod hung;
pub mod legacy;
pub mod metrics;
pub mod node;
pub mod proxy;

pub use fixtures::*;
pub use hung::{HungServer, RuntimeRedirect, redirect_runtime_json};
pub use legacy::{LegacyDataDir, LegacyWorker, copy_legacy_datadir, copy_legacy_worker};
pub use metrics::*;
pub use node::*;
pub use proxy::FaultProxy;

use serde_json::{Map, Value};
use std::path::PathBuf;

/// `tests/fixtures/compat`, where the baseline daemon responses live.
pub fn compat_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compat")
}

/// Load one compatibility golden (`name` without `.json`).
pub fn golden(name: &str) -> Value {
    let path = compat_dir().join(format!("{name}.json"));
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("missing golden {}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap()
}

/// Keys whose numeric value is semantic and stays literal in a golden.
const LITERAL_NUMBERS: &[&str] = &[
    "lane",
    "connections",
    "configured_connections",
    "reserved_connections",
    "index",
    "count",
    "format",
    "version",
];

/// Replace volatile values with typed markers so a response can be stored as a
/// golden and re-captured identically.
///
/// - `known` maps volatile strings (installation, state, repo and device ids) to
///   symbolic names such as `<repo:source>`; they are substituted in values and keys.
/// - other UUIDs become `<id>`, `ws://`/`wss://` URLs `<endpoint>`, absolute paths
///   `<path>`, 64-hex hashes `<hash>`;
/// - `*_ms` keys become `<ms?>` (nullable timestamps), other numbers `<u64>`/`<f64>`
///   except the semantic keys in `LITERAL_NUMBERS`;
/// - `transport` strings become `<transport>` (the value changes at cutover, §7.3);
/// - `error`-like strings keep only their code: `<error:CODE>`.
///
/// Booleans, `null` and other strings stay literal.
pub fn normalize(value: &Value, known: &[(String, String)]) -> Value {
    normalize_at(None, value, known)
}

fn normalize_at(key: Option<&str>, value: &Value, known: &[(String, String)]) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(normalize_key(k, known), normalize_at(Some(k), v, known));
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| normalize_at(key, v, known)).collect())
        }
        Value::Number(n) => {
            let key = key.unwrap_or("");
            if key.ends_with("_ms") {
                Value::String("<ms?>".into())
            } else if LITERAL_NUMBERS.contains(&key) {
                value.clone()
            } else if n.is_f64() {
                Value::String("<f64>".into())
            } else {
                Value::String("<u64>".into())
            }
        }
        Value::Null if key.is_some_and(|k| k.ends_with("_ms")) => Value::String("<ms?>".into()),
        Value::String(s) => {
            let key = key.unwrap_or("");
            if let Some((_, name)) = known.iter().find(|(v, _)| v == s) {
                return Value::String(name.clone());
            }
            if key == "transport" {
                return Value::String("<transport>".into());
            }
            if key == "error" || key.ends_with("_error") {
                return Value::String(format!("<error:{}>", error_code(s)));
            }
            Value::String(normalize_string(s, known))
        }
        _ => value.clone(),
    }
}

fn normalize_key(key: &str, known: &[(String, String)]) -> String {
    if let Some((_, name)) = known.iter().find(|(v, _)| v == key) {
        return name.clone();
    }
    if is_uuid(key) {
        return "<id>".into();
    }
    key.to_owned()
}

fn normalize_string(s: &str, known: &[(String, String)]) -> String {
    if s.starts_with("ws://") || s.starts_with("wss://") {
        return "<endpoint>".into();
    }
    if is_uuid(s) {
        return "<id>".into();
    }
    if s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return "<hash>".into();
    }
    let absolute = s.starts_with('/')
        || s.starts_with(r"\\")
        || (s.len() > 2 && s.as_bytes()[1] == b':' && matches!(s.as_bytes()[2], b'\\' | b'/'));
    if absolute {
        return "<path>".into();
    }
    // Labels and phases are literal, but never let a volatile id leak through.
    let mut out = s.to_owned();
    for (value, name) in known {
        if !value.is_empty() && out.contains(value.as_str()) {
            out = out.replace(value.as_str(), name);
        }
    }
    out
}

/// The first SCREAMING_CASE token of an error string (§8 `code_of`), or `TEXT`.
pub fn error_code(s: &str) -> String {
    s.split(|c: char| !(c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit()))
        .find(|t| t.len() >= 4 && t.contains('_') && t.chars().next().unwrap().is_ascii_uppercase())
        .unwrap_or("TEXT")
        .to_owned()
}

pub fn is_uuid(s: &str) -> bool {
    s.len() == 36 && uuid::Uuid::parse_str(s).is_ok()
}

/// Restrict `actual` to the key paths present in `golden`, so members added by
/// later waves (for example the §7.3 `status` member) do not break re-capture.
pub fn project_onto(golden: &Value, actual: &Value) -> Value {
    match (golden, actual) {
        (Value::Object(g), Value::Object(a)) => Value::Object(
            g.iter()
                .filter_map(|(k, gv)| a.get(k).map(|av| (k.clone(), project_onto(gv, av))))
                .collect(),
        ),
        (Value::Array(g), Value::Array(a)) if g.len() == a.len() => Value::Array(
            g.iter()
                .zip(a)
                .map(|(gv, av)| project_onto(gv, av))
                .collect(),
        ),
        _ => actual.clone(),
    }
}

/// Assert that `actual` (a raw daemon response) has the shape of `golden`
/// (a normalized baseline response from `tests/fixtures/compat`).
///
/// Rules:
/// - every literal golden key must exist in `actual`; extra keys are allowed
///   (additive members such as §7.3 `status`);
/// - a marker key (`<...>`) is a template for dynamic keys: every actual key that is
///   not a literal golden key must match the shape of one marker-keyed value;
/// - arrays: same length → element-wise; otherwise every actual element must match
///   some golden element (an empty golden array accepts any array);
/// - golden `null` accepts anything (nullable, type not pinned);
/// - `<ms?>`, `<u64?>` accept a number or null; `<error:CODE>` a string or null;
///   `<u64>`, `<f64>`, `<ms>` need a number;
///   other markers and literal strings need a string; booleans need a boolean;
///   numbers need a number. Literal values are not compared (shape only).
#[track_caller]
pub fn assert_same_shape(golden: &Value, actual: &Value) {
    let mut problems = Vec::new();
    shape_diff("$", golden, actual, &mut problems);
    assert!(
        problems.is_empty(),
        "shape mismatch:\n  {}\nactual: {}",
        problems.join("\n  "),
        serde_json::to_string_pretty(actual).unwrap()
    );
}

/// The shape differences between `golden` and `actual` (empty when they match).
pub fn shape_problems(golden: &Value, actual: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    shape_diff("$", golden, actual, &mut problems);
    problems
}

fn is_marker(s: &str) -> bool {
    s.starts_with('<') && s.ends_with('>')
}

fn shape_diff(path: &str, golden: &Value, actual: &Value, out: &mut Vec<String>) {
    match golden {
        Value::Null => {}
        Value::Bool(_) => {
            if !actual.is_boolean() {
                out.push(format!("{path}: expected bool, got {}", kind(actual)));
            }
        }
        Value::Number(_) => {
            if !actual.is_number() {
                out.push(format!("{path}: expected number, got {}", kind(actual)));
            }
        }
        Value::String(s) if is_marker(s) => {
            // Error members come and go; their golden code is informative only.
            let nullable = s.ends_with("?>") || s.starts_with("<error:");
            let numeric = matches!(
                s.trim_end_matches("?>").trim_end_matches('>'),
                "<ms" | "<u64" | "<f64"
            );
            let ok = (nullable && actual.is_null())
                || if numeric {
                    actual.is_number()
                } else {
                    actual.is_string()
                };
            if !ok {
                out.push(format!("{path}: expected {s}, got {}", kind(actual)));
            }
        }
        Value::String(_) => {
            if !actual.is_string() {
                out.push(format!("{path}: expected string, got {}", kind(actual)));
            }
        }
        Value::Array(g) => {
            let Some(a) = actual.as_array() else {
                out.push(format!("{path}: expected array, got {}", kind(actual)));
                return;
            };
            if g.is_empty() {
                return;
            }
            if g.len() == a.len() {
                for (i, (gv, av)) in g.iter().zip(a).enumerate() {
                    shape_diff(&format!("{path}[{i}]"), gv, av, out);
                }
            } else {
                for (i, av) in a.iter().enumerate() {
                    let matched = g.iter().any(|gv| shape_problems(gv, av).is_empty());
                    if !matched {
                        out.push(format!(
                            "{path}[{i}]: matches no golden element: {:?}",
                            shape_problems(&g[0], av)
                        ));
                    }
                }
            }
        }
        Value::Object(g) => {
            let Some(a) = actual.as_object() else {
                out.push(format!("{path}: expected object, got {}", kind(actual)));
                return;
            };
            let templates: Vec<&Value> = g
                .iter()
                .filter(|(k, _)| is_marker(k))
                .map(|(_, v)| v)
                .collect();
            for (k, gv) in g.iter().filter(|(k, _)| !is_marker(k)) {
                match a.get(k) {
                    Some(av) => shape_diff(&format!("{path}.{k}"), gv, av, out),
                    None => out.push(format!("{path}.{k}: missing")),
                }
            }
            if !templates.is_empty() {
                for (k, av) in a.iter().filter(|(k, _)| !g.contains_key(*k)) {
                    if !templates.iter().any(|t| shape_problems(t, av).is_empty()) {
                        out.push(format!(
                            "{path}.{k}: matches no template: {:?}",
                            shape_problems(templates[0], av)
                        ));
                    }
                }
            }
        }
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
