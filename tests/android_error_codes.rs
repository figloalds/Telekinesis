//! Every error code the Android app raises is registered in `model::errors`, and
//! the ones only it raises are explained in the README's table.
use std::{
    fs,
    path::{Path, PathBuf},
};
use tkfs::model::errors::is_known;

const ROOT: &str = env!("CARGO_MANIFEST_DIR");
const SOURCES: &[&str] = &["android/app/src/main/java", "android/native/src"];
/// Raised by the app and nothing else, so its user meets them nowhere else.
const ONLY_THE_APP_RAISES: &[&str] = &[
    "INVALID_DOCUMENT_ID",
    "NATIVE_PANIC",
    "PC_UNREACHABLE",
    "PHONE_DB_UNAVAILABLE",
];

fn source_files(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            source_files(&path, found);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "kt" || ext == "rs")
        {
            found.push(path);
        }
    }
}

/// The code at the start of each string literal: `"CODE"` or `"CODE: detail"`.
fn codes_in_literals(text: &str) -> Vec<&str> {
    text.split('"')
        .skip(1)
        .filter_map(|literal| {
            let end = literal
                .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
                .unwrap_or(literal.len());
            let (code, rest) = literal.split_at(end);
            let is_code = code.len() >= 4 && code.as_bytes()[0].is_ascii_uppercase();
            (is_code && (rest.is_empty() || rest.starts_with(':'))).then_some(code)
        })
        .collect()
}

#[test]
fn every_code_the_app_raises_is_registered() {
    let mut files = Vec::new();
    for dir in SOURCES {
        source_files(&Path::new(ROOT).join(dir), &mut files);
    }
    assert!(files.len() >= 5, "found only {files:?}");
    let mut missing = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).unwrap();
        for code in codes_in_literals(&text) {
            if !is_known(code) {
                missing.push(format!("{code} in {}", file.display()));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "add to src/model/errors.rs: {missing:#?}"
    );
}

#[test]
fn the_codes_only_the_app_raises_are_in_the_readme() {
    let readme = fs::read_to_string(Path::new(ROOT).join("README.md")).unwrap();
    for code in ONLY_THE_APP_RAISES {
        assert!(is_known(code), "{code} is not in src/model/errors.rs");
        assert!(
            readme.contains(&format!("`{code}`")),
            "{code} is not in the README's table"
        );
    }
}

#[test]
fn the_scan_finds_both_shapes_and_ignores_the_rest() {
    let text = r#"throw X("ONE_CODE: detail $id"); bail!("TWO_CODE"); log("TAG"); use("java/io/IOException"); q("Plain: words")"#;
    assert_eq!(codes_in_literals(text), ["ONE_CODE", "TWO_CODE"]);
}
