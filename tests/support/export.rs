use serde_json::Value;
use std::time::{Duration, Instant};
use tkfs::{
    backend::ObjectBackend,
    content::{ContentProvider, LocalContent, ReadContext},
    model::content::{FileContentRef, ObjectKey},
    util::Cancel,
};

pub fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(60),
        cancel: Cancel::new(),
    }
}

pub fn restore(
    backend: &dyn ObjectBackend,
    report: &Value,
    root: &std::path::Path,
) -> (Value, LocalContent) {
    let index: Value = serde_json::from_slice(
        &backend
            .get_verified(report["manifest"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(index["format"], "tkfs-shared-export-2");
    let provider = LocalContent::open(root).unwrap();
    let mut keys = std::collections::BTreeSet::new();
    let objects = index["objects"].as_array().unwrap();
    assert_eq!(report["objects"].as_u64().unwrap() as usize, objects.len());
    for object in objects {
        let key: ObjectKey = serde_json::from_value(object["key"].clone()).unwrap();
        assert!(keys.insert(key), "duplicate exported identity");
        let bytes = backend
            .get_verified(object["object"].as_str().unwrap())
            .unwrap();
        assert_eq!(object["bytes"], bytes.len());
        assert!(bytes.len() <= 65_536);
        provider.install(key, &bytes, &context()).unwrap();
    }
    (index, provider)
}

pub fn read(
    provider: &LocalContent,
    reference: &FileContentRef,
    offset: u64,
    len: usize,
) -> Vec<u8> {
    let pin = provider.pin(reference).unwrap();
    let mut bytes = vec![0; len];
    for (i, chunk) in bytes.chunks_mut(1024 * 1024).enumerate() {
        provider
            .read_range(&pin, offset + i as u64 * 1024 * 1024, chunk, &context())
            .unwrap();
    }
    bytes
}
