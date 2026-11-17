use serde_json::json;
use sha2::{Digest, Sha256};

use super::{bearer_value, BearerKeySetError, BearerTokenVerifier};

pub(super) fn document(entries: &[(&str, &str)]) -> String {
    json!({
        "schemaVersion": 1,
        "keys": entries.iter().map(|(id, token)| json!({
            "id": id,
            "sha256": hex::encode(Sha256::digest(token.as_bytes())),
        })).collect::<Vec<_>>(),
    })
    .to_string()
}

#[test]
fn identifies_each_key_and_sorts_audit_ids() -> Result<(), BearerKeySetError> {
    let verifier = BearerTokenVerifier::from_json(&document(&[
        ("pedro", "pedro-test-token"),
        ("edson", "edson-test-token"),
    ]))?;

    assert_eq!(verifier.keys.len(), 2);
    assert_eq!(verifier.keys[0].id, "edson");
    assert_eq!(verifier.keys[1].id, "pedro");
    assert_eq!(verifier.verify("edson-test-token"), Some("edson"));
    assert_eq!(verifier.verify("pedro-test-token"), Some("pedro"));
    assert_eq!(verifier.verify("unknown-token"), None);
    assert_eq!(verifier.verify(""), None);
    Ok(())
}

#[test]
fn accepts_arbitrary_valid_ids() -> Result<(), BearerKeySetError> {
    let longest_id = "a".repeat(63);
    for id in ["a", "0", "analytics-job-7", longest_id.as_str()] {
        let verifier = BearerTokenVerifier::from_json(&document(&[(id, "test-token")]))?;
        assert_eq!(verifier.verify("test-token"), Some(id));
    }
    Ok(())
}

#[test]
fn accepts_at_most_thirty_two_keys() {
    assert_invalid(r#"{"schemaVersion":1,"keys":[]}"#);
    for count in [32, 33] {
        let keys = (0..count)
            .map(|index| json!({"id": format!("key-{index}"), "sha256": format!("{index:064x}")}))
            .collect::<Vec<_>>();
        let value = json!({"schemaVersion": 1, "keys": keys}).to_string();
        assert_eq!(BearerTokenVerifier::from_json(&value).is_ok(), count == 32);
    }
}

#[test]
fn rejects_invalid_ids() {
    let too_long = "a".repeat(64);
    for id in [
        "",
        "-leading",
        "trailing-",
        "Uppercase",
        "has_underscore",
        "has space",
        "a\nb",
        "café",
        too_long.as_str(),
    ] {
        assert_invalid(&document(&[(id, "test-token")]));
    }
}

#[test]
fn rejects_duplicate_ids() {
    assert_invalid(&document(&[
        ("duplicate", "first"),
        ("duplicate", "second"),
    ]));
}

#[test]
fn rejects_duplicate_decoded_digests() {
    let digest = hex::encode(Sha256::digest(b"same-token"));
    assert_invalid(
        &json!({
            "schemaVersion": 1,
            "keys": [
                {"id": "first", "sha256": digest},
                {"id": "second", "sha256": digest.to_uppercase()},
            ],
        })
        .to_string(),
    );
}

#[test]
fn rejects_unknown_versions_malformed_json_and_unknown_fields() {
    assert_invalid("{not-json");
    let key = json!({"id": "key", "sha256": "00".repeat(32)});
    for value in [
        json!({"schemaVersion": 2, "keys": [key]}),
        json!({"schemaVersion": 1, "keys": [key], "extra": true}),
        json!({
            "schemaVersion": 1,
            "keys": [{"id": "key", "sha256": "00".repeat(32), "extra": true}],
        }),
        json!({"schemaVersion": 1}),
        json!({"keys": [key]}),
        json!({"schemaVersion": 1, "keys": [{"id": "key"}]}),
    ] {
        assert_invalid(&value.to_string());
    }
}

#[test]
fn rejects_non_hex_and_wrong_length_digests() {
    for digest in [
        String::new(),
        "not-hex".to_owned(),
        "00".repeat(31),
        "00".repeat(33),
    ] {
        assert_invalid(
            &json!({"schemaVersion": 1, "keys": [{"id": "key", "sha256": digest}]}).to_string(),
        );
    }
}

#[test]
fn malformed_values_never_reach_error_messages() {
    let error =
        BearerTokenVerifier::from_json(&document(&[("private-value!", "test-token")])).err();
    assert_eq!(error, Some(BearerKeySetError::InvalidDocument));
    assert_eq!(
        BearerKeySetError::InvalidDocument.to_string(),
        "bearer key document is invalid"
    );
    assert_eq!(
        format!("{:?}", BearerKeySetError::InvalidDocument),
        "InvalidDocument"
    );
}

#[test]
fn accepts_one_bearer_value_without_normalizing_the_secret() {
    assert_eq!(bearer_value("Bearer exact-token"), Some("exact-token"));
    assert_eq!(bearer_value("bearer exact-token"), Some("exact-token"));
    assert_eq!(bearer_value("Bearer exact-token "), None);
    assert_eq!(bearer_value("Bearer  exact-token"), None);
    assert_eq!(bearer_value("Bearer "), None);
    assert_eq!(bearer_value("Basic exact-token"), None);
}

fn assert_invalid(value: &str) {
    assert_eq!(
        BearerTokenVerifier::from_json(value).err(),
        Some(BearerKeySetError::InvalidDocument)
    );
}
