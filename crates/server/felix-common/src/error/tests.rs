use super::Error;

#[test]
fn error_invalid_id_display() {
    let err = Error::InvalidId("bad-id".to_string());
    assert!(err.to_string().contains("bad-id"));
}
