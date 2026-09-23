use super::Error;

#[test]
fn error_invalid_id_display() {
    let err = Error::InvalidId("bad-id".to_string());
    assert!(err.to_string().contains("bad-id"));
}

#[test]
fn error_config_display() {
    let err = Error::Config("bad config".to_string());
    assert!(err.to_string().contains("bad config"));
}
