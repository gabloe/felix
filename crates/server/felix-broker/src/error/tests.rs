use super::BrokerError;

#[tokio::test]
async fn broker_error_display() {
    let err = BrokerError::CapacityTooLarge;
    assert!(err.to_string().contains("capacity"));

    let err = BrokerError::CursorTooOld {
        oldest: 10,
        requested: 5,
    };
    assert!(err.to_string().contains("10"));
    assert!(err.to_string().contains("5"));

    let err = BrokerError::TenantNotFound("t1".to_string());
    assert!(err.to_string().contains("t1"));

    let err = BrokerError::NamespaceNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));

    let err = BrokerError::StreamNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
        stream: "s1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));
    assert!(err.to_string().contains("s1"));
}
