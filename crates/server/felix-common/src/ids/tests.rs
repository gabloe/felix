use std::str::FromStr;

use uuid::Uuid;

use super::*;

#[test]
fn region_id_round_trip() {
    // IDs should serialize and parse without loss.
    let region = RegionId::new();
    let parsed = RegionId::from_str(&region.to_string()).expect("parse");
    assert_eq!(region, parsed);
}

#[test]
fn region_id_rejects_invalid_input() {
    let err = RegionId::from_str("not-a-uuid").expect_err("invalid");
    assert!(matches!(err, Error::InvalidId(s) if s == "not-a-uuid"));
}

#[test]
fn all_id_types_work() {
    let region = RegionId::new();
    let tenant = TenantId::new();
    let _namespace = NamespaceId::new();
    let _stream = StreamId::new();
    let _topic = TopicId::new();
    let _shard = ShardId::new();

    // Test display
    assert!(!region.to_string().is_empty());
    assert!(!tenant.to_string().is_empty());

    // Test from_uuid
    let uuid = Uuid::new_v4();
    let region2 = RegionId::from_uuid(uuid);
    assert_eq!(region2.as_uuid(), uuid);

    // Test parse from string
    let region_str = region.to_string();
    let region3 = RegionId::from_str(&region_str).expect("parse");
    assert_eq!(region, region3);

    // Test default
    let _ = TenantId::default();
    let _ = NamespaceId::default();
}

#[test]
fn id_types_parse_valid_uuids() {
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    let tenant = TenantId::from_str(uuid_str).expect("parse");
    assert_eq!(tenant.to_string(), uuid_str);
}

#[test]
fn id_types_reject_invalid_uuids() {
    let invalid = "not-valid";
    assert!(TenantId::from_str(invalid).is_err());
    assert!(NamespaceId::from_str(invalid).is_err());
    assert!(StreamId::from_str(invalid).is_err());
}
