use kafka_protocol::messages::{
    FindCoordinatorRequest, HeartbeatRequest, JoinGroupRequest, OffsetFetchRequest,
};
use kafka_protocol::protocol::StrBytes;

use super::Fixture;
use crate::api::groups::MESSAGE;

#[tokio::test]
async fn find_coordinator_is_refused_with_a_reason_at_every_version() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    for version in 0..=3 {
        let response = client
            .call(
                &FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("g")),
                version,
            )
            .await;
        assert_eq!(
            response.error_code, 30,
            "GROUP_AUTHORIZATION_FAILED at v{version}"
        );
        if version >= 1 {
            assert_eq!(
                response.error_message.as_ref().map(|m| m.as_str()),
                Some(MESSAGE)
            );
        }
    }
    let response = client
        .call(
            &FindCoordinatorRequest::default()
                .with_coordinator_keys(vec![StrBytes::from_static_str("g")]),
            4,
        )
        .await;
    assert_eq!(response.coordinators.len(), 1);
    assert_eq!(response.coordinators[0].key.as_str(), "g");
    assert_eq!(response.coordinators[0].error_code, 30);
}

#[tokio::test]
async fn group_apis_reached_anyway_get_the_same_refusal() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    assert_eq!(
        client
            .call(&JoinGroupRequest::default(), 5)
            .await
            .error_code,
        30
    );
    assert_eq!(
        client
            .call(&HeartbeatRequest::default(), 3)
            .await
            .error_code,
        30
    );
    assert_eq!(
        client
            .call(&OffsetFetchRequest::default(), 5)
            .await
            .error_code,
        30
    );
}
