use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, MetadataRequest};

use super::Fixture;

#[tokio::test]
async fn api_versions_advertises_reads_writes_and_find_coordinator() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    for version in 0..=3 {
        let response = client.call(&ApiVersionsRequest::default(), version).await;
        assert_eq!(response.error_code, 0);
        let range = |key: ApiKey| {
            response
                .api_keys
                .iter()
                .find(|api| api.api_key == key as i16)
                .map(|api| (api.min_version, api.max_version))
        };
        assert_eq!(range(ApiKey::Fetch), Some((4, 12)));
        assert_eq!(range(ApiKey::Metadata), Some((0, 12)));
        assert_eq!(range(ApiKey::ListOffsets), Some((1, 7)));
        assert_eq!(range(ApiKey::FindCoordinator), Some((0, 4)));
        assert_eq!(range(ApiKey::Produce), Some((3, 9)));
        assert_eq!(range(ApiKey::InitProducerId), Some((0, 4)));
        assert_eq!(range(ApiKey::JoinGroup), None);
        assert_eq!(range(ApiKey::EndTxn), None);
    }
}

#[tokio::test]
async fn a_too_new_api_versions_is_refused_in_the_v0_shape() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    // ApiVersions v9: header v2, so client id then an empty tag buffer.
    let mut body = BytesMut::new();
    body.put_i16(ApiKey::ApiVersions as i16);
    body.put_i16(9);
    body.put_i32(77);
    body.put_i16(-1);
    body.put_u8(0);
    client.send_raw(&body).await;

    let mut frame = client.read_frame().await.expect("answered, not closed");
    assert_eq!(frame.get_i32(), 77);
    assert_eq!(frame.get_i16(), 35, "UNSUPPORTED_VERSION");
    assert_eq!(frame.get_i32(), 1, "one range: ApiVersions itself");
    assert_eq!(
        (frame.get_i16(), frame.get_i16(), frame.get_i16()),
        (ApiKey::ApiVersions as i16, 0, 3)
    );
    assert!(!frame.has_remaining());

    // And the connection is still usable at a version it offered.
    let response = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(response.error_code, 0);
}

#[tokio::test]
async fn an_api_that_was_not_offered_closes_the_connection() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    // Metadata v13 is past the advertised range.
    client.send(&MetadataRequest::default(), 13).await;
    assert!(
        client.read_frame().await.is_none(),
        "closed without an answer"
    );

    let mut client = fixture.connect();
    let mut body = BytesMut::new();
    body.put_i16(999);
    body.put_i16(0);
    body.put_i32(1);
    body.put_i16(-1);
    client.send_raw(&body).await;
    assert!(client.read_frame().await.is_none(), "unknown api key");
}

#[tokio::test]
async fn an_oversized_frame_closes_the_connection_without_allocating_it() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    // Only the length prefix: a server that trusted it would wait for 1 GiB.
    let mut body = BytesMut::new();
    body.put_i32(1 << 30);
    use tokio::io::AsyncWriteExt;
    client.stream.write_all(&body).await.expect("write");
    assert!(client.read_frame().await.is_none());
}
