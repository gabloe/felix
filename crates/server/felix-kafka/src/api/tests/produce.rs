use bytes::Bytes;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiVersionsRequest, ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::Fixture;
use crate::api::produce::MESSAGE;

fn request(acks: i16) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(acks)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("orders.created")))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(Bytes::from_static(b"not decoded"))),
                ]),
        ])
}

#[tokio::test]
async fn a_produce_is_refused_with_a_reason() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let response = client.call(&request(-1), 7).await;
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, 44, "POLICY_VIOLATION");

    let response = client.call(&request(1), 8).await;
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(
        partition.error_message.as_ref().map(|m| m.as_str()),
        Some(MESSAGE)
    );
}

#[tokio::test]
async fn an_acks_zero_produce_gets_no_answer_and_the_connection_carries_on() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    client.send(&request(0), 7).await;
    // The next answer on the wire is for the next request.
    let response = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(response.error_code, 0);
}
