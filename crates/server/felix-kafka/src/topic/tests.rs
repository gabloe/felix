use super::{TopicStream, parse_topic, topic_name};

fn stream(namespace: &str, stream: &str) -> TopicStream {
    TopicStream {
        namespace: namespace.to_string(),
        stream: stream.to_string(),
    }
}

#[test]
fn a_topic_splits_on_the_first_dot() {
    assert_eq!(
        parse_topic("orders.created", None),
        Some(stream("orders", "created"))
    );
    assert_eq!(
        parse_topic("orders.eu.created", None),
        Some(stream("orders", "eu.created"))
    );
}

#[test]
fn a_bare_topic_needs_a_default_namespace() {
    assert_eq!(parse_topic("created", None), None);
    assert_eq!(
        parse_topic("created", Some("orders")),
        Some(stream("orders", "created"))
    );
}

#[test]
fn illegal_topics_name_nothing() {
    for topic in [
        "",
        ".",
        "..",
        ".created",
        "orders.",
        "orders/created",
        "orders created",
    ] {
        assert_eq!(parse_topic(topic, Some("ns")), None, "{topic:?}");
    }
    assert_eq!(parse_topic(&"a".repeat(250), Some("ns")), None);
}

#[test]
fn streams_kafka_cannot_name_are_not_offered() {
    assert_eq!(
        topic_name("orders", "created").as_deref(),
        Some("orders.created")
    );
    assert_eq!(
        topic_name("orders", "eu.created").as_deref(),
        Some("orders.eu.created")
    );
    // Read back, this would be namespace "orders", stream "eu.created".
    assert_eq!(topic_name("orders.eu", "created"), None);
    assert_eq!(topic_name("orders", "créé"), None);
    assert_eq!(topic_name("orders", "a b"), None);
    assert_eq!(topic_name("", "created"), None);
}

#[test]
fn every_offered_topic_reads_back_as_its_stream() {
    for (namespace, name) in [("a", "b"), ("ns-1", "x.y.z"), ("N_s", "s-_.1")] {
        let topic = topic_name(namespace, name).expect("offered");
        assert_eq!(parse_topic(&topic, None), Some(stream(namespace, name)));
    }
}
