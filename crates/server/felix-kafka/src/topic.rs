//! Kafka topic names for Felix streams.
//!
//! A topic is `<namespace>.<stream>`, split on the first dot. The tenant never
//! appears in it: it comes from the credential, so one tenant cannot name
//! another's streams by spelling them out.
//!
//! Felix allows names Kafka does not. A stream whose names cannot be written
//! as a legal topic, or whose namespace contains a dot (the split would read
//! it back as a different stream), is not offered to Kafka clients at all.

/// Kafka's own limit on a topic name.
const MAX_TOPIC_LEN: usize = 249;

/// A stream a topic names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicStream {
    pub(crate) namespace: String,
    pub(crate) stream: String,
}

/// The topic a stream is offered as, or `None` when it cannot be offered.
pub(crate) fn topic_name(namespace: &str, stream: &str) -> Option<String> {
    if namespace.is_empty() || stream.is_empty() || namespace.contains('.') {
        return None;
    }
    let topic = format!("{namespace}.{stream}");
    legal(&topic).then_some(topic)
}

/// The stream `topic` names.
///
/// A topic without a dot is a stream in `default_namespace`, when one is
/// configured, and names nothing otherwise.
pub(crate) fn parse_topic(topic: &str, default_namespace: Option<&str>) -> Option<TopicStream> {
    if !legal(topic) {
        return None;
    }
    match topic.split_once('.') {
        Some((namespace, stream)) if !namespace.is_empty() && !stream.is_empty() => {
            Some(TopicStream {
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            })
        }
        Some(_) => None,
        None => default_namespace.map(|namespace| TopicStream {
            namespace: namespace.to_string(),
            stream: topic.to_string(),
        }),
    }
}

fn legal(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= MAX_TOPIC_LEN
        && topic != "."
        && topic != ".."
        && topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests;
