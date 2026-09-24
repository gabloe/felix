//! Serializers for `--print-config`, named by string from `BrokerConfig`'s
//! `serialize_with` attributes.

use felix_broker::SubQueuePolicy;

/// The config-file spelling of a queue policy, for `--print-config`.
pub(super) fn queue_policy<S: serde::Serializer>(
    value: &SubQueuePolicy,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    // Named here rather than by deriving on `SubQueuePolicy`, which lives in
    // `felix-broker` and has no serde dependency. A printing feature is not a
    // reason to give a core crate one.
    serializer.serialize_str(match value {
        SubQueuePolicy::DropNew => "drop_new",
        SubQueuePolicy::DropOld => "drop_old",
        SubQueuePolicy::Block => "block",
    })
}

/// Never write a secret into a dump meant to be shared.
///
/// `--print-config` is for pasting into an issue or a ticket, so a credential
/// that appears in it has been published. Shown as a fixed marker rather than
/// omitted: an operator has to be able to see that a token *is* set.
pub(super) fn redacted<S: serde::Serializer>(
    value: &str,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(if value.is_empty() {
        "<unset>"
    } else {
        "<redacted>"
    })
}
