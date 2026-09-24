use felix_storage::EphemeralCache;

use crate::Broker;
use crate::error::BrokerError;

#[test]
fn zero_capacity_is_rejected() {
    let broker = Broker::new(EphemeralCache::new().into());
    let err = broker.with_topic_capacity(0).expect_err("capacity");
    assert!(matches!(err, BrokerError::CapacityTooLarge));
}
