// Broker capacity defaults and subscriber-queue backpressure policy.

pub(crate) const DEFAULT_TOPIC_CAPACITY: usize = 1024;
pub(crate) const DEFAULT_LOG_CAPACITY: usize = 1024;
pub(crate) const DEFAULT_SUB_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubQueuePolicy {
    Block,
    DropNew,
    DropOld,
}
