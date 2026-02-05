use felix_transport::TransportConfig;

use crate::config::BrokerConfig;

pub mod quic;

pub fn cache_transport_config(config: &BrokerConfig, mut base: TransportConfig) -> TransportConfig {
    base.receive_window = config.cache_conn_recv_window;
    base.stream_receive_window = config.cache_stream_recv_window;
    base.send_window = config.cache_send_window;
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn cache_transport_config_overrides_windows() {
        let broker_config = BrokerConfig {
            quic_bind: "0.0.0.0:5000".parse::<SocketAddr>().unwrap(),
            metrics_bind: "0.0.0.0:8080".parse::<SocketAddr>().unwrap(),
            controlplane_url: None,
            controlplane_sync_interval_ms: 2000,
            ack_on_commit: false,
            max_frame_bytes: 16 * 1024 * 1024,
            publish_queue_wait_timeout_ms: 2000,
            ack_wait_timeout_ms: 2000,
            disable_timings: false,
            control_stream_drain_timeout_ms: 50,
            cache_conn_recv_window: 999,
            cache_stream_recv_window: 888,
            cache_send_window: 777,
            event_batch_max_events: 64,
            event_batch_max_bytes: 256 * 1024,
            event_batch_max_delay_us: 250,
            fanout_batch_size: 64,
            pub_workers_per_conn: 4,
            pub_queue_depth: 1024,
            subscriber_queue_capacity: 128,
            subscriber_queue_policy: felix_broker::SubQueuePolicy::DropNew,
            subscriber_writer_lanes: 4,
            subscriber_lane_queue_depth: 8192,
            subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
            max_subscriber_writer_lanes: 8,
            subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
            subscriber_single_writer_per_conn: true,
            subscriber_flush_max_items: 64,
            subscriber_flush_max_delay_us: 200,
            subscriber_max_bytes_per_write: 256 * 1024,
            sub_streams_per_conn: 4,
            sub_stream_mode: crate::config::SubStreamMode::PerSubscriber,
        };

        let base = TransportConfig::default();
        let result = cache_transport_config(&broker_config, base);

        assert_eq!(result.receive_window, 999);
        assert_eq!(result.stream_receive_window, 888);
        assert_eq!(result.send_window, 777);
    }
}
