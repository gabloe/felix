use felix_transport::TransportConfig;

use crate::config::BrokerConfig;

pub fn cache_transport_config(config: &BrokerConfig, mut base: TransportConfig) -> TransportConfig {
    base.receive_window = config.cache_conn_recv_window;
    base.stream_receive_window = config.cache_stream_recv_window;
    base.send_window = config.cache_send_window;
    base
}
