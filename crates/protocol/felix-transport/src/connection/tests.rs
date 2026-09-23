use super::*;

#[test]
fn connection_info_holds_fields() {
    let info = ConnectionInfo {
        id: ConnectionId(42),
        peer_addr: "127.0.0.1:1234".parse().expect("addr"),
    };
    assert_eq!(info.id, ConnectionId(42));
    assert_eq!(info.peer_addr, "127.0.0.1:1234".parse().unwrap());
}

#[test]
fn connection_id_equality() {
    let id1 = ConnectionId(42);
    let id2 = ConnectionId(42);
    let id3 = ConnectionId(43);
    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}
