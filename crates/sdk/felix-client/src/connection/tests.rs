use super::listener_targets;

/// The dialled address is always first and always present. It is the one
/// address known to work; an advertisement is a claim that has not been tested.
#[test]
fn listener_targets_keep_the_dialled_address_first() {
    let dialled: std::net::SocketAddr = "10.0.0.4:5000".parse().expect("addr");
    let targets = listener_targets(dialled, &[5002, 5001]);
    assert_eq!(targets[0], dialled);
    assert_eq!(
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["10.0.0.4:5000", "10.0.0.4:5002", "10.0.0.4:5001"],
    );
}

/// A broker that named no ports leaves the client exactly where it was.
#[test]
fn no_advertised_ports_means_the_dialled_address_alone() {
    let dialled: std::net::SocketAddr = "10.0.0.4:5000".parse().expect("addr");
    assert_eq!(listener_targets(dialled, &[]), vec![dialled],);
}

/// The dialled port appearing in the advertisement must not produce a
/// duplicate, or that listener would take a double share of the pool.
#[test]
fn the_dialled_port_is_not_repeated_when_it_is_also_advertised() {
    let dialled: std::net::SocketAddr = "10.0.0.4:5000".parse().expect("addr");
    let targets = listener_targets(dialled, &[5000, 5001]);
    assert_eq!(
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["10.0.0.4:5000", "10.0.0.4:5001"],
    );
}

/// Only the port is taken. An `AuthOk` must not be able to move a client to
/// another machine -- that is a redirect, and it has its own message.
#[test]
fn only_the_port_is_taken_from_the_advertisement() {
    let dialled: std::net::SocketAddr = "10.0.0.4:5000".parse().expect("addr");
    let targets = listener_targets(dialled, &[5001]);
    assert!(
        targets.iter().all(|t| t.ip() == dialled.ip()),
        "{targets:?}"
    );
}
