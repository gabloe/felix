use super::*;

#[test]
fn child_ready_line_round_trips() {
    let addr: SocketAddr = "127.0.0.1:4433".parse().expect("addr");
    let cert = CertificateDer::from(vec![1u8, 2, 3, 4]);
    let line = format!(
        "SOAK_CHILD_READY addr={} cert={}",
        addr,
        URL_SAFE_NO_PAD.encode(&cert)
    );
    let (parsed_addr, parsed_cert) = parse_child_ready(&line).expect("parse");
    assert_eq!(parsed_addr, addr);
    assert_eq!(parsed_cert, cert);
}

#[test]
fn child_ready_rejects_lines_that_are_not_the_ready_marker() {
    assert!(parse_child_ready("some other output").is_none());
    assert!(parse_child_ready("SOAK_CHILD_READY addr=nonsense cert=zz").is_none());
    assert!(parse_child_ready("SOAK_CHILD_READY addr=127.0.0.1:1").is_none());
}
