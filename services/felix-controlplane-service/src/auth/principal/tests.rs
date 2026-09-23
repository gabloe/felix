use super::*;

#[test]
fn principal_id_is_stable() {
    let a = principal_id("https://issuer", "sub");
    let b = principal_id("https://issuer", "sub");
    assert_eq!(a, b);
}

#[test]
fn principal_id_changes_with_inputs() {
    let a = principal_id("https://issuer", "sub");
    let b = principal_id("https://issuer", "sub2");
    assert_ne!(a, b);
}
