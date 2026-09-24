use felix_authz::Action;

use super::*;

#[test]
fn build_jwks_includes_kid_and_key() {
    let jwks = build_jwks("demo-k1").expect("jwks");
    assert_eq!(jwks.keys.len(), 1);
    assert_eq!(jwks.keys[0].kid, "demo-k1");
    assert!(jwks.keys[0].x.as_ref().expect("x").len() > 10);
}

#[tokio::test]
async fn demo_auth_for_tenant_mints_and_verifies() -> Result<()> {
    let demo = demo_auth_for_tenant("t1")?;
    let ctx = demo.auth.authenticate(&demo.tenant_id, &demo.token).await?;
    assert!(
        ctx.matcher
            .allows(Action::StreamPublish, "stream:t1/default/orders")
    );
    assert!(
        ctx.matcher
            .allows(Action::CacheRead, "cache:t1/default/primary")
    );
    Ok(())
}
