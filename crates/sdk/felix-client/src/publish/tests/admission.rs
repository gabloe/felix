use tokio::time::Duration;

use crate::publish::PublishAdmission;

#[tokio::test]
async fn publish_admission_bounds_shared_inflight_bytes() {
    let admission = PublishAdmission::new(4);
    let permit = admission.acquire(4).await.expect("initial permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
            .await
            .is_err()
    );
    drop(permit);
    let _permit = admission.acquire(1).await.expect("released permit");
}

#[tokio::test]
async fn publish_admission_rejects_oversized_frame() {
    let err = PublishAdmission::new(4)
        .acquire(5)
        .await
        .expect_err("oversized publish");
    assert!(err.to_string().contains("exceeds in-flight byte limit"));
}
