//! Request ids: a retried proposal is answered from the first apply.
use super::*;

/// **A retried proposal is answered with what the first one returned.**
///
/// `RaftStore::write` caps each attempt and retries within a larger budget, so
/// a timed-out attempt may be re-proposing a command that committed. Without a
/// request id the state machine answers the retry from its post-commit state:
/// `409 tenant already exists`, for a tenant the caller successfully created
/// (#529). With one, the retry gets the original success.
#[tokio::test]
async fn a_retried_proposal_is_answered_from_the_first_apply() {
    let machine = machine();
    let create = encode_command(&MetaCommand::CreateTenant {
        tenant: tenant("acme"),
    });
    let proposal = crate::store::raft::command::stamp_request_id(&create, "rid-1").expect("stamps");

    let first = machine.apply(&proposal).await;
    assert!(
        decode_result(&first).expect("decodes").is_ok(),
        "the first apply should create the tenant",
    );

    // The same bytes again, exactly as a retry re-proposes them.
    let second = machine.apply(&proposal).await;
    assert_eq!(
        second, first,
        "a retry must be answered with the original response, not the conflict \
         its own effect now produces",
    );
}

/// Without an id there is nothing to deduplicate on, which is what a proposal
/// from a peer that predates this looks like. It conflicts, as it always did.
#[tokio::test]
async fn an_unstamped_retry_still_conflicts() {
    let machine = machine();
    let create = encode_command(&MetaCommand::CreateTenant {
        tenant: tenant("acme"),
    });

    assert!(
        decode_result(&machine.apply(&create).await)
            .expect("decodes")
            .is_ok()
    );
    let second = decode_result(&machine.apply(&create).await).expect("decodes");
    assert!(
        matches!(second, Err(MetaError::Conflict(_))),
        "an unstamped re-proposal has no identity to be recognised by: {second:?}",
    );
}

/// Two *different* writes must not be confused for each other, however close
/// together they arrive.
#[tokio::test]
async fn different_request_ids_are_applied_separately() {
    let machine = machine();
    let first = crate::store::raft::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-1",
    )
    .expect("stamps");
    let second = crate::store::raft::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-2",
    )
    .expect("stamps");

    assert!(
        decode_result(&machine.apply(&first).await)
            .expect("decodes")
            .is_ok()
    );
    // A genuine second attempt to create the same tenant, by someone else.
    // That is a real conflict and must still be reported as one.
    let answer = decode_result(&machine.apply(&second).await).expect("decodes");
    assert!(
        matches!(answer, Err(MetaError::Conflict(_))),
        "a different write conflicting is not a retry: {answer:?}",
    );
}

/// The applied ids ride the snapshot, so a replica that restored from one
/// still recognises a retry -- the case a leader change would otherwise reopen.
#[tokio::test]
async fn applied_ids_survive_a_snapshot_restore() {
    let original = machine();
    let proposal = crate::store::raft::command::stamp_request_id(
        &encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }),
        "rid-1",
    )
    .expect("stamps");
    let first = original.apply(&proposal).await;

    let restored = machine();
    restored.restore(&original.snapshot().await).await;

    assert_eq!(
        restored.apply(&proposal).await,
        first,
        "a restored replica must answer the retry the way the original did",
    );
}
