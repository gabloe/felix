use felix_wire::StartPosition;

use super::resume_position;

#[test]
fn resumes_past_whichever_is_further() {
    assert_eq!(resume_position(Some(9), Some(8)), StartPosition::Offset(10));
    assert_eq!(resume_position(Some(4), Some(8)), StartPosition::Offset(8));
}

#[test]
fn resumes_at_the_broker_position_when_nothing_was_delivered() {
    assert_eq!(resume_position(None, Some(8)), StartPosition::Offset(8));
}

#[test]
fn resumes_after_the_last_delivered_without_a_broker_position() {
    assert_eq!(resume_position(Some(4), None), StartPosition::Offset(5));
}

#[test]
fn resumes_at_the_tail_with_nothing_to_go_on() {
    assert_eq!(resume_position(None, None), StartPosition::Latest);
}
