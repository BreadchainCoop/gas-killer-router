//! Send-site instrumentation for the task-directive p2p channel.
//!
//! The sequencer broadcasts each directive to the explicit operator set and gets back the list
//! of peers the p2p layer will attempt. Peers over their per-peer quota are filtered out of that
//! list silently, and a local submission refused under backpressure collapses the list to empty,
//! so at the call site a partial drop is indistinguishable from a full delivery — upstream can
//! only log when the list is empty, which is also what a total rate-limit looks like.
//!
//! This matters more than it sounds. The directive quota defaults to one message per second per
//! peer with a burst of one, so a router driving several heights sends faster than the quota
//! allows and an arbitrary subset of `Announce` directives is dropped. A node that misses an
//! `Announce` and later sees a directive for a higher height votes to skip the height it missed,
//! which is the split-digest stall that does not recover on its own.
//!
//! The p2p layer's own `messages_rate_limited_total` counts the *receive* side, where the
//! remedy is to sleep the connection rather than drop the message, so it cannot see any of
//! this. [`CountingSender`] wraps the sender the sequencer is given and separates the three
//! cases into [`DirectiveSendResult`].

use crate::metrics::{DirectiveSendResult, MetricsCollector, send_result_labels};
use commonware_actor::{Feedback, Unreliable};
use commonware_p2p::{CheckedSender, LimitedSender, Recipients};
use commonware_runtime::IoBufs;
use std::sync::Arc;
use std::time::SystemTime;

/// Records one recipient's directive-send result. A send that had nothing to attribute leaves no
/// series behind, so an empty broadcast does not read as a drop.
fn count_directive_send(metrics: &MetricsCollector, result: DirectiveSendResult, recipients: u64) {
    if recipients == 0 {
        return;
    }
    metrics
        .directive_sends
        .get_or_create(&send_result_labels(result))
        .inc_by(recipients);
}

/// A [`LimitedSender`] that counts what happened to every recipient's copy of a message.
///
/// Wraps the sender rather than the sequencer because the sequencer's broadcast loop is
/// upstream: the sender is the last thing this crate still owns on that path.
pub struct CountingSender<S> {
    inner: S,
    metrics: Arc<MetricsCollector>,
}

impl<S> CountingSender<S> {
    pub fn new(inner: S, metrics: Arc<MetricsCollector>) -> Self {
        Self { inner, metrics }
    }
}

// Derived `Clone` would demand `MetricsCollector: Clone`; only the `Arc` is cloned.
impl<S: Clone> Clone for CountingSender<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<S: LimitedSender> LimitedSender for CountingSender<S> {
    type PublicKey = S::PublicKey;
    type Checked<'a>
        = CountingChecked<S::Checked<'a>>
    where
        Self: 'a;

    fn check(
        &mut self,
        recipients: Recipients<Self::PublicKey>,
    ) -> Result<Self::Checked<'_>, SystemTime> {
        let requested = requested_count(&recipients);
        match self.inner.check(recipients) {
            Ok(checked) => {
                // Whatever the check dropped was over its quota. The survivors are only counted
                // once the send is actually attempted, in `CountingChecked::send`.
                if let Some(requested) = requested {
                    let retained = checked.recipients().len() as u64;
                    count_directive_send(
                        &self.metrics,
                        DirectiveSendResult::RateLimited,
                        requested.saturating_sub(retained),
                    );
                }
                Ok(CountingChecked {
                    inner: checked,
                    metrics: Arc::clone(&self.metrics),
                })
            }
            Err(available_at) => {
                // Every recipient is over quota, so the message never reaches a send attempt.
                count_directive_send(
                    &self.metrics,
                    DirectiveSendResult::RateLimited,
                    requested.unwrap_or(0),
                );
                Err(available_at)
            }
        }
    }
}

/// The rate-limit survivors, counted by delivery outcome once the send is attempted.
pub struct CountingChecked<C> {
    inner: C,
    metrics: Arc<MetricsCollector>,
}

impl<C: CheckedSender> CheckedSender for CountingChecked<C> {
    type PublicKey = C::PublicKey;

    fn recipients(&self) -> Vec<Self::PublicKey> {
        self.inner.recipients()
    }

    fn send(self, message: impl Into<IoBufs> + Send, priority: bool) -> Unreliable<Feedback> {
        let retained = self.inner.recipients().len() as u64;
        let feedback = self.inner.send(message, priority);
        // Acceptance is per submission, not per recipient: the local endpoint either took the
        // message for all survivors or for none.
        let result = if feedback.accepted() {
            DirectiveSendResult::Delivered
        } else {
            DirectiveSendResult::Rejected
        };
        count_directive_send(&self.metrics, result, retained);
        feedback
    }
}

/// How many recipients a send asked for, or `None` when that is unknowable.
///
/// `Recipients::All` resolves against the p2p layer's connected-peer snapshot, which the caller
/// cannot see, so there is no denominator to subtract the survivors from. The sequencer always
/// addresses the operator keys explicitly (`Recipients::Some`) precisely because that snapshot
/// stays empty on the router's send side, so the unknowable case does not arise in practice.
fn requested_count<P>(recipients: &Recipients<P>) -> Option<u64>
where
    P: commonware_cryptography::PublicKey,
{
    match recipients {
        Recipients::All => None,
        Recipients::Some(peers) => Some(peers.len() as u64),
        Recipients::One(_) => Some(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::Signer as _;
    use commonware_cryptography::ed25519::{PrivateKey, PublicKey};
    use commonware_p2p::Sender as _;

    fn key(seed: u64) -> PublicKey {
        PrivateKey::from_seed(seed).public_key()
    }

    /// A sender whose quota retains the first `retain` recipients and whose endpoint accepts or
    /// refuses the submission, so all three outcomes can be driven directly.
    #[derive(Clone)]
    struct MockSender {
        retain: usize,
        accept: bool,
    }

    struct MockChecked {
        recipients: Vec<PublicKey>,
        accept: bool,
    }

    impl CheckedSender for MockChecked {
        type PublicKey = PublicKey;

        fn recipients(&self) -> Vec<Self::PublicKey> {
            self.recipients.clone()
        }

        fn send(self, _message: impl Into<IoBufs> + Send, _priority: bool) -> Unreliable<Feedback> {
            if self.accept {
                Unreliable::new(Feedback::Ok)
            } else {
                Unreliable::rejected()
            }
        }
    }

    impl LimitedSender for MockSender {
        type PublicKey = PublicKey;
        type Checked<'a>
            = MockChecked
        where
            Self: 'a;

        fn check(
            &mut self,
            recipients: Recipients<Self::PublicKey>,
        ) -> Result<Self::Checked<'_>, SystemTime> {
            let all = match recipients {
                Recipients::Some(peers) => peers,
                Recipients::One(peer) => vec![peer],
                Recipients::All => Vec::new(),
            };
            let retained: Vec<PublicKey> = all.into_iter().take(self.retain).collect();
            if retained.is_empty() {
                return Err(SystemTime::UNIX_EPOCH);
            }
            Ok(MockChecked {
                recipients: retained,
                accept: self.accept,
            })
        }
    }

    fn broadcast(retain: usize, accept: bool, peers: usize) -> String {
        let metrics = Arc::new(MetricsCollector::new());
        let mut sender = CountingSender::new(MockSender { retain, accept }, Arc::clone(&metrics));
        let recipients = (0..peers as u64).map(key).collect();
        sender.send(Recipients::Some(recipients), bytes::Bytes::from("x"), true);
        metrics.encode()
    }

    #[test]
    fn a_send_with_nothing_to_attribute_leaves_no_series() {
        let metrics = MetricsCollector::new();
        count_directive_send(&metrics, DirectiveSendResult::Rejected, 0);

        assert!(
            !metrics
                .encode()
                .contains("gas_killer_directive_sends_total{")
        );
    }

    #[test]
    fn a_full_delivery_counts_every_recipient_delivered() {
        let output = broadcast(3, true, 3);
        assert!(output.contains("gas_killer_directive_sends_total{result=\"delivered\"} 3"));
        assert!(!output.contains("result=\"rate_limited\""));
        assert!(!output.contains("result=\"rejected\""));
    }

    #[test]
    fn a_partial_drop_is_visible_instead_of_looking_like_a_full_delivery() {
        // This is the case upstream cannot report: two of three peers were over quota, but the
        // send returned a non-empty recipient list, so nothing was logged.
        let output = broadcast(1, true, 3);
        assert!(output.contains("gas_killer_directive_sends_total{result=\"delivered\"} 1"));
        assert!(output.contains("gas_killer_directive_sends_total{result=\"rate_limited\"} 2"));
    }

    #[test]
    fn every_peer_over_quota_counts_them_all_rate_limited() {
        let output = broadcast(0, true, 3);
        assert!(output.contains("gas_killer_directive_sends_total{result=\"rate_limited\"} 3"));
        assert!(!output.contains("result=\"delivered\""));
    }

    #[test]
    fn a_refused_submission_is_counted_apart_from_a_rate_limit() {
        // Both cases surface upstream as an empty recipient list; only the counter tells them
        // apart, which is the difference between "the operators are throttling us" and "our own
        // send buffer refused the message".
        let output = broadcast(3, false, 3);
        assert!(output.contains("gas_killer_directive_sends_total{result=\"rejected\"} 3"));
        assert!(!output.contains("result=\"rate_limited\""));
        assert!(!output.contains("result=\"delivered\""));
    }
}
