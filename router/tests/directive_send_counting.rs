//! Directive send counting against a real p2p rate limiter.
//!
//! `CountingSender` sits in the router's directive send path, so two things have to hold: it must
//! be a transparent `LimitedSender` (a directive the sequencer broadcasts still reaches the
//! operators), and its counts must match what the rate limiter actually did.
//!
//! The simulated network's sender is built on the same `commonware_p2p::utils::limited`
//! rate limiter the production network uses, so these tests exercise the real filtering
//! behaviour rather than a stand-in for it.

use commonware_cryptography::{Signer as _, ed25519};
use commonware_p2p::simulated::{Config, Link, Network};
use commonware_p2p::{Receiver as _, Recipients, Sender as _};
use commonware_runtime::{Clock as _, Quota, Runner, Supervisor as _, deterministic};
use commonware_utils::{NZU32, NZUsize};
use gas_killer_router::directive_metrics::CountingSender;
use gas_killer_router::metrics::MetricsCollector;
use std::sync::Arc;
use std::time::Duration;

/// The channel the router broadcasts task directives on.
const DIRECTIVE_CHANNEL: u64 = 1;

/// One operator plus the three the local fleet runs.
const OPERATORS: usize = 3;

/// Extracts a `gas_killer_directive_sends_total` series, or 0 when it has none.
fn sends(metrics: &MetricsCollector, result: &str) -> u64 {
    let needle = format!("gas_killer_directive_sends_total{{result=\"{result}\"}} ");
    metrics
        .encode()
        .lines()
        .find_map(|line| line.strip_prefix(needle.as_str()))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// A router and `OPERATORS` operators, fully linked, with the router's directive sender wrapped.
///
/// The quota matches the production default for this channel: one message per second per peer,
/// which governor gives a burst of one.
#[test]
fn the_production_quota_is_counted_exactly_as_the_limiter_applies_it() {
    let executor = deterministic::Runner::seeded(0);
    executor.start(|context| async move {
        let router = ed25519::PrivateKey::from_seed(0).public_key();
        let operators: Vec<_> = (1..=OPERATORS as u64)
            .map(|seed| ed25519::PrivateKey::from_seed(seed).public_key())
            .collect();
        let mut peers = vec![router.clone()];
        peers.extend(operators.iter().cloned());

        let (network, oracle) = Network::new_with_peers(
            context.child("network"),
            Config {
                max_size: 1024 * 1024,
                disconnect_on_block: true,
                tracked_peer_sets: NZUsize!(3),
            },
            peers.clone(),
        )
        .await;
        network.start();

        let quota = Quota::per_second(NZU32!(1));
        let (raw_sender, _router_receiver) = oracle
            .control(router.clone())
            .register(DIRECTIVE_CHANNEL, quota)
            .await
            .expect("router registers the directive channel");

        let mut operator_receivers = Vec::new();
        for operator in &operators {
            let (_, receiver) = oracle
                .control(operator.clone())
                .register(DIRECTIVE_CHANNEL, quota)
                .await
                .expect("operator registers the directive channel");
            operator_receivers.push(receiver);
        }

        for operator in &operators {
            oracle
                .add_link(
                    router.clone(),
                    operator.clone(),
                    Link {
                        latency: Duration::ZERO,
                        jitter: Duration::ZERO,
                        success_rate: 1.0,
                    },
                )
                .await
                .expect("router links to the operator");
        }

        let metrics = Arc::new(MetricsCollector::new());
        let mut sender = CountingSender::new(raw_sender, Arc::clone(&metrics));
        let recipients = Recipients::Some(operators.clone());

        // First broadcast: every operator is within its quota.
        let reached = sender.send(recipients.clone(), b"announce-1".to_vec(), true);
        assert_eq!(
            reached.len(),
            OPERATORS,
            "the wrapper must not narrow the recipient set the limiter returned"
        );
        assert_eq!(sends(&metrics, "delivered"), OPERATORS as u64);
        assert_eq!(sends(&metrics, "rate_limited"), 0);
        assert_eq!(sends(&metrics, "rejected"), 0);

        // Every operator actually received it: the wrapper is transparent, not just accurate.
        for receiver in &mut operator_receivers {
            let (from, message) = receiver
                .recv()
                .await
                .expect("operator receives the directive");
            assert_eq!(from, router);
            assert_eq!(message.as_ref(), b"announce-1");
        }

        // Second broadcast in the same second: the burst of one is spent, so the limiter drops
        // every copy before it is sent. This is the production hazard — the router is told
        // nothing beyond an empty recipient list, which is also what a healthy send to no peers
        // looks like.
        let reached = sender.send(recipients.clone(), b"announce-2".to_vec(), true);
        assert!(
            reached.is_empty(),
            "the burst is spent, so the limiter retains nobody"
        );
        assert_eq!(
            sends(&metrics, "delivered"),
            OPERATORS as u64,
            "no new delivery"
        );
        assert_eq!(sends(&metrics, "rate_limited"), OPERATORS as u64);

        // Once the quota refills the same broadcast goes through again, so the counter tracks
        // the limiter rather than latching.
        context.sleep(Duration::from_secs(2)).await;
        let reached = sender.send(recipients, b"announce-3".to_vec(), true);
        assert_eq!(reached.len(), OPERATORS);
        assert_eq!(sends(&metrics, "delivered"), 2 * OPERATORS as u64);
        assert_eq!(sends(&metrics, "rate_limited"), OPERATORS as u64);
    });
}

/// A partial drop is the case upstream cannot report: the recipient list comes back non-empty,
/// so the broadcast looks healthy while some operators never saw the directive.
#[test]
fn a_partially_limited_broadcast_is_counted_on_both_sides() {
    let executor = deterministic::Runner::seeded(0);
    executor.start(|context| async move {
        let router = ed25519::PrivateKey::from_seed(0).public_key();
        let operators: Vec<_> = (1..=OPERATORS as u64)
            .map(|seed| ed25519::PrivateKey::from_seed(seed).public_key())
            .collect();
        let mut peers = vec![router.clone()];
        peers.extend(operators.iter().cloned());

        let (network, oracle) = Network::new_with_peers(
            context.child("network"),
            Config {
                max_size: 1024 * 1024,
                disconnect_on_block: true,
                tracked_peer_sets: NZUsize!(3),
            },
            peers.clone(),
        )
        .await;
        network.start();

        let quota = Quota::per_second(NZU32!(1));
        let (raw_sender, _router_receiver) = oracle
            .control(router.clone())
            .register(DIRECTIVE_CHANNEL, quota)
            .await
            .expect("router registers the directive channel");
        for operator in &operators {
            oracle
                .control(operator.clone())
                .register(DIRECTIVE_CHANNEL, quota)
                .await
                .expect("operator registers the directive channel");
            oracle
                .add_link(
                    router.clone(),
                    operator.clone(),
                    Link {
                        latency: Duration::ZERO,
                        jitter: Duration::ZERO,
                        success_rate: 1.0,
                    },
                )
                .await
                .expect("router links to the operator");
        }

        let metrics = Arc::new(MetricsCollector::new());
        let mut sender = CountingSender::new(raw_sender, Arc::clone(&metrics));

        // Spend the first operator's burst on its own, then broadcast to all three. Two are
        // still within quota, so the limiter returns a non-empty list that is missing one.
        sender.send(
            Recipients::One(operators[0].clone()),
            b"announce-1".to_vec(),
            true,
        );
        assert_eq!(sends(&metrics, "delivered"), 1);

        let reached = sender.send(
            Recipients::Some(operators.clone()),
            b"announce-2".to_vec(),
            true,
        );
        assert_eq!(
            reached.len(),
            OPERATORS - 1,
            "the spent peer is filtered out while the rest go through"
        );
        assert_eq!(sends(&metrics, "delivered"), 1 + (OPERATORS as u64 - 1));
        assert_eq!(
            sends(&metrics, "rate_limited"),
            1,
            "the one dropped copy is counted even though the broadcast looked successful"
        );
    });
}
