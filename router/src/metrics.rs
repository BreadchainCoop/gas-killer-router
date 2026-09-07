use commonware_runtime::telemetry::metrics::encoding::text::encode;
use commonware_runtime::telemetry::metrics::raw::{Counter, Family, Gauge, Histogram};
use commonware_runtime::telemetry::metrics::registry::Registry;
use gas_killer_common::ChainRole;
use std::sync::atomic::{AtomicI64, AtomicU64};

/// Label set scoping a counter to one API key: the key's public id, never the key value. Matches
/// the `key_id` field in the audit log, so a spike on a series can be traced straight to the
/// request lines that produced it.
type KeyLabels = [(&'static str, String); 1];

/// An ingress counter broken down by the API key that drove it.
///
/// Cardinality is one series per key that has made a request since the process started, which for
/// a keyed beta is a handful. A key that is revoked keeps its series until the router restarts —
/// the series is the record of what that key did, so retiring it early would erase the history an
/// investigation needs.
pub type PerKeyCounter = Family<KeyLabels, Counter<u64, AtomicU64>>;

/// The label set naming `key_id`, for indexing a [`PerKeyCounter`].
pub fn key_labels(key_id: &str) -> KeyLabels {
    [("key_id", key_id.to_string())]
}

/// Label set scoping a metric to one chain role, rendered as `chain="l1"` / `chain="l2"`.
type ChainLabels = [(&'static str, String); 1];

/// A gauge broken down by chain role. Cardinality is bounded by the roles the deployment
/// configures, so at most two series.
pub type PerChainGauge = Family<ChainLabels, Gauge<i64, AtomicI64>>;

/// The label set naming `chain`, for indexing a [`PerChainGauge`].
pub fn chain_labels(chain: ChainRole) -> ChainLabels {
    [("chain", chain.name().to_string())]
}

/// Label set scoping a counter to one per-height disposition, rendered as `outcome="executed"`.
type OutcomeLabels = [(&'static str, String); 1];

/// A counter broken down by how an assigned aggregation height ended. Cardinality is the four
/// [`HeightOutcome`] variants.
pub type PerOutcomeCounter = Family<OutcomeLabels, Counter<u64, AtomicU64>>;

/// The label set naming `outcome`, for indexing a [`PerOutcomeCounter`].
pub fn outcome_labels(outcome: HeightOutcome) -> OutcomeLabels {
    [("outcome", outcome.as_str().to_string())]
}

/// How an assigned aggregation height was finally disposed of.
///
/// `Executed`, `Skipped`, and `Foreign` mirror the `ResolutionKind` the submitter reports to the
/// sequencer. `Superseded` has no `ResolutionKind` of its own: the sequencer abandons a height
/// when the operators' reported tips prove the quorum is already past it, so the height ends
/// without any certificate ever arriving for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeightOutcome {
    /// A certificate carried the height's expected digest and on-chain settlement finished.
    Executed,
    /// A certificate carried the skip digest: the quorum abandoned the height and its task.
    Skipped,
    /// A certificate carried neither the expected digest nor the skip digest, or the height had
    /// no assignment at all — a leftover from a previous router life.
    Foreign,
    /// Operator tip reports moved past the height before it could certify, so it can never
    /// certify and the task is re-assigned higher.
    Superseded,
}

impl HeightOutcome {
    /// The `outcome` label value for this disposition.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Executed => "executed",
            Self::Skipped => "skipped",
            Self::Foreign => "foreign",
            Self::Superseded => "superseded",
        }
    }
}

/// Label set scoping a counter to one directive-send result, rendered as `result="delivered"`.
type SendResultLabels = [(&'static str, String); 1];

/// A counter broken down by what happened to one recipient's copy of a directive. Cardinality is
/// the three [`DirectiveSendResult`] variants.
pub type PerSendResultCounter = Family<SendResultLabels, Counter<u64, AtomicU64>>;

/// The label set naming `result`, for indexing a [`PerSendResultCounter`].
pub fn send_result_labels(result: DirectiveSendResult) -> SendResultLabels {
    [("result", result.as_str().to_string())]
}

/// What became of one recipient's copy of a broadcast task directive.
///
/// The p2p sender collapses all three cases into one return value — the list of peers it will
/// attempt — so a partial drop is indistinguishable from a full delivery at the call site.
/// Counting the cases apart is what separates "the operators are throttling us" from "the local
/// send buffer refused the message" from a healthy broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveSendResult {
    /// The recipient was within its rate limit and the local send was accepted.
    Delivered,
    /// The recipient's per-peer quota was exhausted, so its copy was dropped before sending.
    RateLimited,
    /// The recipient passed the rate-limit check but the local send was not accepted
    /// (backpressure, or a closed sender), so nothing went out to it.
    Rejected,
}

impl DirectiveSendResult {
    /// The `result` label value for this outcome.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::RateLimited => "rate_limited",
            Self::Rejected => "rejected",
        }
    }
}

pub struct MetricsCollector {
    registry: Registry,
    /// Ingress requests that passed validation and were queued, by calling key.
    pub ingress_accepted: PerKeyCounter,
    /// Ingress requests that collapsed onto an existing task via deduplication, by calling key.
    pub ingress_deduplicated: PerKeyCounter,
    /// Ingress requests rejected by validation, by calling key.
    pub ingress_rejected: PerKeyCounter,
    /// Ingress requests dropped because the task queue is at capacity, by calling key.
    pub ingress_at_capacity: PerKeyCounter,
    /// Ingress requests rejected by per-API-key rate limiting, by calling key.
    pub ingress_rate_limited: PerKeyCounter,
    /// Ingress requests refused because the chain every round depends on is unavailable, by
    /// calling key.
    pub ingress_rpc_unavailable: PerKeyCounter,
    /// Tasks dequeued and handed to the creator for aggregation.
    pub tasks_created: Counter<u64, AtomicU64>,
    /// Tasks settled `expired` by the periodic TTL sweep, across every stage it visits.
    pub tasks_expired: Counter<u64, AtomicU64>,
    /// Tasks the startup re-queue settled `expired` because their transition index had already
    /// been applied on chain, rather than re-running work the contract would reject.
    pub tasks_expired_at_requeue: Counter<u64, AtomicU64>,
    /// EVM storage-update computation duration (seconds).
    pub storage_computation_seconds: Histogram,
    /// Aggregation rounds that ended in a successful verifyAndUpdate transaction.
    pub aggregation_rounds_completed: Counter<u64, AtomicU64>,
    /// Aggregation rounds that failed (hash mismatch, tx error, etc.).
    pub aggregation_rounds_failed: Counter<u64, AtomicU64>,
    /// Rounds abandoned because the rendered payload's `verifyAndUpdate` reverted when estimated,
    /// which is almost always a misconfigured target (wrong AVS or signature checker) rather than
    /// a router fault. Tracked apart from `aggregation_rounds_failed` so that cause is visible
    /// without reading logs.
    pub payloads_rejected_reverting: Counter<u64, AtomicU64>,
    /// Full handle_verification duration including contract calls and tx submission (seconds).
    pub execution_duration_seconds: Histogram,
    /// Time from creator dispatching a task to the executor receiving threshold signatures (seconds).
    /// Captures P2P transit + node EVMSketch + BLS signing + aggregation.
    pub p2p_round_trip_seconds: Histogram,
    /// End-to-end round latency from creator dispatch to verifyAndUpdate receipt confirmation
    /// (seconds). Observed only for rounds that complete successfully, so failed rounds — which
    /// have no on-chain confirmation — cannot skew the percentiles with receipt-timeout artifacts.
    /// Excludes ingress-queue wait and router-side storage computation, which finish before the
    /// dispatch timestamp is stamped.
    pub round_latency_seconds: Histogram,
    /// Wall-clock time from a task being accepted at the ingress to it settling `ready` or
    /// `failed` (seconds). Unlike [`Self::round_latency_seconds`] this includes the ingress queue
    /// wait and the router-side storage computation, so it is the latency a client actually
    /// experiences. Resolution is whole seconds — both ends are read from the task row's
    /// second-granularity timestamps.
    pub task_e2e_seconds: Histogram,
    /// Current ingress queue depth: enqueued tasks awaiting processing plus submissions
    /// holding a reserved slot while they validate. Reserved slots are released if the
    /// submission is rejected, so a brief bump under a flood of invalid requests is expected
    /// backpressure, not a leak.
    pub task_queue_depth: Gauge<i64, AtomicI64>,
    /// Whether the SQLite store answered its most recent health check (1 = up, 0 = down).
    pub db_up: Gauge<i64, AtomicI64>,
    /// Whether each chain's RPC is currently usable (1 = healthy, 0 = degraded), as judged by
    /// [`crate::rpc_health::RpcHealth`]. Zero means consecutive failures reached the configured
    /// threshold; it returns to one on the next success.
    pub rpc_healthy: PerChainGauge,
    /// Time for the payload-hash preflight computation (seconds).
    pub executor_hash_preflight_seconds: Histogram,
    /// Time for supportsInterface ERC-165 check (seconds).
    pub executor_supports_interface_seconds: Histogram,
    /// Time from calling verifyAndUpdate to receiving the pending tx handle (seconds).
    pub executor_tx_send_seconds: Histogram,
    /// Time waiting for the verifyAndUpdate receipt to be mined (seconds).
    pub executor_receipt_confirmation_seconds: Histogram,
    /// Aggregation heights currently assigned and not yet resolved.
    pub in_flight_heights: Gauge<i64, AtomicI64>,
    /// Lowest height currently assigned, or 0 when nothing is in flight. Paired with
    /// [`Self::highest_assigned_height`] this is what makes a stalled window visible: both stay
    /// pinned while heights stop resolving, where a healthy pipeline advances them together.
    pub window_base: Gauge<i64, AtomicI64>,
    /// Highest height ever assigned during this router life. Monotonic, so an idle router holds
    /// the last value rather than reporting an empty window.
    pub highest_assigned_height: Gauge<i64, AtomicI64>,
    /// Age in whole seconds of the oldest height still awaiting its certificate, or 0 when
    /// nothing is in flight.
    pub height_age_seconds: Gauge<i64, AtomicI64>,
    /// How assigned heights ended, by disposition.
    pub height_outcomes: PerOutcomeCounter,
    /// The `(f+1)`-th highest tip the operators have reported, which is the floor the sequencer
    /// will not assign below. A height at or under this value can never certify, so a rising
    /// safe tip is the cause behind an `outcome="superseded"` spike.
    pub node_safe_tip: Gauge<i64, AtomicI64>,
    /// Per-recipient outcomes of task-directive broadcasts, counted at the send site.
    pub directive_sends: PerSendResultCounter,
    /// Terminal-state transitions the store refused because the task had already settled. A
    /// nonzero value means a task was settled twice and its row may carry another task's
    /// payload, so it is never expected in a healthy deployment.
    pub settlement_conflicts: Counter<u64, AtomicU64>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let ingress_accepted = Family::default();
        registry.register(
            "gas_killer_ingress_requests_accepted",
            "Total ingress task requests accepted and queued, by calling API key",
            ingress_accepted.clone(),
        );

        let ingress_deduplicated = Family::default();
        registry.register(
            "gas_killer_ingress_requests_deduplicated",
            "Total ingress task requests that collapsed onto an existing task via deduplication, by calling API key",
            ingress_deduplicated.clone(),
        );

        let ingress_rejected = Family::default();
        registry.register(
            "gas_killer_ingress_requests_rejected",
            "Total ingress task requests rejected by validation, by calling API key",
            ingress_rejected.clone(),
        );

        let ingress_at_capacity = Family::default();
        registry.register(
            "gas_killer_ingress_requests_at_capacity",
            "Total ingress task requests dropped because the queue was full, by calling API key",
            ingress_at_capacity.clone(),
        );

        let ingress_rate_limited = Family::default();
        registry.register(
            "gas_killer_ingress_requests_rate_limited",
            "Total ingress task requests rejected by per-API-key rate limiting, by calling API key",
            ingress_rate_limited.clone(),
        );

        let ingress_rpc_unavailable = Family::default();
        registry.register(
            "gas_killer_ingress_requests_rpc_unavailable",
            "Total ingress task requests refused because a required chain's RPC is unavailable, by calling API key",
            ingress_rpc_unavailable.clone(),
        );

        let tasks_created = Counter::default();
        registry.register(
            "gas_killer_tasks_created",
            "Total tasks dequeued and processed by the creator",
            tasks_created.clone(),
        );

        let tasks_expired = Counter::default();
        registry.register(
            "gas_killer_tasks_expired",
            "Total tasks settled expired by the periodic task-TTL sweep",
            tasks_expired.clone(),
        );

        let tasks_expired_at_requeue = Counter::default();
        registry.register(
            "gas_killer_tasks_expired_at_requeue",
            "Total recovered tasks settled expired at startup because their transition index was already applied on chain",
            tasks_expired_at_requeue.clone(),
        );

        let storage_computation_seconds =
            Histogram::new([0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0, 120.0, 300.0]);
        registry.register(
            "gas_killer_storage_computation_seconds",
            "EVM storage-update computation duration in seconds",
            storage_computation_seconds.clone(),
        );

        let aggregation_rounds_completed = Counter::default();
        registry.register(
            "gas_killer_aggregation_rounds_completed",
            "Total aggregation rounds completed with a successful verifyAndUpdate transaction",
            aggregation_rounds_completed.clone(),
        );

        let aggregation_rounds_failed = Counter::default();
        registry.register(
            "gas_killer_aggregation_rounds_failed",
            "Total aggregation rounds that failed (hash mismatch, tx error, interface check, etc.)",
            aggregation_rounds_failed.clone(),
        );

        // Fast reverts (~sub-second, fail at tx send) and confirmed runs (~block-time dominated); Buckets resolve both ends.
        let payloads_rejected_reverting = Counter::default();
        registry.register(
            "gas_killer_payloads_rejected_reverting",
            "Total rounds failed because the rendered verifyAndUpdate reverted when estimated",
            payloads_rejected_reverting.clone(),
        );

        let execution_duration_seconds = Histogram::new([
            0.5, 1.0, 2.0, 5.0, 8.0, 12.0, 16.0, 20.0, 24.0, 30.0, 45.0, 60.0, 120.0, 300.0,
        ]);
        registry.register(
            "gas_killer_execution_duration_seconds",
            "Duration of handle_verification including all contract calls and tx submission",
            execution_duration_seconds.clone(),
        );

        let p2p_round_trip_seconds =
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]);
        registry.register(
            "gas_killer_p2p_round_trip_seconds",
            "Time from creator dispatching a task to executor receiving threshold signatures (P2P transit + node EVMSketch + BLS signing + aggregation)",
            p2p_round_trip_seconds.clone(),
        );

        // Roughly p2p_round_trip + execution_duration; block-time dominated, so the
        // execution-duration buckets resolve it well.
        let round_latency_seconds = Histogram::new([
            0.5, 1.0, 2.0, 5.0, 8.0, 12.0, 16.0, 20.0, 24.0, 30.0, 45.0, 60.0, 120.0, 300.0,
        ]);
        registry.register(
            "gas_killer_round_latency_seconds",
            "End-to-end round latency from creator dispatch to verifyAndUpdate receipt confirmation (successful rounds only)",
            round_latency_seconds.clone(),
        );

        // Spans the ingress queue wait as well as the round itself, so it needs headroom well
        // past the round-latency buckets: a task can sit queued behind others for minutes.
        let task_e2e_seconds = Histogram::new([
            1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 120.0, 300.0, 600.0,
        ]);
        registry.register(
            "gas_killer_task_e2e_seconds",
            "Wall-clock seconds from ingress acceptance to a task settling ready or failed",
            task_e2e_seconds.clone(),
        );

        let task_queue_depth = Gauge::default();
        registry.register(
            "gas_killer_queue_depth",
            "Ingress queue depth: enqueued tasks awaiting processing plus submissions holding a reserved slot during validation",
            task_queue_depth.clone(),
        );

        let db_up = Gauge::default();
        registry.register(
            "gas_killer_db_up",
            "Whether the SQLite store answered its most recent health check (1 = up, 0 = down)",
            db_up.clone(),
        );

        let rpc_healthy = Family::default();
        registry.register(
            "gas_killer_rpc_healthy",
            "Whether each chain's RPC is usable (1 = healthy, 0 = degraded past the consecutive-failure threshold)",
            rpc_healthy.clone(),
        );

        // Single same-RPC round-trips (~5-150ms); fine low-end buckets so p50/p95 resolve.
        let rpc_buckets = [
            0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0, 2.5,
        ];
        let executor_hash_preflight_seconds = Histogram::new(rpc_buckets);
        registry.register(
            "gas_killer_executor_hash_preflight_seconds",
            "Time for the payload-hash preflight computation",
            executor_hash_preflight_seconds.clone(),
        );

        let executor_supports_interface_seconds = Histogram::new(rpc_buckets);
        registry.register(
            "gas_killer_executor_supports_interface_seconds",
            "Time for the supportsInterface ERC-165 check",
            executor_supports_interface_seconds.clone(),
        );

        let executor_tx_send_seconds = Histogram::new([0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]);
        registry.register(
            "gas_killer_executor_tx_send_seconds",
            "Time from calling verifyAndUpdate to receiving the pending tx handle",
            executor_tx_send_seconds.clone(),
        );

        // Block-time driven (~1-2 confirmations); dense through the 8-30s window.
        let executor_receipt_confirmation_seconds = Histogram::new([
            1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 15.0, 18.0, 24.0, 30.0, 45.0, 60.0, 120.0,
        ]);
        registry.register(
            "gas_killer_executor_receipt_confirmation_seconds",
            "Time waiting for the verifyAndUpdate receipt to be mined",
            executor_receipt_confirmation_seconds.clone(),
        );

        let in_flight_heights = Gauge::default();
        registry.register(
            "gas_killer_in_flight_heights",
            "Aggregation heights currently assigned and not yet resolved",
            in_flight_heights.clone(),
        );

        let window_base = Gauge::default();
        registry.register(
            "gas_killer_window_base",
            "Lowest aggregation height currently assigned, or 0 when nothing is in flight",
            window_base.clone(),
        );

        let highest_assigned_height = Gauge::default();
        registry.register(
            "gas_killer_highest_assigned_height",
            "Highest aggregation height ever assigned during this router life",
            highest_assigned_height.clone(),
        );

        let height_age_seconds = Gauge::default();
        registry.register(
            "gas_killer_height_age_seconds",
            "Age in seconds of the oldest aggregation height still awaiting its certificate",
            height_age_seconds.clone(),
        );

        let height_outcomes = Family::default();
        registry.register(
            "gas_killer_height_outcomes",
            "Total assigned aggregation heights by final disposition",
            height_outcomes.clone(),
        );

        let node_safe_tip = Gauge::default();
        registry.register(
            "gas_killer_node_safe_tip",
            "Highest aggregation tip reachable per the operators' tip reports, the floor the sequencer will not assign below",
            node_safe_tip.clone(),
        );

        let directive_sends = Family::default();
        registry.register(
            "gas_killer_directive_sends",
            "Total per-recipient task-directive send attempts by result, counted at the send site",
            directive_sends.clone(),
        );

        let settlement_conflicts = Counter::default();
        registry.register(
            "gas_killer_settlement_conflicts",
            "Total terminal-state transitions refused because the task had already settled",
            settlement_conflicts.clone(),
        );

        Self {
            registry,
            ingress_accepted,
            ingress_deduplicated,
            ingress_rejected,
            ingress_at_capacity,
            ingress_rate_limited,
            ingress_rpc_unavailable,
            tasks_created,
            tasks_expired,
            tasks_expired_at_requeue,
            storage_computation_seconds,
            aggregation_rounds_completed,
            aggregation_rounds_failed,
            payloads_rejected_reverting,
            execution_duration_seconds,
            p2p_round_trip_seconds,
            round_latency_seconds,
            task_e2e_seconds,
            task_queue_depth,
            db_up,
            rpc_healthy,
            executor_hash_preflight_seconds,
            executor_supports_interface_seconds,
            executor_tx_send_seconds,
            executor_receipt_confirmation_seconds,
            in_flight_heights,
            window_base,
            highest_assigned_height,
            height_age_seconds,
            height_outcomes,
            node_safe_tip,
            directive_sends,
            settlement_conflicts,
        }
    }

    pub fn encode(&self) -> String {
        let mut output = String::new();
        encode(&mut output, &self.registry).expect("metrics encoding failed");
        output
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_latency_histogram_registered_and_observable() {
        let metrics = MetricsCollector::new();
        metrics.round_latency_seconds.observe(12.5);

        let output = metrics.encode();
        assert!(output.contains(
            "gas_killer_round_latency_seconds End-to-end round latency from creator dispatch"
        ));
        assert!(output.contains("gas_killer_round_latency_seconds_count 1"));
        assert!(output.contains("gas_killer_round_latency_seconds_sum 12.5"));
    }

    #[test]
    fn per_key_counters_emit_one_series_per_key() {
        let metrics = MetricsCollector::new();
        metrics
            .ingress_accepted
            .get_or_create(&key_labels("aaaa1111"))
            .inc();
        metrics
            .ingress_accepted
            .get_or_create(&key_labels("bbbb2222"))
            .inc_by(3);
        metrics
            .ingress_rate_limited
            .get_or_create(&key_labels("bbbb2222"))
            .inc();

        let output = metrics.encode();
        assert!(
            output.contains("gas_killer_ingress_requests_accepted_total{key_id=\"aaaa1111\"} 1")
        );
        assert!(
            output.contains("gas_killer_ingress_requests_accepted_total{key_id=\"bbbb2222\"} 3")
        );
        assert!(
            output
                .contains("gas_killer_ingress_requests_rate_limited_total{key_id=\"bbbb2222\"} 1")
        );
        // A key that has not driven this counter has no series on it, so a per-key panel shows
        // only keys that actually did something.
        assert!(
            !output.contains("gas_killer_ingress_requests_rate_limited_total{key_id=\"aaaa1111\"}")
        );
    }

    #[test]
    fn task_e2e_histogram_registered_and_observable() {
        let metrics = MetricsCollector::new();
        metrics.task_e2e_seconds.observe(42.0);

        let output = metrics.encode();
        assert!(
            output
                .contains("gas_killer_task_e2e_seconds Wall-clock seconds from ingress acceptance")
        );
        assert!(output.contains("gas_killer_task_e2e_seconds_count 1"));
        assert!(output.contains("gas_killer_task_e2e_seconds_sum 42.0"));
    }

    #[test]
    fn test_db_up_gauge_registered_and_reports_status() {
        let metrics = MetricsCollector::new();
        metrics.db_up.set(1);

        let output = metrics.encode();
        assert!(output.contains("Whether the SQLite store answered its most recent health check"));
        assert!(output.contains("gas_killer_db_up 1"));

        metrics.db_up.set(0);
        assert!(metrics.encode().contains("gas_killer_db_up 0"));
    }

    #[test]
    fn window_gauges_report_the_assigned_height_range() {
        let metrics = MetricsCollector::new();
        metrics.in_flight_heights.set(4);
        metrics.window_base.set(120);
        metrics.highest_assigned_height.set(123);
        metrics.height_age_seconds.set(87);
        metrics.node_safe_tip.set(119);

        let output = metrics.encode();
        assert!(output.contains("gas_killer_in_flight_heights 4"));
        assert!(output.contains("gas_killer_window_base 120"));
        assert!(output.contains("gas_killer_highest_assigned_height 123"));
        assert!(output.contains("gas_killer_height_age_seconds 87"));
        assert!(output.contains("gas_killer_node_safe_tip 119"));
    }

    #[test]
    fn height_outcomes_emit_one_series_per_disposition() {
        let metrics = MetricsCollector::new();
        for outcome in [
            HeightOutcome::Executed,
            HeightOutcome::Executed,
            HeightOutcome::Skipped,
            HeightOutcome::Foreign,
            HeightOutcome::Superseded,
        ] {
            metrics
                .height_outcomes
                .get_or_create(&outcome_labels(outcome))
                .inc();
        }

        let output = metrics.encode();
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"executed\"} 2"));
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"skipped\"} 1"));
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"foreign\"} 1"));
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"superseded\"} 1"));
    }

    #[test]
    fn directive_sends_separate_delivery_from_the_two_drop_causes() {
        let metrics = MetricsCollector::new();
        metrics
            .directive_sends
            .get_or_create(&send_result_labels(DirectiveSendResult::Delivered))
            .inc_by(2);
        metrics
            .directive_sends
            .get_or_create(&send_result_labels(DirectiveSendResult::RateLimited))
            .inc();

        let output = metrics.encode();
        assert!(output.contains("gas_killer_directive_sends_total{result=\"delivered\"} 2"));
        assert!(output.contains("gas_killer_directive_sends_total{result=\"rate_limited\"} 1"));
        // A cause that has not occurred has no series, so a panel shows only real drops.
        assert!(!output.contains("gas_killer_directive_sends_total{result=\"rejected\"}"));
    }

    #[test]
    fn settlement_conflicts_export_zero_until_one_happens() {
        let metrics = MetricsCollector::new();
        let output = metrics.encode();
        assert!(output.contains("gas_killer_settlement_conflicts_total 0"));

        metrics.settlement_conflicts.inc();
        assert!(
            metrics
                .encode()
                .contains("gas_killer_settlement_conflicts_total 1")
        );
    }
}
