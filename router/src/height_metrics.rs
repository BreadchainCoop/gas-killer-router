//! Window and height observability for the aggregation pipeline.
//!
//! The height loop itself lives upstream in [`commonware_avs_router::sequencer`]: it assigns
//! heights, drives each to resolution, and owns the only view of why a height ended. None of
//! that is instrumented, and a pipeline that has stopped resolving heights is otherwise
//! indistinguishable from an idle one — the process is up, memory is flat, and nothing restarts.
//!
//! Everything needed to observe it from outside is created by the router and handed to the
//! sequencer, so this module reads that same shared state rather than changing the loop:
//!
//! - [`SharedAssignments`] holds one entry per height from assignment until it resolves, so its
//!   key range is the live window.
//! - [`DispatchTime`] holds the instant each height was dispatched, so the oldest surviving
//!   entry is the age of the height the pipeline is waiting on.
//! - [`TipReports`] is the floor below which the sequencer refuses to assign.
//! - The resolution channel carries each height's final disposition.
//!
//! [`HeightObserver`] does two jobs against that state. It sits **in** the resolution path
//! rather than beside it (see [`HeightObserver::forward_resolutions`]), and it samples the
//! shared maps on a fixed interval (see [`HeightObserver::sample_forever`]).

use crate::metrics::{HeightOutcome, MetricsCollector, outcome_labels};
use commonware_avs_core::wire::TaskData;
use commonware_avs_router::sequencer::{
    DispatchTime, Resolution, ResolutionKind, ResolutionReceiver, ResolutionSender,
    SharedAssignments, TipReports,
};
use commonware_cryptography::PublicKey;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// How often the shared assignment and dispatch maps are sampled.
///
/// Rounds take tens of seconds, so this only has to be fine enough that a height's appearance
/// and disappearance are both observed. One second leaves the window base, the in-flight count,
/// and the height age accurate to within a scrape interval at negligible cost.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Upper bound on remembered resolutions.
///
/// A resolution is normally matched to its height's disappearance within one sample tick.
/// Entries only accumulate for heights that resolved without ever being assigned — certificates
/// left over from a previous router life, which this router never sees enter or leave the
/// assignment map. Retaining the highest [`RESOLVED_MEMORY`] heights bounds the set without
/// discarding any that a live assignment could still match.
const RESOLVED_MEMORY: usize = 256;

/// What one sample of the assignment map observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WindowSample {
    /// Heights assigned and not yet resolved.
    in_flight: usize,
    /// Lowest and highest assigned height, absent when nothing is in flight.
    base: Option<u64>,
    highest: Option<u64>,
}

/// Reduces the assigned heights to the window's edges. Heights arrive sorted from the
/// assignment map's key order, but this does not rely on that.
fn window_sample(heights: &[u64]) -> WindowSample {
    WindowSample {
        in_flight: heights.len(),
        base: heights.iter().copied().min(),
        highest: heights.iter().copied().max(),
    }
}

/// Age of the oldest height still awaiting its consumer, or `None` when none are.
///
/// Entries are removed by the height's consumer once it has been executed, so a surviving entry
/// is a height the pipeline is still waiting on. A clock that appears to run backwards yields
/// zero rather than wrapping.
fn oldest_age(dispatch_time: &DispatchTime, now: Instant) -> Option<Duration> {
    let times = dispatch_time.lock().ok()?;
    times
        .values()
        .map(|dispatched| now.saturating_duration_since(*dispatched))
        .max()
}

/// Publishes the window's shape and each height's final disposition.
///
/// Cloning shares the underlying state, so the resolution forwarder and the sampler are two
/// clones of one observer.
#[derive(Clone)]
pub struct HeightObserver {
    metrics: Arc<MetricsCollector>,
    /// Heights whose resolution has been seen but whose assignment has not yet been observed to
    /// disappear. Bounded by [`RESOLVED_MEMORY`].
    resolved: Arc<Mutex<BTreeSet<u64>>>,
}

impl HeightObserver {
    pub fn new(metrics: Arc<MetricsCollector>) -> Self {
        Self {
            metrics,
            resolved: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Relays resolutions from the submitter to the sequencer, counting each height's outcome on
    /// the way through.
    ///
    /// This forwards rather than observing from the side, and that is load-bearing for the
    /// `superseded` count below. The sequencer only learns a height resolved by receiving the
    /// forwarded message, and it removes the assignment after that — so a resolution is always
    /// recorded here before its height can disappear from the assignment map. A side observer
    /// would race the removal and misreport healthy heights as superseded.
    pub async fn forward_resolutions(
        self,
        mut incoming: ResolutionReceiver,
        outgoing: ResolutionSender,
    ) {
        while let Some(resolution) = incoming.recv().await {
            self.record_resolution(resolution);
            if outgoing.send(resolution).is_err() {
                info!("sequencer closed the resolution channel; height observer exiting");
                return;
            }
        }
        info!("resolution channel closed; height observer exiting");
    }

    /// Counts one resolution and remembers its height for the superseded check.
    fn record_resolution(&self, resolution: Resolution) {
        let outcome = match resolution.kind {
            ResolutionKind::Executed { .. } => HeightOutcome::Executed,
            ResolutionKind::Skipped => HeightOutcome::Skipped,
            ResolutionKind::Foreign => HeightOutcome::Foreign,
        };
        self.count(outcome);
        self.raise_highest_assigned(resolution.height);

        if let Ok(mut resolved) = self.resolved.lock() {
            resolved.insert(resolution.height);
            // Retain the highest entries; the lowest are the ones a live assignment can no
            // longer match.
            while resolved.len() > RESOLVED_MEMORY {
                resolved.pop_first();
            }
        }
    }

    /// Samples the shared window state until the task is aborted.
    pub async fn sample_forever<T, P>(
        self,
        assignments: SharedAssignments<T>,
        dispatch_time: DispatchTime,
        tip_reports: TipReports<P>,
        interval: Duration,
    ) where
        T: TaskData,
        P: PublicKey,
    {
        // Heights present in the assignment map on the previous tick, so a disappearance can be
        // detected.
        let mut previous: BTreeSet<u64> = BTreeSet::new();
        loop {
            tokio::time::sleep(interval).await;

            let Some(heights) = assigned_heights(&assignments) else {
                warn!("assignments lock poisoned; skipping window sample");
                continue;
            };
            self.publish_window(&heights);
            self.publish_height_age(&dispatch_time);
            self.metrics
                .node_safe_tip
                .set(clamp_to_gauge(tip_reports.safe_tip()));

            let current: BTreeSet<u64> = heights.iter().copied().collect();
            for height in previous.difference(&current) {
                self.classify_departure(*height);
            }
            previous = current;
        }
    }

    /// Sets the window gauges from one sample.
    fn publish_window(&self, heights: &[u64]) {
        let sample = window_sample(heights);
        self.metrics.in_flight_heights.set(sample.in_flight as i64);
        // An idle router has no base to report; the monotonic highest-assigned gauge is what
        // still says where the pipeline got to.
        self.metrics
            .window_base
            .set(sample.base.map(clamp_to_gauge).unwrap_or(0));
        if let Some(highest) = sample.highest {
            self.raise_highest_assigned(highest);
        }
    }

    /// Sets the height-age gauge from the dispatch timestamps.
    fn publish_height_age(&self, dispatch_time: &DispatchTime) {
        let age = oldest_age(dispatch_time, Instant::now())
            .map(|age| clamp_to_gauge(age.as_secs()))
            .unwrap_or(0);
        self.metrics.height_age_seconds.set(age);
    }

    /// Attributes a height that left the assignment map.
    ///
    /// A height that resolved was already counted by [`Self::record_resolution`], so the
    /// remaining case is the one the resolution channel never carries: the sequencer abandoned
    /// the height because operator tip reports proved the quorum is past it. Upstream's only
    /// other path that drops an assignment without a resolution poisons the lock first, which
    /// the sampler reports separately.
    fn classify_departure(&self, height: u64) {
        let resolved = match self.resolved.lock() {
            Ok(mut resolved) => resolved.remove(&height),
            Err(_) => {
                warn!(
                    height,
                    "resolved-height lock poisoned; outcome not attributed"
                );
                return;
            }
        };
        if !resolved {
            debug!(height, "height left the window without resolving");
            self.count(HeightOutcome::Superseded);
        }
    }

    fn count(&self, outcome: HeightOutcome) {
        self.metrics
            .height_outcomes
            .get_or_create(&outcome_labels(outcome))
            .inc();
    }

    /// Raises the monotonic highest-assigned gauge, never lowering it.
    fn raise_highest_assigned(&self, height: u64) {
        let height = clamp_to_gauge(height);
        let gauge = &self.metrics.highest_assigned_height;
        if height > gauge.get() {
            gauge.set(height);
        }
    }
}

/// The heights currently assigned, or `None` if the lock is poisoned.
fn assigned_heights<T: TaskData>(assignments: &SharedAssignments<T>) -> Option<Vec<u64>> {
    let assignments = assignments.read().ok()?;
    Some(assignments.keys().copied().collect())
}

/// Narrows a height or duration to the gauge's signed width, saturating rather than wrapping.
/// Heights this large are unreachable in practice; saturating keeps a nonsensical value visibly
/// pinned instead of flipping negative.
fn clamp_to_gauge(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn observer() -> HeightObserver {
        HeightObserver::new(Arc::new(MetricsCollector::new()))
    }

    #[test]
    fn window_sample_reports_the_edges_of_the_assigned_range() {
        let sample = window_sample(&[120, 121, 123]);
        assert_eq!(sample.in_flight, 3);
        assert_eq!(sample.base, Some(120));
        assert_eq!(sample.highest, Some(123));
    }

    #[test]
    fn window_sample_of_an_idle_router_has_no_edges() {
        let sample = window_sample(&[]);
        assert_eq!(sample.in_flight, 0);
        assert_eq!(sample.base, None);
        assert_eq!(sample.highest, None);
    }

    #[test]
    fn oldest_age_tracks_the_longest_waiting_height() {
        let now = Instant::now();
        let times: DispatchTime = Arc::new(Mutex::new(HashMap::from([
            (10, now - Duration::from_secs(5)),
            (11, now - Duration::from_secs(90)),
            (12, now - Duration::from_secs(30)),
        ])));

        let age = oldest_age(&times, now).expect("entries present");
        assert_eq!(age.as_secs(), 90);
    }

    #[test]
    fn oldest_age_is_absent_when_no_height_is_waiting() {
        let times: DispatchTime = Arc::new(Mutex::new(HashMap::new()));
        assert_eq!(oldest_age(&times, Instant::now()), None);
    }

    #[test]
    fn an_executed_resolution_is_counted_and_not_reported_superseded() {
        let observer = observer();
        observer.record_resolution(Resolution {
            height: 42,
            kind: ResolutionKind::Executed { success: true },
        });
        observer.classify_departure(42);

        let output = observer.metrics.encode();
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"executed\"} 1"));
        assert!(!output.contains("outcome=\"superseded\""));
    }

    #[test]
    fn a_height_leaving_the_window_unresolved_is_reported_superseded() {
        let observer = observer();
        observer.classify_departure(42);

        assert!(
            observer
                .metrics
                .encode()
                .contains("gas_killer_height_outcomes_total{outcome=\"superseded\"} 1")
        );
    }

    #[test]
    fn each_resolution_matches_only_its_own_departure() {
        let observer = observer();
        observer.record_resolution(Resolution {
            height: 7,
            kind: ResolutionKind::Skipped,
        });
        // The same height cannot be assigned twice, but a second departure must not silently
        // consume another height's record.
        observer.classify_departure(7);
        observer.classify_departure(8);

        let output = observer.metrics.encode();
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"skipped\"} 1"));
        assert!(output.contains("gas_killer_height_outcomes_total{outcome=\"superseded\"} 1"));
    }

    #[test]
    fn remembered_resolutions_stay_bounded_and_keep_the_highest() {
        let observer = observer();
        for height in 0..(RESOLVED_MEMORY as u64 + 50) {
            observer.record_resolution(Resolution {
                height,
                kind: ResolutionKind::Foreign,
            });
        }

        let resolved = observer.resolved.lock().expect("lock held");
        assert_eq!(resolved.len(), RESOLVED_MEMORY);
        // The highest heights survive: those are the ones a live assignment can still match.
        assert!(resolved.contains(&(RESOLVED_MEMORY as u64 + 49)));
        assert!(!resolved.contains(&0));
    }

    #[test]
    fn highest_assigned_height_never_falls_back() {
        let observer = observer();
        observer.publish_window(&[120, 123]);
        assert_eq!(observer.metrics.highest_assigned_height.get(), 123);

        // The window empties when the router goes idle; the high-water mark holds.
        observer.publish_window(&[]);
        assert_eq!(observer.metrics.highest_assigned_height.get(), 123);
        assert_eq!(observer.metrics.window_base.get(), 0);
        assert_eq!(observer.metrics.in_flight_heights.get(), 0);
    }
}
