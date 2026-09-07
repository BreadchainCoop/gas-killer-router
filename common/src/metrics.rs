//! Metrics both binaries publish about their own configuration.
//!
//! The router and every operator must agree on a handful of settings — the ones that feed the
//! task digest, and the ones that decide which heights a binary will sign. A fleet that
//! disagrees on any of them does not fail loudly: the peers stay connected, the quorum simply
//! never forms, and every pod goes on reporting itself healthy while nothing settles. Recovering
//! from that means stopping the whole fleet, so detecting it early is worth a metric of its own.
//!
//! [`ConfigMetrics`] carries its own registry, exposed alongside the runtime's on each binary's
//! `/metrics` endpoint, so the same fingerprint is published identically by the router and the
//! operators and a split is a single query across the deployment.

use commonware_runtime::telemetry::metrics::encoding::text::encode;
use commonware_runtime::telemetry::metrics::raw::{Family, Gauge};
use commonware_runtime::telemetry::metrics::registry::Registry;
use std::sync::atomic::AtomicI64;

/// Label set naming the fingerprint value, rendered as `fingerprint="1a2b3c4d5e6f7a8b"`.
type FingerprintLabels = [(&'static str, String); 1];

/// The label set naming `fingerprint`.
fn fingerprint_labels(fingerprint: &str) -> FingerprintLabels {
    [("fingerprint", fingerprint.to_string())]
}

/// Configuration identity for one process, published on its `/metrics` endpoint.
///
/// Holds only the registry: the gauge is written once at construction and never again, so the
/// registry's own clone of it is the only handle anything needs afterwards.
pub struct ConfigMetrics {
    registry: Registry,
}

impl ConfigMetrics {
    /// Registers the gauge and publishes `fingerprint`.
    ///
    /// One series per process life, always 1: the information is in the label, so counting the
    /// distinct labels across the deployment is what reveals a split fleet. The fingerprint is
    /// read once at startup because the settings behind it are read once at startup; a process
    /// that needs different settings is restarted.
    pub fn new(fingerprint: &str) -> Self {
        let mut registry = Registry::default();

        let config_fingerprint: Family<FingerprintLabels, Gauge<i64, AtomicI64>> =
            Family::default();
        registry.register(
            "gas_killer_config_fingerprint",
            "Always 1, labelled with this process's consensus-critical configuration fingerprint",
            config_fingerprint.clone(),
        );
        config_fingerprint
            .get_or_create(&fingerprint_labels(fingerprint))
            .set(1);

        Self { registry }
    }

    /// Prometheus text exposition of this registry.
    pub fn encode(&self) -> String {
        let mut output = String::new();
        encode(&mut output, &self.registry).expect("metrics encoding failed");
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fingerprint_is_published_as_a_label_on_a_constant_gauge() {
        let metrics = ConfigMetrics::new("1a2b3c4d5e6f7a8b");
        let output = metrics.encode();

        assert!(
            output.contains("gas_killer_config_fingerprint{fingerprint=\"1a2b3c4d5e6f7a8b\"} 1")
        );
    }

    #[test]
    fn a_process_publishes_exactly_one_fingerprint_series() {
        // Counting distinct fingerprints across the fleet is how a split is detected, so a
        // single process must never contribute more than one series.
        let metrics = ConfigMetrics::new("deadbeefdeadbeef");
        let series = metrics
            .encode()
            .lines()
            .filter(|line| line.starts_with("gas_killer_config_fingerprint{"))
            .count();

        assert_eq!(series, 1);
    }
}
