//! The operator HTTP surface: Kubernetes probes and the Prometheus scrape endpoint.
//!
//! Served on its own listener (`HEALTHZ_PORT`, 8081 by default) rather than on the ingress, so a
//! cluster can probe and scrape the router without any of it being reachable through the public
//! ingress. The chart's `livenessProbe` and `readinessProbe` both point here.
//!
//! [`healthz_handler`] is the exception: the ingress serves the same handler on its own port as
//! well, because the public ingress allowlists `/healthz` and the local scripts check it there.
//! One handler, two listeners, and one entry in the OpenAPI document naming both.

use crate::metrics::MetricsCollector;
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use commonware_runtime::Metrics as _;
use gas_killer_common::ConfigMetrics;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// State behind the probes and the metrics encoder.
#[derive(Clone)]
pub struct HealthState {
    /// Flipped once the engine, sequencer and submitter are spawned and the network is starting.
    pub ready: Arc<AtomicBool>,
    /// `tokio::Context` is `!Clone` in 2026.5.0; `encode()` works through a shared handle.
    pub context: Arc<commonware_runtime::tokio::Context>,
    pub metrics: Arc<MetricsCollector>,
    /// This process's configuration fingerprint, published identically by the router and every
    /// operator so a split fleet is one query across the deployment.
    pub config_metrics: Arc<ConfigMetrics>,
}

/// Liveness probe: `200` whenever the process is running.
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "Health",
    operation_id = "getHealthz",
    summary = "Liveness probe",
    description = "Answers `200` whenever the process is listening. It reports nothing about \
                   readiness or the operator network, so a cluster should probe `/readyz` to \
                   decide whether to send traffic.\n\nServed on both listeners: the operator port \
                   that Kubernetes probes, and the ingress port, which the public ingress \
                   allowlists.",
    servers(
        (url = "http://localhost:8081", description = "Operator port (`HEALTHZ_PORT`)"),
        (url = "http://localhost:8080", description = "Ingress port")
    ),
    responses(
        (status = 200, description = "The process is listening.")
    )
)]
pub async fn healthz_handler() -> StatusCode {
    StatusCode::OK
}

/// Readiness probe: `503` until the engine, sequencer and submitter are spawned and the network
/// is starting.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "Health",
    operation_id = "getReadyz",
    summary = "Readiness probe",
    description = "Answers `200` once the aggregation engine, sequencer and submitter are spawned \
                   and the p2p network is starting, and `503` before that. This is the probe that \
                   decides whether the pod should receive traffic; `/healthz` only reports that \
                   the process exists.",
    servers(
        (url = "http://localhost:8081", description = "Operator port (`HEALTHZ_PORT`)")
    ),
    responses(
        (status = 200, description = "The router is ready to accept work."),
        (status = 503, description = "The router is still starting up.")
    )
)]
pub async fn readyz_handler(State(state): State<HealthState>) -> StatusCode {
    if state.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Prometheus scrape endpoint: the commonware runtime's metrics, then the router's own, then
/// this process's configuration fingerprint.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "Health",
    operation_id = "getMetrics",
    summary = "Prometheus metrics",
    description = "Prometheus text exposition of the commonware runtime metrics, then the \
                   router's own, then this process's configuration fingerprint. Not JSON, and \
                   not versioned as part of the API: metric names may change with the code that \
                   emits them.",
    servers(
        (url = "http://localhost:8081", description = "Operator port (`HEALTHZ_PORT`)")
    ),
    responses(
        (
            status = 200,
            description = "The current metric values.",
            content_type = "text/plain; version=0.0.4; charset=utf-8",
            body = String
        )
    )
)]
pub async fn metrics_handler(State(state): State<HealthState>) -> impl IntoResponse {
    let mut output = state.context.encode();
    output.push_str(&state.metrics.encode());
    output.push_str(&state.config_metrics.encode());
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

/// Builds the operator listener's router.
///
/// Adding a route here is three edits, the same as for the ingress: annotate the handler with
/// `#[utoipa::path]`, add it to the `paths(...)` list in [`crate::openapi`], and update the
/// operator route table in that module's `the_documented_routes_are_exactly_the_route_table`
/// test. A route added only here compiles and serves, but ships undocumented.
pub fn build_operator_app() -> Router<HealthState> {
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/metrics", get(metrics_handler))
}
