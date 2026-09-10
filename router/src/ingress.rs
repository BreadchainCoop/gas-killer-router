use crate::error::{ApiError, ApiErrorBody, ApiErrorEnvelope, ApiJson, ApiQuery, ErrorCode};
use crate::metrics::{MetricsCollector, key_labels};
use crate::rate_limit::KeyRateLimiter;
use crate::rpc_health::RpcHealth;
use crate::sequencer::{QueuedTask, TaskQueueDepth, TaskSender};
use crate::store::{
    ApiKeyMetadata, AuthenticatedKey, CreatedApiKey, KeyVerdict, SqliteStore, SubmittedTask, Task,
    TaskStatus,
};
use alloy_primitives::{Address, U256};
use alloy_provider::Provider;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::{delete, get, post},
};
use gas_killer_common::ChainRole;
use gas_killer_common::ReadOnlyProvider;
use gas_killer_common::avs_contracts::ResolvedContracts;
use gas_killer_common::bindings::gaskillersdk::GasKillerSDK;
use gas_killer_common::config::CHAIN_DETECTION_ORDER;
use gas_killer_common::task_data::MAX_EVM_TX_CALLDATA_SIZE;
use gas_killer_common::{PayloadView, TaskBundle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// TTL for the chain reads backing the payload freshness check. Roughly half an L1 block, so a
/// burst of polls for the same task reuses one read while a stale payload is still caught within
/// a block.
const FRESHNESS_CACHE_TTL: Duration = Duration::from_secs(6);

/// Short-lived cache of the chain reads that back the `ready`-payload freshness check, keyed so
/// rapid polling of the same task reuses a read rather than issuing an RPC per poll. Shared across
/// [`IngressState`] clones.
#[derive(Default)]
pub struct FreshnessCache {
    block_numbers: RwLock<HashMap<ChainRole, (u64, Instant)>>,
    transition_counts: RwLock<HashMap<(ChainRole, Address), (u64, Instant)>>,
    chain_roles: RwLock<HashMap<Address, (ChainRole, Instant)>>,
}

impl FreshnessCache {
    fn cached_chain_role(&self, target: Address) -> Option<ChainRole> {
        let guard = self.chain_roles.read().ok()?;
        guard
            .get(&target)
            .and_then(|(role, at)| (at.elapsed() < FRESHNESS_CACHE_TTL).then_some(*role))
    }

    fn store_chain_role(&self, target: Address, role: ChainRole) {
        if let Ok(mut guard) = self.chain_roles.write() {
            guard.insert(target, (role, Instant::now()));
        }
    }

    fn cached_block_number(&self, role: ChainRole) -> Option<u64> {
        let guard = self.block_numbers.read().ok()?;
        guard
            .get(&role)
            .and_then(|(value, at)| (at.elapsed() < FRESHNESS_CACHE_TTL).then_some(*value))
    }

    fn store_block_number(&self, role: ChainRole, value: u64) {
        if let Ok(mut guard) = self.block_numbers.write() {
            guard.insert(role, (value, Instant::now()));
        }
    }

    fn cached_transition_count(&self, role: ChainRole, target: Address) -> Option<u64> {
        let guard = self.transition_counts.read().ok()?;
        guard
            .get(&(role, target))
            .and_then(|(value, at)| (at.elapsed() < FRESHNESS_CACHE_TTL).then_some(*value))
    }

    fn store_transition_count(&self, role: ChainRole, target: Address, value: u64) {
        if let Ok(mut guard) = self.transition_counts.write() {
            guard.insert((role, target), (value, Instant::now()));
        }
    }
}

/// AVS identity metadata served at `GET /avs-metadata`.
///
/// The EigenLayer indexer fetches the URL passed to `updateAVSMetadataURI`
/// and expects this exact JSON shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AvsMetadata {
    pub name: String,
    pub website: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub logo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub twitter: Option<String>,
    #[serde(rename = "operatorSets", skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub operator_sets: Option<Vec<AvsOperatorSetMetadata>>,
    /// Settlement-relevant contract addresses, resolved from the running deployment by a
    /// background task. Absent until it establishes something, so an integrator gets no answer
    /// rather than a wrong one: a wrong checker is what makes a target revert on every submission.
    /// Held as a shared slot, so cloning this metadata for a response reads whatever the resolver
    /// has published rather than snapshotting what was known when the router started, and a record
    /// a deploy job writes later shows up without a restart.
    #[serde(default, skip_serializing_if = "ResolvedContracts::is_unresolved")]
    #[schema(value_type = Option<gas_killer_common::avs_contracts::AvsContracts>, nullable = false)]
    pub contracts: ResolvedContracts,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AvsOperatorSetMetadata {
    pub name: String,
    pub id: String,
    pub description: String,
    pub software: Vec<AvsOperatorSetSoftware>,
    #[serde(rename = "slashingConditions", skip_serializing_if = "Vec::is_empty")]
    pub slashing_conditions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AvsOperatorSetSoftware {
    pub name: String,
    pub description: String,
    pub url: String,
}

#[derive(Clone)]
pub struct IngressState {
    pub sender: TaskSender,
    pub queue_depth: TaskQueueDepth,
    /// Maximum number of tasks allowed in the queue before the ingress starts returning 503.
    pub max_queue_depth: usize,
    /// Per-API-key request rate limiter guarding `POST /tasks`; see [`KeyRateLimiter`].
    pub rate_limiter: Arc<KeyRateLimiter>,
    pub metrics: Option<Arc<MetricsCollector>>,
    pub providers: Arc<HashMap<ChainRole, ReadOnlyProvider>>,
    /// Short-lived cache backing the `ready`-payload freshness check on `GET /tasks/{id}`.
    pub freshness: Arc<FreshnessCache>,
    /// Shared secret guarding the `/admin/keys` endpoints (`ADMIN_KEY`). `None` disables the
    /// admin API, so keys cannot be managed until it is set.
    pub admin_key: Option<String>,
    pub avs_metadata: AvsMetadata,
    /// Durable SQLite store shared with the orchestrator. `None` when persistence is not
    /// configured (e.g. in tests); every task endpoint and the admin API require it.
    pub store: Option<SqliteStore>,
    /// Per-chain RPC circuit breaker. Every chain read on the submission and freshness paths
    /// reports its outcome here, and a degraded chain sheds new submissions. `None` disables both,
    /// for harnesses with no providers to watch.
    pub rpc_health: Option<Arc<RpcHealth>>,
}

impl IngressState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sender: TaskSender,
        queue_depth: TaskQueueDepth,
        max_queue_depth: usize,
        rate_limiter: Arc<KeyRateLimiter>,
        metrics: Arc<MetricsCollector>,
        providers: HashMap<ChainRole, ReadOnlyProvider>,
        avs_metadata: AvsMetadata,
    ) -> Self {
        Self {
            sender,
            queue_depth,
            max_queue_depth,
            rate_limiter,
            metrics: Some(metrics),
            providers: Arc::new(providers),
            freshness: Arc::new(FreshnessCache::default()),
            admin_key: None,
            avs_metadata,
            store: None,
            rpc_health: None,
        }
    }

    /// Bare constructor for tests and local development: no metrics, providers, store, or admin
    /// key. Without a store the task endpoints answer `503 NotConfigured`, so a harness that
    /// drives them past authentication has to attach one with [`IngressState::with_store`].
    pub fn without_metrics(sender: TaskSender, queue_depth: TaskQueueDepth) -> Self {
        Self {
            sender,
            queue_depth,
            max_queue_depth: gas_killer_common::max_queue_depth(),
            rate_limiter: Arc::new(KeyRateLimiter::new(gas_killer_common::rate_limit_rpm())),
            metrics: None,
            providers: Arc::new(HashMap::new()),
            freshness: Arc::new(FreshnessCache::default()),
            admin_key: None,
            avs_metadata: AvsMetadata::default(),
            store: None,
            rpc_health: None,
        }
    }

    /// Attaches the durable SQLite store, returning the updated state for chained construction.
    pub fn with_store(mut self, store: SqliteStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Sets the shared secret guarding the admin API, returning the updated state for chained
    /// construction.
    pub fn with_admin_key(mut self, admin_key: Option<String>) -> Self {
        self.admin_key = admin_key;
        self
    }

    /// Attaches the RPC circuit breaker, returning the updated state for chained construction.
    pub fn with_rpc_health(mut self, rpc_health: Arc<RpcHealth>) -> Self {
        self.rpc_health = Some(rpc_health);
        self
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Extracts the token from an `Authorization: Bearer <token>` header, if present and valid.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Whether the request carries a Bearer token matching `expected`, compared in constant time
/// so a mismatch's position cannot be inferred from timing. Used for the `ADMIN_KEY` shared
/// secret, which is compared directly rather than looked up by hash.
fn check_bearer_auth(headers: &HeaderMap, expected: &str) -> bool {
    bearer_token(headers)
        .is_some_and(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()))
}

/// `key_id` recorded for a request that cannot be attributed to an issued key. Every audit log
/// line carries the field, so a search by `key_id` never silently omits unattributable traffic.
const UNATTRIBUTED_KEY_ID: &str = "unknown";

/// Milliseconds a request has spent in its handler, for the `duration_ms` field on its audit log
/// lines. Measured from handler entry, so it covers authentication, validation, and persistence
/// but not the framework's body read.
fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

/// Authenticates the caller against the durable store, returning the authenticated key (its id
/// and rate-limit ceiling). Used by every task endpoint (submission and status) to identify — and
/// scope work to — the calling key: a valid, unrevoked API key is required, and anything else is
/// a `401`.
///
/// Takes the store rather than reading it off the state, so the caller has to resolve it (see
/// [`require_store`]) before authenticating. Every task endpoint needs the store regardless — to
/// persist or to read — so there is no configuration in which one could authenticate a caller and
/// then find it has nowhere to work.
///
/// Every rejection is logged with a `key_id` so an operator can trace abusive traffic back to a
/// client: a key that was issued but has since been revoked or expired is named by its id
/// ([`KeyVerdict::Rejected`]), and anything else — no header, or a value matching no issued key —
/// records [`UNATTRIBUTED_KEY_ID`]. The presented key value is never logged.
async fn authenticate_caller(
    store: &SqliteStore,
    headers: &HeaderMap,
) -> Result<AuthenticatedKey, ApiError> {
    let Some(token) = bearer_token(headers) else {
        warn!(
            key_id = UNATTRIBUTED_KEY_ID,
            "Request rejected: missing or malformed Authorization header"
        );
        return Err(ApiError::unauthorized());
    };

    // Each verdict decides only what the rejection is attributed to; every one of them is the
    // same opaque `401` to the caller, so a probe cannot tell a revoked key from an invented one.
    let key_id = match store.verify_api_key(token).await {
        Ok(KeyVerdict::Valid(authed)) => return Ok(authed),
        Ok(KeyVerdict::Rejected { id }) => id,
        Ok(KeyVerdict::Unknown) => UNATTRIBUTED_KEY_ID.to_string(),
        Err(e) => {
            tracing::error!(error = %e, "api key verification failed");
            return Err(ApiError::internal("Internal error during authentication"));
        }
    };

    warn!(
        %key_id,
        "Request rejected: API key is unknown, revoked, or expired"
    );
    Err(ApiError::unauthorized())
}

/// Guards the `/admin/keys` endpoints with the `ADMIN_KEY` shared secret. Returns a 503 when
/// the admin API is not configured, so an operator who has not set `ADMIN_KEY` gets a clear
/// signal rather than a locked door with no key, and a 401 when the credential is wrong.
fn authorize_admin(state: &IngressState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(admin_key) = &state.admin_key else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::NotConfigured,
            "Admin API is not configured (set ADMIN_KEY)",
        ));
    };
    if check_bearer_auth(headers, admin_key) {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

/// Borrows the durable store, or returns a 503 when persistence is not configured. Every task
/// endpoint and the admin API need it, so this runs before any other work on those paths.
fn require_store(state: &IngressState) -> Result<&SqliteStore, ApiError> {
    state.store.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::NotConfigured,
            "Persistence is not configured",
        )
    })
}

/// Current unix time in seconds. Falls back to 0 if the system clock is before the epoch (never
/// in practice), which only makes the past-expiry check maximally permissive.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Onchain validation errors for incoming task requests.
#[derive(Debug)]
pub enum OnchainValidationError {
    ContractNotFound,
    TransitionIndexMismatch {
        provided: u64,
        current: u64,
    },
    BlockHeightInFuture {
        provided: u64,
        current: u64,
    },
    BlockHeightTooStale {
        provided: u64,
        current: u64,
        max_age: u64,
    },
    TargetNotDeployedAtBlock {
        provided: u64,
        current: u64,
    },
    RpcError(String),
}

impl fmt::Display for OnchainValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContractNotFound => write!(f, "no contract found at target_address on any chain"),
            Self::TransitionIndexMismatch { provided, current } => write!(
                f,
                "transition_index {provided} does not match current onchain state {current}"
            ),
            Self::BlockHeightInFuture { provided, current } => write!(
                f,
                "block_height {provided} is ahead of current chain height {current}"
            ),
            Self::BlockHeightTooStale {
                provided,
                current,
                max_age,
            } => write!(
                f,
                "block_height {provided} is older than the staleness window ({max_age} blocks) relative to current chain height {current}"
            ),
            Self::TargetNotDeployedAtBlock { provided, current } => write!(
                f,
                "target_address has no code at block_height {provided} (it is deployed as of the current chain height {current}), so an analysis anchored there would trace a call into an empty account"
            ),
            Self::RpcError(msg) => write!(f, "RPC error during onchain validation: {msg}"),
        }
    }
}

impl std::error::Error for OnchainValidationError {}

impl From<OnchainValidationError> for ApiError {
    fn from(e: OnchainValidationError) -> Self {
        let (status, code) = match e {
            OnchainValidationError::ContractNotFound => {
                (StatusCode::BAD_REQUEST, ErrorCode::ContractNotFound)
            }
            OnchainValidationError::TransitionIndexMismatch { .. } => {
                (StatusCode::BAD_REQUEST, ErrorCode::TransitionMismatch)
            }
            OnchainValidationError::BlockHeightInFuture { .. } => {
                (StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest)
            }
            OnchainValidationError::BlockHeightTooStale { .. } => {
                (StatusCode::BAD_REQUEST, ErrorCode::StaleBlock)
            }
            OnchainValidationError::TargetNotDeployedAtBlock { .. } => {
                (StatusCode::BAD_REQUEST, ErrorCode::TargetNotDeployed)
            }
            OnchainValidationError::RpcError(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::RpcUnavailable)
            }
        };
        // RPC failures are transient and their detail is internal; surface a generic message
        // to clients rather than leaking the upstream error string.
        let message = match code {
            ErrorCode::RpcUnavailable => "Service temporarily unavailable".to_string(),
            _ => e.to_string(),
        };
        ApiError::new(status, code, message)
    }
}

/// Reports one RPC call's outcome to the circuit breaker, passing the result through unchanged.
///
/// Every chain read on the submission path goes through here, so the breaker sees each call
/// individually rather than inferring a verdict from whichever error happened to surface. `None`
/// disables recording, for the test and store-only harnesses that have no breaker.
fn record_rpc<T, E>(
    health: Option<&RpcHealth>,
    chain: ChainRole,
    result: Result<T, E>,
) -> Result<T, E> {
    if let Some(health) = health {
        match &result {
            Ok(_) => health.record_success(chain),
            Err(_) => health.record_failure(chain),
        }
    }
    result
}

async fn detect_contract_chain<P: Provider + Clone>(
    providers: &HashMap<ChainRole, P>,
    health: Option<&RpcHealth>,
    address: Address,
) -> Result<ChainRole, OnchainValidationError> {
    let mut rpc_error: Option<String> = None;
    for &chain_id in &CHAIN_DETECTION_ORDER {
        if let Some(provider) = providers.get(&chain_id) {
            // A chain that answers "no code here" has answered: that is a healthy provider, not a
            // failure, so detection probing past it still clears any failure run on it.
            match record_rpc(health, chain_id, provider.get_code_at(address).await) {
                Ok(code) if !code.is_empty() => return Ok(chain_id),
                Ok(_) => {}
                Err(e) => {
                    warn!(chain = %chain_id, error = %e, "RPC error checking contract code");
                    rpc_error = Some(e.to_string());
                }
            }
        }
    }
    Err(match rpc_error {
        Some(e) => OnchainValidationError::RpcError(e),
        None => OnchainValidationError::ContractNotFound,
    })
}

async fn validate_onchain<P: Provider + Clone>(
    providers: &HashMap<ChainRole, P>,
    health: Option<&RpcHealth>,
    body: &GasKillerTaskRequestBody,
) -> Result<(), OnchainValidationError> {
    let chain_id = detect_contract_chain(providers, health, body.target_address).await?;

    // Detection only returns a chain it read from this map, so the provider is always present.
    let provider = providers
        .get(&chain_id)
        .ok_or_else(|| OnchainValidationError::RpcError("no provider for detected chain".into()))?;

    let current_block = record_rpc(health, chain_id, provider.get_block_number().await)
        .map_err(|e| OnchainValidationError::RpcError(e.to_string()))?;

    if body.block_height > current_block {
        return Err(OnchainValidationError::BlockHeightInFuture {
            provided: body.block_height,
            current: current_block,
        });
    }

    // Reject analyses anchored too far behind head. This is an off-chain admission bound (the
    // contract bounds the operator-set reference block, not this gas-analysis block_height): it
    // keeps requests within the speculative executor cache's window, rejects analyses old enough
    // to likely hit a transition_index mismatch, and leaves room inside the contract's staleness
    // window for the aggregation round and the rendered payload's own validity. The window is
    // configurable and can be turned off entirely; see
    // [`gas_killer_common::ingress_staleness_window`]. age == max_age stays valid, matching the
    // contract's `referenceBlockNumber + BLOCK_STALE_MEASURE >= block.number` convention.
    if let Some(max_age) = gas_killer_common::ingress_staleness_window().window()
        && current_block.saturating_sub(body.block_height) > max_age
    {
        return Err(OnchainValidationError::BlockHeightTooStale {
            provided: body.block_height,
            current: current_block,
            max_age,
        });
    }

    // Chain detection probes `latest`, which answers "is this a contract at all" but not "was it
    // a contract at the height this analysis is anchored to". A `block_height` that predates the
    // target's deployment traces a call into an empty account: the trace succeeds trivially, and
    // the round returns a signed payload carrying an empty diff, so the request is refused here
    // instead. A client whose RPC serves a lagging head sees this rather than a no-op payload.
    //
    // A provider that cannot serve state at `block_height` surfaces as a transient RPC failure
    // rather than a rejection. That imposes no archive requirement the service does not already
    // have: the analysis replays the call at that height, so a provider that cannot answer this
    // probe cannot serve the task either.
    let code_at_block = record_rpc(
        health,
        chain_id,
        provider
            .get_code_at(body.target_address)
            .number(body.block_height)
            .await,
    )
    .map_err(|e| OnchainValidationError::RpcError(e.to_string()))?;
    if code_at_block.is_empty() {
        return Err(OnchainValidationError::TargetNotDeployedAtBlock {
            provided: body.block_height,
            current: current_block,
        });
    }

    // Only validate the transition index if explicitly provided.
    // None means "auto" — the server resolves the index at dequeue time.
    if let Some(provided) = body.transition_index {
        let contract = GasKillerSDK::new(body.target_address, provider.clone());
        let count = record_rpc(
            health,
            chain_id,
            contract.stateTransitionCount().call().await,
        )
        .map_err(|e| OnchainValidationError::RpcError(e.to_string()))?;
        let current_count: u64 = count.try_into().map_err(|_| {
            OnchainValidationError::RpcError("stateTransitionCount overflow".into())
        })?;

        if provided != current_count {
            return Err(OnchainValidationError::TransitionIndexMismatch {
                provided,
                current: current_count,
            });
        }
    }

    Ok(())
}

/// Validation errors for incoming task requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    ZeroTargetAddress,
    ZeroFromAddress,
    EmptyCallData,
    CallDataTooShort { len: usize },
    CallDataTooLarge { len: usize, max: usize },
    ZeroBlockHeight,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTargetAddress => write!(f, "target_address is zero"),
            Self::ZeroFromAddress => write!(f, "from_address is zero"),
            Self::EmptyCallData => write!(f, "call_data is empty"),
            Self::CallDataTooShort { len } => {
                write!(f, "call_data too short ({len} bytes, minimum 4)")
            }
            Self::CallDataTooLarge { len, max } => {
                write!(f, "call_data too large ({len} bytes, maximum {max})")
            }
            Self::ZeroBlockHeight => write!(f, "block_height is zero"),
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<ValidationError> for ApiError {
    fn from(e: ValidationError) -> Self {
        let code = match e {
            ValidationError::ZeroTargetAddress | ValidationError::ZeroFromAddress => {
                ErrorCode::InvalidAddress
            }
            ValidationError::CallDataTooLarge { .. } => ErrorCode::CalldataTooLarge,
            ValidationError::EmptyCallData
            | ValidationError::CallDataTooShort { .. }
            | ValidationError::ZeroBlockHeight => ErrorCode::InvalidRequest,
        };
        ApiError::new(StatusCode::BAD_REQUEST, code, e.to_string())
    }
}

/// Deserializes `transition_index` from JSON.
///
/// Accepted values:
/// - `null` or missing field → `None` (auto: server assigns the next slot)
/// - `"auto"` → `None`
/// - non-negative integer → `Some(n)` (explicit fixed index)
///
/// Any other string or non-integer type is rejected with a descriptive error.
fn deserialize_transition_index<'de, D>(d: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Unexpected, Visitor};

    struct TransitionIndexVisitor;

    impl<'de> Visitor<'de> for TransitionIndexVisitor {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, r#"a non-negative integer, "auto", or null"#)
        }

        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2: serde::Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            d.deserialize_any(self)
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            u64::try_from(v)
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Signed(v), &self))
        }

        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            if v == "auto" {
                Ok(None)
            } else {
                Err(E::invalid_value(Unexpected::Str(v), &self))
            }
        }
    }

    d.deserialize_option(TransitionIndexVisitor)
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct GasKillerTaskRequestBody {
    /// The contract the computation is executed against.
    #[schema(value_type = gas_killer_common::openapi::Address)]
    pub target_address: Address,
    #[schema(value_type = crate::openapi::CallData)]
    pub call_data: Vec<u8>,
    /// `None`, JSON `null`, `"auto"`, or a missing field all mean "auto":
    /// the server resolves the next available slot at dequeue time,
    /// enabling safe parallel submissions.
    #[serde(default, deserialize_with = "deserialize_transition_index")]
    #[schema(value_type = crate::openapi::TransitionIndex)]
    pub transition_index: Option<u64>,
    /// The address the simulated call is sent from.
    #[schema(value_type = gas_killer_common::openapi::Address)]
    pub from_address: Address,
    /// Wei value to attach to the call.
    #[schema(value_type = gas_killer_common::openapi::HexUint256)]
    pub value: U256,
    /// The block height the computation is pinned to.
    #[schema(minimum = 1, example = 1)]
    pub block_height: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct GasKillerTaskRequest {
    pub body: GasKillerTaskRequestBody,
}

impl GasKillerTaskRequest {
    /// Validates all request fields, returning the first error found.
    pub fn validate(&self) -> Result<(), ValidationError> {
        let body = &self.body;

        if body.target_address.is_zero() {
            return Err(ValidationError::ZeroTargetAddress);
        }
        if body.from_address.is_zero() {
            return Err(ValidationError::ZeroFromAddress);
        }
        if body.call_data.is_empty() {
            return Err(ValidationError::EmptyCallData);
        }
        // minimum 4 bytes for function selector
        if body.call_data.len() < 4 {
            return Err(ValidationError::CallDataTooShort {
                len: body.call_data.len(),
            });
        }
        // maximum 128 KB for call data
        if body.call_data.len() > MAX_EVM_TX_CALLDATA_SIZE {
            return Err(ValidationError::CallDataTooLarge {
                len: body.call_data.len(),
                max: MAX_EVM_TX_CALLDATA_SIZE,
            });
        }
        if body.block_height == 0 {
            return Err(ValidationError::ZeroBlockHeight);
        }

        Ok(())
    }
}

/// Body returned by `POST /tasks` when a task is accepted: the id the client polls for status,
/// and the task's current state.
///
/// `deduplicated` is `true` when the submission collapsed onto an existing in-flight or `ready`
/// task rather than creating a new one. A retry keyed on `(key_id, target_address,
/// transition_index)` is idempotent, returning the original task id with a `200 OK`. A fresh
/// submission carries `false` and a `202 Accepted`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TaskAcceptedResponse {
    /// The id to poll for status via `GET /tasks/{task_id}`.
    #[schema(example = "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11")]
    pub task_id: String,
    /// The task's state at the moment it was accepted.
    pub status: TaskStatus,
    /// Whether the submission collapsed onto a task already in flight rather than creating a new
    /// one. `true` accompanies a `200`, `false` a `202`.
    pub deduplicated: bool,
}

/// `Retry-After` estimate (seconds) returned alongside a `503 QUEUE_FULL`. The router drains
/// the queue one task at a time, so a slot typically frees once the in-flight round settles;
/// sizing the hint to roughly one round nudges a shed client to retry about when capacity is
/// likely to open rather than hot-looping against a full queue.
const QUEUE_FULL_RETRY_AFTER_SECS: u64 = 60;

/// A reserved slot in the ingress queue.
///
/// [`QueueSlot::reserve`] atomically increments the shared depth counter only while it is below
/// the cap, so the capacity check and the reservation are a single operation with no
/// check-then-increment race. Dropping the guard releases the slot (decrement + gauge refresh)
/// unless it has been [`committed`](QueueSlot::commit) to an enqueued task, so a submission that
/// is rejected or fails after reserving never permanently consumes capacity — regardless of which
/// early-return path it takes.
struct QueueSlot<'a> {
    state: &'a IngressState,
    committed: bool,
}

impl<'a> QueueSlot<'a> {
    /// Atomically reserves a slot when the queue is below its cap, returning the guard and the
    /// resulting depth. Returns `Err(current_depth)` when already at capacity, taking no slot.
    fn reserve(state: &'a IngressState) -> Result<(Self, usize), usize> {
        let max = state.max_queue_depth;
        match state
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < max).then_some(n + 1)
            }) {
            Ok(prev) => {
                let depth = prev + 1;
                if let Some(m) = &state.metrics {
                    m.task_queue_depth.set(depth as i64);
                }
                Ok((
                    Self {
                        state,
                        committed: false,
                    },
                    depth,
                ))
            }
            Err(current) => Err(current),
        }
    }

    /// Transfers the reservation to an enqueued task so it is not released on drop; the sequencer
    /// decrements the depth when it dequeues the task.
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for QueueSlot<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let depth = self
            .state
            .queue_depth
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        if let Some(m) = &self.state.metrics {
            m.task_queue_depth.set(depth as i64);
        }
    }
}

/// Handler for `POST /tasks`.
///
/// Validates the request, persists it as a `queued` task before responding — so a restart can
/// recover work already acknowledged to the client — then enqueues it for aggregation and returns
/// the task id for status polling. Persistence requires a configured store and an authenticated
/// key that owns the task, both resolved up front: neither validation nor its onchain RPC
/// round-trips are worth spending on a request that cannot be persisted or attributed. Beyond
/// that, validation and load-shedding stay synchronous and run before any task row is written, so
/// a malformed or at-capacity request never persists.
///
/// Every outcome — accepted, deduplicated, or rejected — logs the calling `key_id`, the request's
/// `target_address` and `transition_index`, and how long it took (`duration_ms`), so an operator
/// can audit a client's traffic without the key value ever appearing in a log.
#[utoipa::path(
    post,
    path = "/tasks",
    tag = "Tasks",
    operation_id = "submitTask",
    summary = "Submit a compute task",
    description = "Queues a task for the operator network and returns a `task_id` to poll. The \
                   request is authenticated, load-shed against the queue capacity, then validated \
                   (field-level, then on-chain) before it is accepted. A `202` means the task was \
                   queued: poll `GET /tasks/{task_id}` until it is `ready`, then sign and submit \
                   the returned payload yourself.\n\nA repeat of a submission that is still \
                   in flight collapses onto the original rather than creating a second task, and \
                   answers `200` with `deduplicated: true`. Deduplication is keyed on the calling \
                   key, the target, and the transition index, so it only applies to a submission \
                   that named an explicit `transition_index`.\n\nSubmission is rate limited per \
                   API key (60 requests per minute by default). Over the limit returns `429` with \
                   a `Retry-After` header.",
    security(("ApiKeyAuth" = [])),
    request_body = GasKillerTaskRequest,
    responses(
        (
            status = 202,
            description = "Task accepted and queued. Poll `GET /tasks/{task_id}` for status.",
            body = TaskAcceptedResponse,
            example = json!({
                "task_id": "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11",
                "status": "queued",
                "deduplicated": false
            })
        ),
        (
            status = 200,
            description = "The submission matched a task already in flight for this key, target \
                           and transition index, so it returned the original task id rather than \
                           queueing a second one.",
            body = TaskAcceptedResponse,
            example = json!({
                "task_id": "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11",
                "status": "queued",
                "deduplicated": true
            })
        ),
        (
            status = 400,
            description = "The request failed validation. `code` is one of `INVALID_REQUEST`, \
                           `INVALID_ADDRESS`, `STALE_BLOCK`, `TRANSITION_MISMATCH`, \
                           `CALLDATA_TOO_LARGE`, `CONTRACT_NOT_FOUND`, or `TARGET_NOT_DEPLOYED`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INVALID_ADDRESS", "message": "target_address is zero" } })
        ),
        (
            status = 401,
            description = "Missing or invalid API key.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 422,
            description = "The body is well-formed JSON but does not match the expected schema, \
                           for example a field of the wrong type. Returned with code \
                           `INVALID_REQUEST`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INVALID_REQUEST", "message": "Failed to deserialize the JSON body into the target type" } })
        ),
        (
            status = 429,
            description = "The API key exceeded its request rate limit. Wait for the number of \
                           seconds in the `Retry-After` header, then retry.",
            body = ApiErrorEnvelope,
            headers(("Retry-After" = u64, description = "Seconds to wait before retrying.")),
            example = json!({ "error": { "code": "RATE_LIMITED", "message": "Rate limit exceeded; retry after the indicated delay" } })
        ),
        (
            status = 503,
            description = "The service is at capacity (`QUEUE_FULL`), an upstream RPC endpoint \
                           used for validation is unreachable (`RPC_UNAVAILABLE`), or task \
                           persistence is not configured (`NOT_CONFIGURED`). The first two are \
                           transient; a `QUEUE_FULL` response carries a `Retry-After` estimate.",
            body = ApiErrorEnvelope,
            headers((
                "Retry-After" = u64,
                description = "Seconds to wait before retrying, on a `QUEUE_FULL` response."
            )),
            example = json!({ "error": { "code": "QUEUE_FULL", "message": "Service at capacity, please try again in a few minutes" } })
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn submit_task_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
    ApiJson(request): ApiJson<GasKillerTaskRequest>,
) -> Result<(StatusCode, Json<TaskAcceptedResponse>), ApiError> {
    let started = Instant::now();
    let store = require_store(&state)?;
    let key = authenticate_caller(store, &headers).await?;

    // Enforce the caller's per-key rate limit before reserving a queue slot, so an abusive key is
    // turned away before it can occupy capacity or trigger validation RPCs.
    if let Err(retry_after_secs) = state
        .rate_limiter
        .check(&key.id, key.rpm_limit.and_then(NonZeroU32::new))
    {
        warn!(
            key_id = %key.id,
            target_address = %request.body.target_address,
            transition_index = ?request.body.transition_index,
            retry_after_secs,
            duration_ms = elapsed_ms(started),
            "Task rejected: per-key rate limit exceeded"
        );
        if let Some(m) = &state.metrics {
            m.ingress_rate_limited
                .get_or_create(&key_labels(&key.id))
                .inc();
        }
        return Err(ApiError::rate_limited(retry_after_secs));
    }

    // Shed before spending anything on a request that cannot complete: with the chain every round
    // depends on unreachable, the task would be accepted, queued, and then fail on its own. This
    // sits after the rate limit so an abusive key still sees its own 429, and before the queue
    // reservation so a refused request never occupies capacity.
    if let Some(chain) = state.rpc_health.as_ref().and_then(|h| h.blocking_chain()) {
        warn!(
            key_id = %key.id,
            chain = %chain.name(),
            target_address = %request.body.target_address,
            duration_ms = elapsed_ms(started),
            "Task rejected: chain RPC unavailable"
        );
        if let Some(m) = &state.metrics {
            m.ingress_rpc_unavailable
                .get_or_create(&key_labels(&key.id))
                .inc();
        }
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::RpcUnavailable,
            "Service temporarily unavailable: upstream RPC is not responding",
        ));
    }

    // Load-shed before any validation work by atomically reserving a queue slot. Onchain
    // validation costs multiple RPC round-trips, so rejecting at-capacity requests up front keeps
    // an overloaded service from amplifying its own load; a request that would have failed
    // validation gets a 503 instead of a 400 while the queue is full. The reservation is released
    // (see `QueueSlot`) on every early return below, so only a task that is actually enqueued
    // consumes lasting capacity.
    let slot = match QueueSlot::reserve(&state) {
        Ok((slot, _depth)) => slot,
        Err(current_depth) => {
            warn!(
                key_id = %key.id,
                target_address = %request.body.target_address,
                transition_index = ?request.body.transition_index,
                queue_depth = current_depth,
                max_queue_depth = state.max_queue_depth,
                duration_ms = elapsed_ms(started),
                "Task rejected: queue at capacity"
            );
            if let Some(m) = &state.metrics {
                m.ingress_at_capacity
                    .get_or_create(&key_labels(&key.id))
                    .inc();
            }
            return Err(ApiError::queue_full(
                "Service at capacity, please retry shortly",
                QUEUE_FULL_RETRY_AFTER_SECS,
            ));
        }
    };

    if let Err(e) = request.validate() {
        warn!(
            key_id = %key.id,
            target_address = %request.body.target_address,
            from_address = %request.body.from_address,
            transition_index = ?request.body.transition_index,
            block_height = request.body.block_height,
            error = %e,
            duration_ms = elapsed_ms(started),
            "Task rejected"
        );
        if let Some(m) = &state.metrics {
            m.ingress_rejected.get_or_create(&key_labels(&key.id)).inc();
        }
        return Err(e.into());
    }

    if !state.providers.is_empty()
        && let Err(e) = validate_onchain(
            &*state.providers,
            state.rpc_health.as_deref(),
            &request.body,
        )
        .await
    {
        warn!(
            key_id = %key.id,
            target_address = %request.body.target_address,
            from_address = %request.body.from_address,
            block_height = request.body.block_height,
            transition_index = ?request.body.transition_index,
            error = %e,
            duration_ms = elapsed_ms(started),
            "Task rejected (onchain)"
        );
        if let Some(m) = &state.metrics {
            m.ingress_rejected.get_or_create(&key_labels(&key.id)).inc();
        }
        return Err(e.into());
    }

    let SubmittedTask { task, deduplicated } = store
        .create_task_deduplicated(&key.id, &request.body)
        .await
        .map_err(|e| {
            tracing::error!(
                key_id = %key.id,
                target_address = %request.body.target_address,
                transition_index = ?request.body.transition_index,
                error = %e,
                duration_ms = elapsed_ms(started),
                "failed to persist task"
            );
            ApiError::internal("Internal error: failed to persist task")
        })?;

    // A retry that collapsed onto an existing task must not be enqueued a second time — the
    // original submission already owns a queue slot (or has settled). Release this request's
    // reserved slot (the guard drops uncommitted) and hand back the existing task with `200 OK`,
    // so the client polls the same id idempotently.
    if deduplicated {
        info!(
            key_id = %key.id,
            task_id = %task.id,
            status = ?task.status,
            target_address = %request.body.target_address,
            transition_index = ?request.body.transition_index,
            duration_ms = elapsed_ms(started),
            "Task deduplicated onto existing submission"
        );
        if let Some(m) = &state.metrics {
            m.ingress_deduplicated
                .get_or_create(&key_labels(&key.id))
                .inc();
        }
        return Ok((
            StatusCode::OK,
            Json(TaskAcceptedResponse {
                task_id: task.id,
                status: task.status,
                deduplicated: true,
            }),
        ));
    }

    info!(
        key_id = %key.id,
        task_id = %task.id,
        target_address = %request.body.target_address,
        from_address = %request.body.from_address,
        transition_index = ?request.body.transition_index,
        block_height = request.body.block_height,
        call_data_len = request.body.call_data.len(),
        duration_ms = elapsed_ms(started),
        "Task accepted"
    );
    let queued = QueuedTask {
        task_id: task.id.clone(),
        request,
    };
    if state.sender.send(queued).is_err() {
        tracing::error!(
            key_id = %key.id,
            task_id = %task.id,
            duration_ms = elapsed_ms(started),
            "task channel closed, dropping request"
        );
        return Err(ApiError::internal("Internal error: task queue unavailable"));
    }
    // The enqueued task now owns the reserved slot; the sequencer decrements the depth counter
    // when it dequeues. Releasing here would double-count against a task that is genuinely queued.
    slot.commit();
    if let Some(m) = &state.metrics {
        m.ingress_accepted.get_or_create(&key_labels(&key.id)).inc();
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(TaskAcceptedResponse {
            task_id: task.id,
            status: task.status,
            deduplicated: false,
        }),
    ))
}

/// Full task state returned by the status endpoints (`GET /tasks/{id}` and `GET /tasks`).
///
/// `created_at`/`updated_at` are unix timestamps in seconds, matching the timestamp convention
/// used elsewhere in the API (e.g. the admin key listing). `payload` is populated once the task
/// reaches `ready`; `error` once it is `failed` or `expired`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TaskView {
    #[schema(example = "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11")]
    pub task_id: String,
    pub status: TaskStatus,
    /// Unix timestamp (seconds) when the task was created.
    pub created_at: i64,
    /// Unix timestamp (seconds) of the last state change.
    pub updated_at: i64,
    /// Failure reason; present once the task is `failed` or `expired`.
    pub error: Option<String>,
    /// The ready-to-sign transaction; present once the task is `ready`.
    pub payload: Option<PayloadView>,
}

impl From<Task> for TaskView {
    fn from(task: Task) -> Self {
        // Lenient parse: the stored payload is JSON the server itself wrote, so it always parses;
        // a failure yields `None` (logged) rather than failing the whole response. The list
        // endpoint relies on this to surface the stored payload without a freshness check, while
        // `GET /tasks/{id}` overwrites `payload` with the stale-checked result.
        let payload = task.payload.as_deref().and_then(|raw| {
            serde_json::from_str::<PayloadView>(raw)
                .inspect_err(|e| {
                    tracing::warn!(
                        key_id = %task.key_id,
                        task_id = %task.id,
                        error = %e,
                        "stored payload failed to parse"
                    );
                })
                .ok()
        });
        Self {
            task_id: task.id,
            status: task.status,
            created_at: task.created_at,
            updated_at: task.updated_at,
            error: task.error,
            payload,
        }
    }
}

/// Page size used by `GET /tasks` when the caller does not specify `limit`.
const DEFAULT_TASK_PAGE_SIZE: i64 = 50;
/// Largest page size `GET /tasks` will serve; larger `limit` values are clamped down to it.
const MAX_TASK_PAGE_SIZE: i64 = 200;

/// Resolves the effective page size from the caller's optional `limit`: defaulted to
/// [`DEFAULT_TASK_PAGE_SIZE`] and clamped to `[1, MAX_TASK_PAGE_SIZE]`, so a request can never
/// ask for a zero, negative, or unbounded page.
fn clamp_page_limit(limit: Option<i64>) -> i64 {
    limit
        .unwrap_or(DEFAULT_TASK_PAGE_SIZE)
        .clamp(1, MAX_TASK_PAGE_SIZE)
}

/// Resolves the effective offset from the caller's optional `offset`: defaulted to 0 with
/// negatives floored to 0. SQLite treats a negative OFFSET as 0 anyway; normalizing here keeps
/// the bound explicit and independent of the backing store.
fn clamp_offset(offset: Option<i64>) -> i64 {
    offset.unwrap_or(0).max(0)
}

/// Query parameters for `GET /tasks`: an optional status filter and pagination bounds.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListTasksQuery {
    /// Return only tasks in this lifecycle state.
    #[serde(default)]
    status: Option<TaskStatus>,
    /// Maximum number of tasks to return. Defaults to 50 and is clamped to the range 1 to 200.
    #[serde(default)]
    #[param(minimum = 1, maximum = 200, default = 50)]
    limit: Option<i64>,
    /// Number of tasks to skip, for pagination. Negative values are treated as 0.
    #[serde(default)]
    #[param(minimum = 0, default = 0)]
    offset: Option<i64>,
}

/// Validates a task's stored `ready` payload against current chain state, returning the payload to
/// serve, `None` when the task has no submittable payload, or a typed re-request error when the
/// payload has gone stale.
///
/// The check is the freshness authority for `GET /tasks/{id}`: a caller must fetch here (not the
/// list endpoint) before submitting. It is ordered cheapest-first and short-circuits so a poll
/// costs at most one `eth_getCode` (chain detection), one `eth_blockNumber`, and one
/// `stateTransitionCount`, all served from a short-TTL cache under rapid polling. Once a payload is
/// found stale the task is recorded `expired`, so every later poll returns immediately without a
/// chain read.
async fn render_or_reject_payload<P: Provider + Clone>(
    providers: &HashMap<ChainRole, P>,
    freshness: &FreshnessCache,
    health: Option<&RpcHealth>,
    store: &SqliteStore,
    task: &Task,
) -> Result<Option<PayloadView>, ApiError> {
    let payload = match task.payload.as_deref() {
        Some(raw) => serde_json::from_str::<PayloadView>(raw).map_err(|e| {
            tracing::error!(
                key_id = %task.key_id,
                task_id = %task.id,
                error = %e,
                "stored payload failed to parse"
            );
            ApiError::internal("Internal error: stored payload is malformed")
        })?,
        // No payload recorded yet (queued/processing/failed).
        None => return Ok(None),
    };

    // A task recorded `expired` had its payload found stale on an earlier poll; return the
    // re-request error without touching the chain again.
    if task.status == TaskStatus::Expired {
        return Err(ApiError::payload_expired(
            task.error
                .clone()
                .unwrap_or_else(|| "payload is no longer valid; re-request".to_string()),
        ));
    }

    // Only a ready task carries a submittable payload.
    if task.status != TaskStatus::Ready {
        return Ok(None);
    }

    // Store-only harnesses (no providers) cannot reach a chain; serve the stored payload as-is.
    if providers.is_empty() {
        return Ok(Some(payload));
    }

    // The bundle carries the chain / target / transition / validity data the check needs.
    let bundle: TaskBundle = match task.bundle.as_deref() {
        Some(raw) => serde_json::from_str(raw).map_err(|e| {
            tracing::error!(
                key_id = %task.key_id,
                task_id = %task.id,
                error = %e,
                "stored bundle failed to parse"
            );
            ApiError::internal("Internal error: stored bundle is malformed")
        })?,
        // Ready without a bundle: nothing to validate against, so serve as-is.
        None => return Ok(Some(payload)),
    };

    // Block-window check first (cheapest), short-circuiting before the contract call. It is
    // measured on the operator-set chain (L1): the certificate's reference block — from which
    // `valid_until_block` is derived — is an L1 block, since operator state lives on L1, even when
    // the target executes on L2. Comparing against the target chain's height would falsely expire
    // every L2 payload. If no L1 provider is configured, skip the window check.
    if let Some(l1_provider) = providers.get(&ChainRole::L1) {
        let current_block = match freshness.cached_block_number(ChainRole::L1) {
            Some(block) => block,
            None => {
                let block = record_rpc(health, ChainRole::L1, l1_provider.get_block_number().await)
                    .map_err(|e| ApiError::from(OnchainValidationError::RpcError(e.to_string())))?;
                freshness.store_block_number(ChainRole::L1, block);
                block
            }
        };
        if current_block > bundle.valid_until_block {
            let reason = format!(
                "payload valid_until_block {} passed (current block {}); re-request",
                bundle.valid_until_block, current_block
            );
            let _ = store.mark_task_expired(&task.id, &reason).await;
            return Err(ApiError::payload_expired(reason));
        }
    }

    // Consumed-bundle check on the target chain: `stateTransitionCount` is target-contract state,
    // so it is read from the chain the target is deployed on (L1 or L2). The resolved chain is
    // cached per target — a contract's chain does not change — so rapid polling skips the
    // `eth_getCode` detection probe.
    let role = match freshness.cached_chain_role(bundle.target_address) {
        Some(role) => role,
        None => {
            let role = detect_contract_chain(providers, health, bundle.target_address).await?;
            freshness.store_chain_role(bundle.target_address, role);
            role
        }
    };
    let provider = providers
        .get(&role)
        .ok_or_else(|| ApiError::internal("no provider for detected chain"))?;
    let current_count = match freshness.cached_transition_count(role, bundle.target_address) {
        Some(count) => count,
        None => {
            let contract = GasKillerSDK::new(bundle.target_address, provider.clone());
            let count = record_rpc(health, role, contract.stateTransitionCount().call().await)
                .map_err(|e| ApiError::from(OnchainValidationError::RpcError(e.to_string())))?;
            let count: u64 = count.try_into().map_err(|_| {
                ApiError::from(OnchainValidationError::RpcError(
                    "stateTransitionCount overflow".into(),
                ))
            })?;
            freshness.store_transition_count(role, bundle.target_address, count);
            count
        }
    };
    if current_count != bundle.transition_index {
        let reason = format!(
            "bundle consumed: on-chain transition index is {current_count}, payload targets {}; re-request",
            bundle.transition_index
        );
        let _ = store.mark_task_expired(&task.id, &reason).await;
        return Err(ApiError::payload_expired(reason));
    }

    Ok(Some(payload))
}

/// Handler for `GET /tasks/{task_id}` — returns the full state of a single task.
///
/// A task is visible only to the API key that created it: an unknown id yields `404`, and a task
/// owned by a different key yields `403` (kept distinct from `404` so a caller can tell "no such
/// task" from "not yours"). For a `ready` task this is the authoritative freshness check: a stale
/// payload yields a `409 PAYLOAD_EXPIRED` re-request error rather than calldata that would revert.
#[utoipa::path(
    get,
    path = "/tasks/{task_id}",
    tag = "Tasks",
    operation_id = "getTask",
    summary = "Get task status",
    description = "Returns the full state of a single task. Poll this endpoint until `status` is \
                   `ready`, at which point `payload` holds a ready-to-sign transaction request. \
                   This is the authoritative freshness check: for a `ready` task the router \
                   re-validates the payload against current chain state, so a stale payload \
                   returns `409 PAYLOAD_EXPIRED` (re-request the task) rather than calldata that \
                   would revert on submission. A task is visible only to the API key that created \
                   it.",
    security(("ApiKeyAuth" = [])),
    params(
        ("task_id" = String, Path, description = "The task id returned by `POST /tasks`.")
    ),
    responses(
        (
            status = 200,
            description = "Current task state.",
            body = TaskView,
            example = json!({
                "task_id": "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11",
                "status": "ready",
                "created_at": 1753180800,
                "updated_at": 1753180812,
                "error": null,
                "payload": {
                    "to": "0x0000000000000000000000000000000000000001",
                    "data": "0x93de4531000000000000000000000000000000000000000000000000000000000000002a",
                    "value": "0x0",
                    "chain_id": 11155111,
                    "estimated_gas": 234000,
                    "valid_until_block": 22345678
                }
            })
        ),
        (
            status = 401,
            description = "Missing or invalid API key.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 403,
            description = "The credentials are valid but the task belongs to a different API key. \
                           Kept distinct from `404` so a caller can tell \"not yours\" from \
                           \"no such task\".",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "FORBIDDEN", "message": "Task belongs to a different API key" } })
        ),
        (
            status = 404,
            description = "No task with this id exists.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "NOT_FOUND", "message": "Task not found" } })
        ),
        (
            status = 409,
            description = "The task's `ready` payload is no longer submittable: its \
                           `valid_until_block` has passed, or the on-chain state-transition index \
                           has advanced and the round was already consumed. Submit a fresh task \
                           and poll again.",
            body = ApiErrorEnvelope,
            example = json!({
                "error": {
                    "code": "PAYLOAD_EXPIRED",
                    "message": "payload valid_until_block 22345678 passed (current block \
                                22345700); re-request"
                }
            })
        ),
        (
            status = 503,
            description = "An upstream RPC endpoint used for the freshness check is unreachable \
                           (`RPC_UNAVAILABLE`), or task persistence is not configured \
                           (`NOT_CONFIGURED`).",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "RPC_UNAVAILABLE", "message": "Service temporarily unavailable" } })
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn get_task_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<TaskView>, ApiError> {
    let started = Instant::now();
    let store = require_store(&state)?;
    let key_id = authenticate_caller(store, &headers).await?.id;

    let task = store
        .get_task(&task_id)
        .await
        .map_err(|e| {
            tracing::error!(key_id = %key_id, task_id = %task_id, error = %e, "failed to fetch task");
            ApiError::internal("Internal error: failed to fetch task")
        })?
        .ok_or_else(|| ApiError::not_found("Task not found"))?;

    // A key reaching for a task it does not own is worth recording: the only way to hit this is
    // to present a task id belonging to another client.
    if task.key_id != key_id {
        warn!(
            key_id = %key_id,
            task_id = %task_id,
            duration_ms = elapsed_ms(started),
            "Task request rejected: task belongs to a different API key"
        );
        return Err(ApiError::forbidden("Task belongs to a different API key"));
    }

    let payload = render_or_reject_payload(
        &*state.providers,
        &state.freshness,
        state.rpc_health.as_deref(),
        store,
        &task,
    )
    .await?;
    let mut view = TaskView::from(task);
    view.payload = payload;
    Ok(Json(view))
}

/// Handler for `GET /tasks` — lists the calling key's tasks, newest first.
///
/// Scoped to the caller's API key; supports an optional `?status=` filter and `?limit=`/`?offset=`
/// pagination. `limit` defaults to [`DEFAULT_TASK_PAGE_SIZE`] and is clamped to
/// `[1, MAX_TASK_PAGE_SIZE]`; a negative `offset` is treated as `0`.
#[utoipa::path(
    get,
    path = "/tasks",
    tag = "Tasks",
    operation_id = "listTasks",
    summary = "List your tasks",
    description = "Lists the tasks created by the calling API key, newest first. Supports an \
                   optional `status` filter and `limit`/`offset` pagination. Payloads are \
                   returned as last stored; unlike `GET /tasks/{task_id}`, this endpoint does not \
                   re-check payload freshness, so always fetch a single task before submitting \
                   its payload.",
    security(("ApiKeyAuth" = [])),
    params(ListTasksQuery),
    responses(
        (
            status = 200,
            description = "The calling key's tasks, newest first.",
            body = Vec<TaskView>,
            example = json!([
                {
                    "task_id": "9b2f7d41-6c3a-4e58-9d0b-1a4f8e2c7b33",
                    "status": "queued",
                    "created_at": 1753180845,
                    "updated_at": 1753180845,
                    "error": null,
                    "payload": null
                },
                {
                    "task_id": "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11",
                    "status": "ready",
                    "created_at": 1753180800,
                    "updated_at": 1753180812,
                    "error": null,
                    "payload": {
                        "to": "0x0000000000000000000000000000000000000001",
                        "data": "0x93de4531000000000000000000000000000000000000000000000000000000000000002a",
                        "value": "0x0",
                        "chain_id": 11155111,
                        "estimated_gas": 234000,
                        "valid_until_block": 22345678
                    }
                }
            ])
        ),
        (
            status = 400,
            description = "A query parameter is invalid, for example an unknown `status` value or \
                           a non-integer `limit`. Returned with code `INVALID_REQUEST`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INVALID_REQUEST", "message": "Failed to deserialize query string" } })
        ),
        (
            status = 401,
            description = "Missing or invalid API key.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 503,
            description = "Task persistence is not configured, so the endpoint cannot serve the \
                           request.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "NOT_CONFIGURED", "message": "Persistence is not configured" } })
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn list_tasks_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
    ApiQuery(params): ApiQuery<ListTasksQuery>,
) -> Result<Json<Vec<TaskView>>, ApiError> {
    let store = require_store(&state)?;
    let key_id = authenticate_caller(store, &headers).await?.id;

    let limit = clamp_page_limit(params.limit);
    let offset = clamp_offset(params.offset);

    let tasks = store
        .list_tasks_for_key(&key_id, params.status, limit, offset)
        .await
        .map_err(|e| {
            tracing::error!(key_id = %key_id, error = %e, "failed to list tasks");
            ApiError::internal("Internal error: failed to list tasks")
        })?;

    Ok(Json(tasks.into_iter().map(TaskView::from).collect()))
}

/// Request body for `POST /admin/keys`. The whole body is optional; an empty body creates an
/// unlabeled key. `invalid_at` is an optional unix timestamp at which the key expires; omit or
/// send `null` for a key that never expires. It must be in the future.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct CreateApiKeyRequest {
    /// Free-text name for the key, shown in listings. Blank labels are normalized to absent.
    #[serde(default)]
    pub label: Option<String>,
    /// Unix timestamp at which the key stops authenticating. Omit or send `null` for a key that
    /// never expires. Must be in the future.
    #[serde(default)]
    pub invalid_at: Option<i64>,
    /// Per-key requests-per-minute override for `POST /tasks`. Omit or send `null` to limit the
    /// key at the global default rate (`RATE_LIMIT_RPM`). Must be positive when present.
    #[serde(default)]
    pub rpm_limit: Option<u32>,
}

/// `POST /admin/keys` — issues a new API key. Admin-only. The response carries the raw key
/// value exactly once; it is not persisted in the clear and cannot be retrieved later.
#[utoipa::path(
    post,
    path = "/admin/keys",
    tag = "Admin",
    operation_id = "createApiKey",
    summary = "Issue an API key",
    description = "Mints a new API key and returns its value. The value is returned exactly once \
                   and is not recoverable afterwards, since only its hash is stored. The whole \
                   body is optional: an empty body creates an unlabelled key that never expires \
                   and uses the global rate limit.",
    security(("AdminAuth" = [])),
    request_body = CreateApiKeyRequest,
    responses(
        (
            status = 201,
            description = "The key was created. `key` is the value to send as a bearer token and \
                           is shown only here.",
            body = CreatedApiKey
        ),
        (
            status = 400,
            description = "`invalid_at` is not in the future, `rpm_limit` is zero, or the body is \
                           not valid JSON. Returned with code `INVALID_REQUEST`.",
            body = ApiErrorEnvelope
        ),
        (
            status = 401,
            description = "Missing or invalid `ADMIN_KEY`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 503,
            description = "The admin API is not configured (`ADMIN_KEY` is unset), or task \
                           persistence is not configured.",
            body = ApiErrorEnvelope
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn create_api_key_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<(StatusCode, Json<CreatedApiKey>), ApiError> {
    authorize_admin(&state, &headers)?;
    let store = require_store(&state)?;

    // The body is an optional JSON object; an empty body means an unlabeled, never-expiring key.
    let request = if body.is_empty() {
        CreateApiKeyRequest::default()
    } else {
        serde_json::from_slice::<CreateApiKeyRequest>(&body).map_err(|e| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                format!("invalid request body: {e}"),
            )
        })?
    };

    // Blank labels are normalized away so listings never show empty strings.
    let label = request.label.filter(|l| !l.trim().is_empty());

    // Reject an expiry that is already in the past — such a key would be born dead.
    if let Some(invalid_at) = request.invalid_at
        && invalid_at <= unix_now()
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "invalid_at must be a unix timestamp in the future",
        ));
    }

    // A zero override would rate-limit the key to nothing; reject it so a misconfiguration
    // surfaces at creation rather than as a permanently 429ing key. Omit the field for the
    // global default rate.
    if request.rpm_limit == Some(0) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "rpm_limit must be a positive number of requests per minute",
        ));
    }

    let created = store
        .create_api_key_with_rpm(label, request.invalid_at, request.rpm_limit)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to create api key");
            ApiError::internal("Failed to create API key")
        })?;
    info!(key_id = %created.id, rpm_limit = ?created.rpm_limit, "api key created");
    Ok((StatusCode::CREATED, Json(created)))
}

/// `GET /admin/keys` — lists metadata for active keys. Admin-only. Never returns key values.
#[utoipa::path(
    get,
    path = "/admin/keys",
    tag = "Admin",
    operation_id = "listApiKeys",
    summary = "List active API keys",
    description = "Lists metadata for every key that still authenticates. Key values and their \
                   hashes are never returned.",
    security(("AdminAuth" = [])),
    responses(
        (status = 200, description = "Metadata for the active keys.", body = Vec<ApiKeyMetadata>),
        (
            status = 401,
            description = "Missing or invalid `ADMIN_KEY`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 503,
            description = "The admin API is not configured (`ADMIN_KEY` is unset), or task \
                           persistence is not configured.",
            body = ApiErrorEnvelope
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn list_api_keys_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiKeyMetadata>>, ApiError> {
    authorize_admin(&state, &headers)?;
    let store = require_store(&state)?;

    let keys = store.list_api_keys().await.map_err(|e| {
        tracing::error!(error = %e, "failed to list api keys");
        ApiError::internal("Failed to list API keys")
    })?;
    Ok(Json(keys))
}

/// `DELETE /admin/keys/:id` — revokes a key, taking effect immediately. Admin-only. Returns
/// 204 on success and 404 when no active key with that id exists.
#[utoipa::path(
    delete,
    path = "/admin/keys/{id}",
    tag = "Admin",
    operation_id = "revokeApiKey",
    summary = "Revoke an API key",
    description = "Revokes a key by its public id, taking effect on the next request that \
                   presents it. Tasks the key already created are unaffected but become \
                   unreachable, since a task is only visible to the key that created it.",
    security(("AdminAuth" = [])),
    params(
        ("id" = String, Path, description = "Public id of the key, from the create or list response.")
    ),
    responses(
        (status = 204, description = "The key was revoked."),
        (
            status = 401,
            description = "Missing or invalid `ADMIN_KEY`.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "UNAUTHORIZED", "message": "Unauthorized" } })
        ),
        (
            status = 404,
            description = "No active key with this id exists.",
            body = ApiErrorEnvelope
        ),
        (
            status = 503,
            description = "The admin API is not configured (`ADMIN_KEY` is unset), or task \
                           persistence is not configured.",
            body = ApiErrorEnvelope
        ),
        (
            status = 500,
            description = "An unexpected server-side error occurred.",
            body = ApiErrorEnvelope,
            example = json!({ "error": { "code": "INTERNAL", "message": "Internal error: task queue unavailable" } })
        )
    )
)]
pub async fn revoke_api_key_handler(
    State(state): State<IngressState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    authorize_admin(&state, &headers)?;
    let store = require_store(&state)?;

    let revoked = store.revoke_api_key(&id).await.map_err(|e| {
        tracing::error!(error = %e, "failed to revoke api key");
        ApiError::internal("Failed to revoke API key")
    })?;

    if revoked {
        info!(key_id = %id, "api key revoked");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(
            StatusCode::NOT_FOUND,
            ErrorCode::NotFound,
            "API key not found",
        ))
    }
}

#[utoipa::path(
    get,
    path = "/avs-metadata",
    tag = "Metadata",
    operation_id = "getAvsMetadata",
    summary = "AVS identity and contract addresses",
    description = "Returns the AVS identity document in the exact shape the restaking indexer \
                   expects (the URL registered via `updateAVSMetadataURI`), plus a `contracts` \
                   block naming the addresses a target contract must be wired to in order to \
                   settle. Public; no authentication required.\n\nThe `contracts` block is the \
                   authoritative source for those addresses. They belong to whichever AVS \
                   deployment is currently signing, each redeployment provisions a new signature \
                   checker, and a target wired to a superseded one passes every validation here \
                   and then reverts `InvalidQuorumApkHash` on chain. Read them from this endpoint \
                   rather than copying them into your own configuration.\n\nThe block is \
                   omitted entirely when the router could not establish the addresses, so a \
                   client that finds it absent has no answer rather than a wrong one.",
    responses(
        (status = 200, description = "AVS metadata document.", body = AvsMetadata)
    )
)]
pub async fn avs_metadata_handler(State(state): State<IngressState>) -> Json<AvsMetadata> {
    Json(state.avs_metadata.clone())
}

/// Gives axum's built-in 404 (no matching route) and 405 (method not allowed) responses the same
/// `{ "error": { "code", "message" } }` body as handler errors, so every error the ingress emits
/// shares one contract.
///
/// Only framework-generated errors are rewritten: a 404/405 with no `Content-Type`. Handler errors
/// and the API's own envelopes always set `application/json`, so they pass through untouched. The
/// body is replaced in place rather than rebuilt, preserving the headers axum computed — in
/// particular the `Allow` header that a spec-compliant 405 must carry.
async fn wrap_framework_error(mut resp: Response) -> Response {
    let status = resp.status();
    let is_framework_error = matches!(
        status,
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
    ) && resp.headers().get(header::CONTENT_TYPE).is_none();
    if !is_framework_error {
        return resp;
    }

    let (code, message) = if status == StatusCode::METHOD_NOT_ALLOWED {
        (ErrorCode::MethodNotAllowed, "Method not allowed")
    } else {
        (ErrorCode::NotFound, "Not found")
    };
    let envelope = ApiErrorEnvelope {
        error: ApiErrorBody {
            code,
            message: message.to_string(),
        },
    };
    let body = match serde_json::to_vec(&envelope) {
        Ok(bytes) => bytes,
        // A fixed-shape struct cannot realistically fail to serialize; if it somehow does, leave
        // axum's original response untouched rather than panicking.
        Err(_) => return resp,
    };

    let len = body.len();
    *resp.body_mut() = axum::body::Body::from(body);
    let headers = resp.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    resp
}

/// Builds the ingress router.
///
/// Adding a route here is three edits, because the OpenAPI document is generated from the
/// handlers and axum offers no way to enumerate a `Router`: annotate the handler with
/// `#[utoipa::path]`, add it to the `paths(...)` list in [`crate::openapi`], and update the route
/// table in that module's `the_documented_routes_are_exactly_the_route_table` test. A route added
/// only here compiles and serves, but ships undocumented.
pub fn build_app() -> Router<IngressState> {
    Router::new()
        .route("/healthz", get(crate::operator_http::healthz_handler))
        .route("/avs-metadata", get(avs_metadata_handler))
        .route("/tasks", post(submit_task_handler).get(list_tasks_handler))
        .route("/tasks/:task_id", get(get_task_handler))
        .route(
            "/admin/keys",
            post(create_api_key_handler).get(list_api_keys_handler),
        )
        .route("/admin/keys/:id", delete(revoke_api_key_handler))
        .layer(axum::middleware::map_response(wrap_framework_error))
}

// Start the HTTP server in a background task
pub async fn start_gas_killer_http_server(state: IngressState, addr: &str) {
    let app = build_app().with_state(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind HTTP server");
    info!("Creator HTTP server running on {}", addr);
    axum::serve(listener, app)
        .await
        .expect("HTTP server failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a valid request that passes all checks.
    fn valid_request() -> GasKillerTaskRequest {
        GasKillerTaskRequest {
            body: GasKillerTaskRequestBody {
                target_address: "0x0000000000000000000000000000000000000001"
                    .parse()
                    .unwrap(),
                from_address: "0x0000000000000000000000000000000000000002"
                    .parse()
                    .unwrap(),
                call_data: vec![0xAB, 0xCD, 0xEF, 0x01], // 4-byte selector
                transition_index: Some(0),
                value: U256::ZERO,
                block_height: 1,
            },
        }
    }

    // -- baseline --

    #[test]
    fn test_valid_request_passes() {
        assert!(valid_request().validate().is_ok());
    }

    // -- individual validation checks --

    #[test]
    fn test_zero_target_address() {
        let mut req = valid_request();
        req.body.target_address = Address::ZERO;
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::ZeroTargetAddress
        );
    }

    #[test]
    fn test_zero_from_address() {
        let mut req = valid_request();
        req.body.from_address = Address::ZERO;
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::ZeroFromAddress
        );
    }

    #[test]
    fn test_empty_call_data() {
        let mut req = valid_request();
        req.body.call_data = vec![];
        assert_eq!(req.validate().unwrap_err(), ValidationError::EmptyCallData);
    }

    #[test]
    fn test_call_data_too_short() {
        let mut req = valid_request();
        req.body.call_data = vec![0x01, 0x02, 0x03]; // 3 bytes, need 4
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::CallDataTooShort { len: 3 }
        );
    }

    #[test]
    fn test_call_data_at_exactly_4_bytes() {
        let mut req = valid_request();
        req.body.call_data = vec![0x01, 0x02, 0x03, 0x04];
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_call_data_too_large() {
        let mut req = valid_request();
        req.body.call_data = vec![0u8; MAX_EVM_TX_CALLDATA_SIZE + 1];
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::CallDataTooLarge {
                len: MAX_EVM_TX_CALLDATA_SIZE + 1,
                max: MAX_EVM_TX_CALLDATA_SIZE,
            }
        );
    }

    #[test]
    fn test_call_data_at_max_size() {
        let mut req = valid_request();
        req.body.call_data = vec![0u8; MAX_EVM_TX_CALLDATA_SIZE];
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_zero_block_height() {
        let mut req = valid_request();
        req.body.block_height = 0;
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::ZeroBlockHeight
        );
    }

    // -- Display --

    #[test]
    fn test_validation_error_display() {
        assert_eq!(
            ValidationError::ZeroTargetAddress.to_string(),
            "target_address is zero"
        );
        assert_eq!(
            ValidationError::ZeroFromAddress.to_string(),
            "from_address is zero"
        );
        assert_eq!(
            ValidationError::EmptyCallData.to_string(),
            "call_data is empty"
        );
        assert_eq!(
            ValidationError::CallDataTooShort { len: 2 }.to_string(),
            "call_data too short (2 bytes, minimum 4)"
        );
        assert_eq!(
            ValidationError::CallDataTooLarge {
                len: 200_000,
                max: 131_072
            }
            .to_string(),
            "call_data too large (200000 bytes, maximum 131072)"
        );
        assert_eq!(
            ValidationError::ZeroBlockHeight.to_string(),
            "block_height is zero"
        );
    }

    // -- priority ordering --

    #[test]
    fn test_first_failure_wins() {
        // Request that fails multiple checks: target=zero, from=zero, call_data empty, block=0
        let req = GasKillerTaskRequest {
            body: GasKillerTaskRequestBody {
                target_address: Address::ZERO,
                from_address: Address::ZERO,
                call_data: vec![],
                transition_index: Some(u64::MAX),
                value: U256::MAX,
                block_height: 0,
            },
        };
        // First check is target_address
        assert_eq!(
            req.validate().unwrap_err(),
            ValidationError::ZeroTargetAddress
        );
    }

    // -- pagination bound tests --

    #[test]
    fn page_limit_defaults_when_unset() {
        assert_eq!(clamp_page_limit(None), DEFAULT_TASK_PAGE_SIZE);
    }

    #[test]
    fn page_limit_clamps_to_bounds() {
        // Above the cap is clamped down; below 1 (zero or negative) is floored to 1.
        assert_eq!(
            clamp_page_limit(Some(MAX_TASK_PAGE_SIZE + 1)),
            MAX_TASK_PAGE_SIZE
        );
        assert_eq!(clamp_page_limit(Some(10_000)), MAX_TASK_PAGE_SIZE);
        assert_eq!(clamp_page_limit(Some(0)), 1);
        assert_eq!(clamp_page_limit(Some(-5)), 1);
        // A value already within range passes through unchanged.
        assert_eq!(clamp_page_limit(Some(25)), 25);
        assert_eq!(
            clamp_page_limit(Some(MAX_TASK_PAGE_SIZE)),
            MAX_TASK_PAGE_SIZE
        );
    }

    #[test]
    fn offset_defaults_and_floors_negatives() {
        assert_eq!(clamp_offset(None), 0);
        assert_eq!(clamp_offset(Some(-1)), 0);
        assert_eq!(clamp_offset(Some(0)), 0);
        assert_eq!(clamp_offset(Some(42)), 42);
    }

    // -- queue-slot reservation --

    fn slot_test_state(max_queue_depth: usize) -> (IngressState, TaskQueueDepth) {
        // The receiver is unused: reservation only touches the depth counter, never the channel.
        let (sender, _receiver) = crate::sequencer::task_channel();
        let queue_depth = crate::sequencer::task_queue_depth();
        let mut state = IngressState::without_metrics(sender, queue_depth.clone());
        state.max_queue_depth = max_queue_depth;
        (state, queue_depth)
    }

    #[test]
    fn queue_slot_reserves_up_to_cap_then_refuses() {
        let (state, queue_depth) = slot_test_state(2);

        let (_s1, d1) = QueueSlot::reserve(&state).expect("first reservation fits");
        let (_s2, d2) = QueueSlot::reserve(&state).expect("second reservation fills the queue");
        assert_eq!((d1, d2), (1, 2));
        assert_eq!(queue_depth.load(Ordering::Relaxed), 2);

        // At capacity the reservation is refused atomically and consumes no slot, so concurrent
        // submissions cannot race past the cap.
        assert!(QueueSlot::reserve(&state).is_err());
        assert_eq!(queue_depth.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn queue_slot_releases_on_drop_but_not_after_commit() {
        let (state, queue_depth) = slot_test_state(2);

        let (dropped, _) = QueueSlot::reserve(&state).expect("reservation fits");
        let (kept, _) = QueueSlot::reserve(&state).expect("reservation fits");
        assert_eq!(queue_depth.load(Ordering::Relaxed), 2);

        // An uncommitted slot frees capacity when it drops (a rejected/failed submission).
        drop(dropped);
        assert_eq!(queue_depth.load(Ordering::Relaxed), 1);

        // A committed slot stays counted — the enqueued task owns it until the sequencer dequeues.
        kept.commit();
        assert_eq!(queue_depth.load(Ordering::Relaxed), 1);
    }

    // -- HTTP handler integration tests --
    //
    // These tests call submit_task_handler through the real Axum router using
    // tower::ServiceExt::oneshot, so they exercise JSON extraction, status codes,
    // and queue interaction end-to-end without binding a port.

    mod http {
        use super::*;
        use alloy_primitives::address;
        use axum::body::Body;
        use axum::http::{Method, Request, StatusCode};
        use tower::util::ServiceExt; // for `oneshot`

        fn make_app() -> (Router, crate::sequencer::TaskReceiver) {
            let (sender, receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let state = IngressState::without_metrics(sender, queue_depth);
            let app = build_app().with_state(state);
            (app, receiver)
        }

        /// Builds an app backed by an in-memory store, optionally with an admin key. Returns the
        /// store handle so tests can mint/revoke keys directly, and the receiver so accepted
        /// tasks have somewhere to land (a dropped receiver would make `POST /tasks` fail on send).
        async fn make_app_with_store(
            admin_key: Option<&str>,
        ) -> (Router, SqliteStore, crate::sequencer::TaskReceiver) {
            let (sender, receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let store = SqliteStore::connect_in_memory()
                .await
                .expect("in-memory store should open");
            let state = IngressState::without_metrics(sender, queue_depth)
                .with_store(store.clone())
                .with_admin_key(admin_key.map(str::to_string));
            let app = build_app().with_state(state);
            (app, store, receiver)
        }

        /// An ingress state backed by an in-memory store with one API key minted in it, plus the
        /// queue-depth counter and receiver tests assert against. Task endpoints resolve the store
        /// and authenticate before anything else, so a test that means to reach validation, load
        /// shedding, or the queue needs both. Returned as state rather than a router so a caller
        /// can tune it (capacity, metrics, rate limiter) before building the app.
        async fn keyed_state() -> (
            IngressState,
            String,
            TaskQueueDepth,
            crate::sequencer::TaskReceiver,
        ) {
            let (sender, receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let store = SqliteStore::connect_in_memory()
                .await
                .expect("in-memory store should open");
            let key = store
                .create_api_key(None, None)
                .await
                .expect("minting a key should succeed")
                .key;
            let state =
                IngressState::without_metrics(sender, queue_depth.clone()).with_store(store);
            (state, key, queue_depth, receiver)
        }

        /// [`keyed_state`] with metrics attached, returning the created key rather than just its
        /// value so a test can assert on that key's counter series.
        async fn metered_state(
            metrics: Arc<MetricsCollector>,
        ) -> (
            IngressState,
            CreatedApiKey,
            TaskQueueDepth,
            crate::sequencer::TaskReceiver,
        ) {
            let (sender, receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let store = SqliteStore::connect_in_memory()
                .await
                .expect("in-memory store should open");
            let key = store
                .create_api_key(None, None)
                .await
                .expect("minting a key should succeed");
            let mut state =
                IngressState::without_metrics(sender, queue_depth.clone()).with_store(store);
            state.metrics = Some(metrics);
            (state, key, queue_depth, receiver)
        }

        /// One key's count on a per-key counter, or 0 when that key has no series on it yet. A
        /// dashboard reads the same numbers through `sum by (key_id) (...)`.
        fn key_count(counter: &crate::metrics::PerKeyCounter, key_id: &str) -> u64 {
            counter
                .get(&crate::metrics::key_labels(key_id))
                .map(|c| c.get())
                .unwrap_or(0)
        }

        /// [`keyed_state`] built into a router, for tests that do not tune the state.
        async fn make_app_with_key() -> (Router, String, crate::sequencer::TaskReceiver) {
            let (state, key, _queue_depth, receiver) = keyed_state().await;
            (build_app().with_state(state), key, receiver)
        }

        fn admin_request(
            method: Method,
            uri: &str,
            token: Option<&str>,
            body: &str,
        ) -> Request<Body> {
            let mut builder = Request::builder().method(method).uri(uri);
            if let Some(token) = token {
                builder = builder.header("Authorization", format!("Bearer {token}"));
            }
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }

        /// An authenticated submission: `POST /tasks` carrying a Bearer token.
        fn bearer_request(body: &str, token: &str) -> Request<Body> {
            Request::builder()
                .method(Method::POST)
                .uri("/tasks")
                .header("content-type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap()
        }

        fn json_request(body: &str) -> Request<Body> {
            Request::builder()
                .method(Method::POST)
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }

        fn valid_body() -> String {
            valid_body_with_index(0)
        }

        /// A valid submission body pinned to an explicit `transition_index`, so successive
        /// submissions are distinct work rather than deduplicated retries of the same request.
        fn valid_body_with_index(transition_index: u64) -> String {
            serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": transition_index,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string()
        }

        async fn accepted_body(resp: axum::response::Response) -> TaskAcceptedResponse {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).expect("accepted response should deserialize")
        }

        async fn error_envelope(resp: axum::response::Response) -> crate::error::ApiErrorEnvelope {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&bytes)
                .expect("error response should deserialize as the ApiError envelope")
        }

        #[tokio::test]
        async fn test_healthz_returns_200() {
            let (app, _queue) = make_app();
            let req = Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        #[tokio::test]
        async fn test_valid_request_returns_202_and_queues_task() {
            let (app, store, mut receiver) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();

            let resp = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();

            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let body = accepted_body(resp).await;
            assert_eq!(body.status, TaskStatus::Queued);
            let id = uuid::Uuid::parse_str(&body.task_id).expect("task_id should be a UUID");
            assert_eq!(id.get_version_num(), 4, "task ids must be UUID v4");
            assert!(
                receiver.try_recv().is_ok(),
                "task should have been pushed to queue"
            );

            // The task is persisted before the response, scoped to the submitting key.
            let persisted = store
                .get_task(&body.task_id)
                .await
                .unwrap()
                .expect("accepted task should be persisted");
            assert_eq!(persisted.key_id, created.id);
            assert_eq!(persisted.status, TaskStatus::Queued);
        }

        #[tokio::test]
        async fn test_zero_target_address_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000000",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidAddress);
            assert!(body.error.message.contains("target_address is zero"));
        }

        #[tokio::test]
        async fn test_zero_from_address_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000000",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidAddress);
            assert!(body.error.message.contains("from_address is zero"));
        }

        #[tokio::test]
        async fn test_empty_call_data_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
            assert!(body.error.message.contains("call_data is empty"));
        }

        #[tokio::test]
        async fn test_call_data_too_short_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0x01, 0x02, 0x03],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
            assert!(body.error.message.contains("call_data too short"));
        }

        #[tokio::test]
        async fn test_call_data_too_large_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let oversized = vec![0u8; MAX_EVM_TX_CALLDATA_SIZE + 1];
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      oversized,
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::CalldataTooLarge);
            assert!(body.error.message.contains("call_data too large"));
        }

        #[tokio::test]
        async fn test_zero_block_height_returns_400() {
            let (app, key, _rx) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 0
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
            assert!(body.error.message.contains("block_height is zero"));
        }

        #[tokio::test]
        async fn test_malformed_json_returns_4xx() {
            let (app, _queue) = make_app();
            let req = Request::builder()
                .method(Method::POST)
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from("not json at all {{{"))
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert!(
                resp.status().is_client_error(),
                "malformed JSON should return 4xx, got {}",
                resp.status()
            );
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn test_missing_required_field_returns_422() {
            let (app, _queue) = make_app();
            // `block_height` is missing
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000001",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0"
                }
            })
            .to_string();

            let resp = app.oneshot(json_request(&payload)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn test_empty_body_returns_4xx() {
            let (app, _queue) = make_app();
            let req = Request::builder()
                .method(Method::POST)
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert!(
                resp.status().is_client_error(),
                "empty body should return 4xx, got {}",
                resp.status()
            );
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn test_wrong_method_returns_405() {
            let (app, _queue) = make_app();
            let req = Request::builder()
                .method(Method::DELETE)
                .uri("/tasks")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
            // A spec-compliant 405 must advertise the supported methods; rewriting the body into
            // the error envelope must not drop the Allow header axum computes for the route.
            let allow = resp
                .headers()
                .get(axum::http::header::ALLOW)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            assert!(
                allow.contains("POST"),
                "Allow should list POST, got {allow:?}"
            );
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::MethodNotAllowed);
        }

        #[tokio::test]
        async fn test_unknown_path_returns_404() {
            let (app, _queue) = make_app();
            let req = Request::builder()
                .method(Method::GET)
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::NotFound);
        }

        // Pins the documented contract of `wrap_framework_error`: a handler that emits a bare,
        // bodyless `StatusCode::NOT_FOUND`/`METHOD_NOT_ALLOWED` (no Content-Type) is rewritten into
        // the error envelope, while a handler that already returns an envelope (Content-Type
        // application/json) keeps its specific status, code, and message untouched. Guards against a
        // future handler's error shape silently diverging from — or being clobbered by — the layer.
        #[tokio::test]
        async fn test_bare_status_handler_is_wrapped_but_envelope_handler_is_preserved() {
            use crate::error::ErrorCode;

            let app: Router = Router::new()
                .route("/bare", get(|| async { StatusCode::NOT_FOUND }))
                .route(
                    "/typed",
                    get(|| async {
                        ApiError::new(
                            StatusCode::NOT_FOUND,
                            ErrorCode::NotFound,
                            "widget 7 not found",
                        )
                    }),
                )
                .layer(axum::middleware::map_response(wrap_framework_error));

            // Bare StatusCode from a handler → wrapped into the generic envelope.
            let bare = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri("/bare")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(bare.status(), StatusCode::NOT_FOUND);
            let bare_body = error_envelope(bare).await;
            assert_eq!(bare_body.error.code, ErrorCode::NotFound);
            assert_eq!(bare_body.error.message, "Not found");

            // Handler-built envelope (application/json) → passed through with its specific message.
            let typed = app
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri("/typed")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(typed.status(), StatusCode::NOT_FOUND);
            let typed_body = error_envelope(typed).await;
            assert_eq!(typed_body.error.code, ErrorCode::NotFound);
            assert_eq!(typed_body.error.message, "widget 7 not found");
        }

        #[tokio::test]
        async fn test_valid_request_does_not_leave_extra_tasks() {
            // Two sequential valid requests for distinct work → queue should hold exactly two tasks.
            let (app, store, mut receiver) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();

            app.clone()
                .oneshot(bearer_request(&valid_body_with_index(0), &created.key))
                .await
                .unwrap();
            app.oneshot(bearer_request(&valid_body_with_index(1), &created.key))
                .await
                .unwrap();

            assert!(receiver.try_recv().is_ok());
            assert!(receiver.try_recv().is_ok());
            assert!(
                receiver.try_recv().is_err(),
                "queue should be empty after two recvs"
            );
        }

        // -- auth tests --
        //
        // Task-submission auth against the store is covered in the admin/API-key section below
        // (valid, revoked, and unknown keys). These cover a store-less deployment and the
        // always-open utility endpoints.

        #[tokio::test]
        async fn test_submission_without_store_returns_503() {
            // Submission persists before responding and attributes the task to a key, both of
            // which need the store, so a store-less ingress cannot serve `POST /tasks` at all. It
            // says so before spending validation or RPC work on the request, and there is no
            // configuration in which it instead accepts the task and drops it.
            let (app, _queue) = make_app();
            let resp = app.oneshot(json_request(&valid_body())).await.unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::NotConfigured);
        }

        #[tokio::test]
        async fn test_submission_without_store_is_refused_before_validation() {
            // The store check precedes validation, so an invalid body still reports the missing
            // store: nothing downstream of it runs, which is the point of checking first.
            let (app, _queue) = make_app();
            let invalid = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000000",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(json_request(&invalid)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::NotConfigured);
        }

        #[tokio::test]
        async fn test_healthz_unauthenticated_with_store_configured() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .body(Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        #[tokio::test]
        async fn test_avs_metadata_returns_200_with_valid_json() {
            let (sender, _receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let mut state = IngressState::without_metrics(sender, queue_depth);
            state.avs_metadata = AvsMetadata {
                name: "Gas Killer".to_string(),
                website: "https://gaskiller.xyz".to_string(),
                description: "Test AVS".to_string(),
                logo: Some("https://example.com/logo.png".to_string()),
                twitter: Some("https://x.com/gaskiller".to_string()),
                operator_sets: None,
                contracts: ResolvedContracts::default(),
            };
            let app = build_app().with_state(state);
            let req = Request::builder()
                .method(Method::GET)
                .uri("/avs-metadata")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let metadata: AvsMetadata =
                serde_json::from_slice(&bytes).expect("response should be valid AvsMetadata JSON");
            assert_eq!(metadata.name, "Gas Killer");
            assert_eq!(metadata.website, "https://gaskiller.xyz");

            // Absent rather than null when unresolved, so a client can branch on presence.
            let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(
                raw.get("contracts").is_none(),
                "an unresolved contract set must be omitted, got {raw}"
            );
        }

        #[tokio::test]
        async fn avs_metadata_publishes_the_contract_set_in_camel_case() {
            let (sender, _receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let mut state = IngressState::without_metrics(sender, queue_depth);
            // Published the way the resolver publishes it: into the shared slot the response reads.
            let contracts = ResolvedContracts::default();
            contracts.publish(gas_killer_common::avs_contracts::AvsContracts {
                chain_id: 11155111,
                avs_address: address!("dCec8ce0a03848B55989Bcc711e424Ca31d9eeD9"),
                bls_signature_checker: address!("6953fc47FC8b7568801f3fdc327bc0d9aD12E5b9"),
                registry_coordinator: address!("0a032D62dde46670Ae40Ce532C97f6CE9Af72Dc4"),
                schnorr_stake_registry: Some(address!("00000000000000000000000000000000000000ee")),
                demo_target: Some(address!("00000000000000000000000000000000000000aa")),
                demo_factory: None,
            });
            state.avs_metadata = AvsMetadata {
                name: "Gas Killer".to_string(),
                website: "https://gaskiller.xyz".to_string(),
                description: "Test AVS".to_string(),
                logo: None,
                twitter: None,
                operator_sets: None,
                contracts,
            };
            let app = build_app().with_state(state);
            let req = Request::builder()
                .method(Method::GET)
                .uri("/avs-metadata")
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let contracts = &raw["contracts"];

            // Integrators read these keys off the wire, so the names are the contract, not an
            // implementation detail of the Rust field names.
            assert_eq!(contracts["chainId"], 11155111);
            assert_eq!(
                contracts["avsAddress"],
                "0xdCec8ce0a03848B55989Bcc711e424Ca31d9eeD9"
            );
            assert_eq!(
                contracts["blsSignatureChecker"],
                "0x6953fc47FC8b7568801f3fdc327bc0d9aD12E5b9"
            );
            assert_eq!(
                contracts["registryCoordinator"],
                "0x0a032D62dde46670Ae40Ce532C97f6CE9Af72Dc4"
            );
            assert_eq!(
                contracts["demoTarget"],
                "0x00000000000000000000000000000000000000AA"
            );
            assert!(
                contracts.get("demoFactory").is_none(),
                "an unconfigured demo contract must be omitted, not null"
            );
        }

        #[tokio::test]
        async fn test_avs_metadata_accessible_without_auth_with_store_configured() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = Request::builder()
                .method(Method::GET)
                .uri("/avs-metadata")
                .body(Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // -- admin API + API-key auth tests --

        async fn created_key_json(resp: axum::response::Response) -> serde_json::Value {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).expect("create response should be JSON")
        }

        #[tokio::test]
        async fn admin_create_returns_503_when_admin_key_unset() {
            let (app, _store, _rx) = make_app_with_store(None).await;
            let req = admin_request(Method::POST, "/admin/keys", None, "{}");
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::NotConfigured);
        }

        #[tokio::test]
        async fn admin_create_rejects_missing_or_wrong_credential() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            // No credential.
            let resp = app
                .clone()
                .oneshot(admin_request(Method::POST, "/admin/keys", None, "{}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
            // Wrong credential.
            let resp = app
                .oneshot(admin_request(
                    Method::POST,
                    "/admin/keys",
                    Some("wrong"),
                    "{}",
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn admin_create_issues_key_with_prefix() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = admin_request(
                Method::POST,
                "/admin/keys",
                Some("admin-secret"),
                r#"{"label":"client-a"}"#,
            );
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
            let json = created_key_json(resp).await;
            assert!(
                json["key"].as_str().unwrap().starts_with("gk_"),
                "created key should carry the gk_ prefix"
            );
            assert_eq!(json["label"], "client-a");
            assert!(json["id"].as_str().is_some());
            assert!(json["created_at"].as_i64().unwrap() > 0);
            assert!(json["invalid_at"].is_null(), "no expiry was requested");
        }

        #[tokio::test]
        async fn admin_create_honors_future_expiry() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            // Year 2100 — comfortably in the future.
            let req = admin_request(
                Method::POST,
                "/admin/keys",
                Some("admin-secret"),
                r#"{"invalid_at":4102444800}"#,
            );
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
            let json = created_key_json(resp).await;
            assert_eq!(json["invalid_at"].as_i64(), Some(4_102_444_800));
        }

        #[tokio::test]
        async fn admin_create_honors_rpm_override() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = admin_request(
                Method::POST,
                "/admin/keys",
                Some("admin-secret"),
                r#"{"label":"vip","rpm_limit":600}"#,
            );
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
            let json = created_key_json(resp).await;
            assert_eq!(json["rpm_limit"].as_u64(), Some(600));
        }

        #[tokio::test]
        async fn admin_create_defaults_rpm_to_null() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = admin_request(Method::POST, "/admin/keys", Some("admin-secret"), "{}");
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
            let json = created_key_json(resp).await;
            assert!(
                json["rpm_limit"].is_null(),
                "a key with no override reports a null rpm_limit (global default applies)"
            );
        }

        #[tokio::test]
        async fn admin_create_rejects_zero_rpm() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = admin_request(
                Method::POST,
                "/admin/keys",
                Some("admin-secret"),
                r#"{"rpm_limit":0}"#,
            );
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn admin_create_rejects_past_expiry() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            // A 1970 timestamp is already in the past.
            let req = admin_request(
                Method::POST,
                "/admin/keys",
                Some("admin-secret"),
                r#"{"invalid_at":1}"#,
            );
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn admin_create_accepts_empty_body_as_unlabeled() {
            let (app, _store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let req = Request::builder()
                .method(Method::POST)
                .uri("/admin/keys")
                .header("Authorization", "Bearer admin-secret")
                .body(Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
            let json = created_key_json(resp).await;
            assert!(json["label"].is_null(), "empty body should yield no label");
        }

        #[tokio::test]
        async fn admin_list_returns_metadata_without_key_value() {
            let (app, store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let created = store
                .create_api_key(Some("client-a".to_string()), None)
                .await
                .unwrap();

            let resp = app
                .oneshot(admin_request(
                    Method::GET,
                    "/admin/keys",
                    Some("admin-secret"),
                    "",
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let entries = list.as_array().expect("list should be a JSON array");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0]["id"], created.id);
            assert_eq!(entries[0]["label"], "client-a");
            assert!(
                entries[0].get("key").is_none(),
                "listing must never expose the key value"
            );
            assert!(
                entries[0].get("key_hash").is_none(),
                "listing must never expose the key hash"
            );
        }

        #[tokio::test]
        async fn admin_revoke_returns_204_then_404() {
            let (app, store, _rx) = make_app_with_store(Some("admin-secret")).await;
            let created = store.create_api_key(None, None).await.unwrap();

            let uri = format!("/admin/keys/{}", created.id);
            let resp = app
                .clone()
                .oneshot(admin_request(
                    Method::DELETE,
                    &uri,
                    Some("admin-secret"),
                    "",
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NO_CONTENT);

            // Revoking again reports 404 — no active key with that id remains.
            let resp = app
                .oneshot(admin_request(
                    Method::DELETE,
                    &uri,
                    Some("admin-secret"),
                    "",
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn tasks_endpoint_accepts_valid_api_key_and_persists() {
            let (app, store, mut rx) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();

            let resp = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let body = accepted_body(resp).await;
            assert_eq!(body.status, TaskStatus::Queued);
            assert!(rx.try_recv().is_ok(), "valid key should queue the task");

            let persisted = store
                .get_task(&body.task_id)
                .await
                .unwrap()
                .expect("accepted task should be persisted");
            assert_eq!(persisted.key_id, created.id);
        }

        #[tokio::test]
        async fn duplicate_submission_returns_200_and_does_not_reenqueue() {
            let (app, store, mut rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();

            let first = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(first.status(), StatusCode::ACCEPTED);
            let first_body = accepted_body(first).await;
            assert!(
                !first_body.deduplicated,
                "a fresh submission is not deduplicated"
            );
            assert!(
                rx.try_recv().is_ok(),
                "the first submission enqueues a task"
            );

            // Same request again: a retry collapses onto the in-flight task.
            let second = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(
                second.status(),
                StatusCode::OK,
                "a deduplicated retry answers 200 OK, not 202"
            );
            let second_body = accepted_body(second).await;
            assert!(second_body.deduplicated);
            assert_eq!(
                second_body.task_id, first_body.task_id,
                "the retry returns the original task id"
            );
            assert_eq!(second_body.status, TaskStatus::Queued);
            assert!(
                rx.try_recv().is_err(),
                "a deduplicated retry must not enqueue a second task"
            );
        }

        #[tokio::test]
        async fn resubmission_after_failure_creates_new_task() {
            let (app, store, mut rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();

            let first = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(first.status(), StatusCode::ACCEPTED);
            let first_body = accepted_body(first).await;
            assert!(rx.try_recv().is_ok());

            // Once the task fails, its work is no longer covered by an in-flight submission.
            store
                .mark_task_failed(&first_body.task_id, "aggregation timed out")
                .await
                .unwrap();

            let second = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(
                second.status(),
                StatusCode::ACCEPTED,
                "resubmission after a failure creates a fresh task"
            );
            let second_body = accepted_body(second).await;
            assert!(!second_body.deduplicated);
            assert_ne!(second_body.task_id, first_body.task_id);
            assert!(
                rx.try_recv().is_ok(),
                "the fresh task is enqueued for aggregation"
            );
        }

        #[tokio::test]
        async fn deduplicated_submission_increments_metric() {
            let metrics = Arc::new(MetricsCollector::new());
            let (state, key, _queue_depth, mut rx) = metered_state(Arc::clone(&metrics)).await;
            let app = build_app().with_state(state);

            app.clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            let _ = rx.try_recv();
            let second = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(second.status(), StatusCode::OK);

            assert_eq!(key_count(&metrics.ingress_deduplicated, &key.id), 1);
            assert_eq!(
                key_count(&metrics.ingress_accepted, &key.id),
                1,
                "only the fresh submission counts as accepted"
            );
        }

        #[tokio::test]
        async fn tasks_endpoint_rejects_missing_token_when_store_present() {
            let (app, _store, _rx) = make_app_with_store(None).await;
            let req = Request::builder()
                .method(Method::POST)
                .uri("/tasks")
                .header("content-type", "application/json")
                .body(Body::from(valid_body()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn tasks_endpoint_rejects_revoked_api_key() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();
            store.revoke_api_key(&created.id).await.unwrap();

            let resp = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn tasks_endpoint_rejects_unknown_key_when_store_present() {
            let (app, _store, _rx) = make_app_with_store(None).await;
            let resp = app
                .oneshot(bearer_request(&valid_body(), "gk_unknown"))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn tasks_endpoint_without_token_rejected_when_store_present() {
            let (app, _store, _rx) = make_app_with_store(None).await;
            let resp = app.oneshot(json_request(&valid_body())).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        // -- audit logging tests --
        //
        // The audit trail is a contract with the operator: every request outcome must name the
        // key that sent it (never the key value) and how long it took, so abusive traffic can be
        // traced back to a client. These assert on the fields the handlers actually emit.

        #[tokio::test]
        async fn accepted_submission_logs_the_calling_key_and_duration() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();

            let logs = crate::log_capture::capture_events();
            let resp = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let accepted = accepted_body(resp).await;

            let event = logs.find("Task accepted").unwrap_or_else(|| {
                panic!(
                    "expected a Task accepted line, logged: {:?}",
                    logs.messages()
                )
            });
            assert_eq!(event.field("key_id"), Some(created.id.as_str()));
            assert_eq!(event.field("task_id"), Some(accepted.task_id.as_str()));
            assert_eq!(
                event.field("target_address"),
                Some("0x0000000000000000000000000000000000000001")
            );
            assert_eq!(event.field("transition_index"), Some("Some(0)"));
            assert!(
                event
                    .field("duration_ms")
                    .and_then(|d| d.parse::<u64>().ok())
                    .is_some(),
                "duration_ms should be logged as a number, got {:?}",
                event.field("duration_ms")
            );
            // The key value itself must never reach a log line, in the message or in any field.
            logs.assert_never_logged(&created.key);
        }

        #[tokio::test]
        async fn revoked_key_rejection_names_the_key() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let created = store.create_api_key(None, None).await.unwrap();
            store.revoke_api_key(&created.id).await.unwrap();

            let logs = crate::log_capture::capture_events();
            let resp = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

            // A key that was issued and then revoked is still attributable, which is the whole
            // point: an operator can see which client is retrying with a dead key.
            let event = logs
                .find("Request rejected: API key is unknown, revoked, or expired")
                .unwrap_or_else(|| {
                    panic!("expected a rejection line, logged: {:?}", logs.messages())
                });
            assert_eq!(event.field("key_id"), Some(created.id.as_str()));
            // The rejection path holds the presented token to resolve its id; it must log the id
            // and nothing else.
            logs.assert_never_logged(&created.key);
        }

        #[tokio::test]
        async fn unknown_key_rejection_is_unattributed() {
            let (app, _store, _rx) = make_app_with_store(None).await;

            let logs = crate::log_capture::capture_events();
            let resp = app
                .oneshot(bearer_request(&valid_body(), "gk_never_issued"))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

            let event = logs
                .find("Request rejected: API key is unknown, revoked, or expired")
                .unwrap_or_else(|| {
                    panic!("expected a rejection line, logged: {:?}", logs.messages())
                });
            assert_eq!(event.field("key_id"), Some(UNATTRIBUTED_KEY_ID));
        }

        #[tokio::test]
        async fn missing_token_rejection_is_unattributed() {
            let (app, _store, _rx) = make_app_with_store(None).await;

            let logs = crate::log_capture::capture_events();
            let resp = app.oneshot(json_request(&valid_body())).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

            let event = logs
                .find("Request rejected: missing or malformed Authorization header")
                .unwrap_or_else(|| {
                    panic!("expected a rejection line, logged: {:?}", logs.messages())
                });
            assert_eq!(event.field("key_id"), Some(UNATTRIBUTED_KEY_ID));
        }

        #[tokio::test]
        async fn rate_limited_request_logs_the_calling_key_and_duration() {
            let (app, store, _rx) = make_rate_limited_app(1, None).await;
            let created = store.create_api_key(None, None).await.unwrap();

            let logs = crate::log_capture::capture_events();
            let first = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(first.status(), StatusCode::ACCEPTED);
            let second = app
                .oneshot(bearer_request(&valid_body(), &created.key))
                .await
                .unwrap();
            assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

            let event = logs
                .find("Task rejected: per-key rate limit exceeded")
                .unwrap_or_else(|| {
                    panic!("expected a rate-limit line, logged: {:?}", logs.messages())
                });
            assert_eq!(event.field("key_id"), Some(created.id.as_str()));
            assert!(
                event
                    .field("duration_ms")
                    .and_then(|d| d.parse::<u64>().ok())
                    .is_some(),
                "duration_ms should be logged as a number, got {:?}",
                event.field("duration_ms")
            );
        }

        // -- RPC circuit breaker tests --

        #[tokio::test]
        async fn submission_is_refused_while_the_required_chain_is_degraded() {
            let metrics = Arc::new(MetricsCollector::new());
            let (mut state, key, _queue_depth, _rx) = metered_state(Arc::clone(&metrics)).await;
            let health = Arc::new(crate::rpc_health::RpcHealth::new(
                1,
                [ChainRole::L1],
                Some(Arc::clone(&metrics)),
            ));
            health.record_failure(ChainRole::L1);
            state.rpc_health = Some(Arc::clone(&health));
            let app = build_app().with_state(state);

            // An invalid body: the refusal has to come from the breaker, which sits ahead of
            // validation, rather than from the field checks this body would also fail.
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000000",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app
                .oneshot(bearer_request(&payload, &key.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::RpcUnavailable);
            assert_eq!(key_count(&metrics.ingress_rpc_unavailable, &key.id), 1);
            assert_eq!(
                key_count(&metrics.ingress_rejected, &key.id),
                0,
                "the breaker sheds before validation, so nothing counts as rejected"
            );
        }

        #[tokio::test]
        async fn submission_is_accepted_once_the_chain_recovers() {
            let metrics = Arc::new(MetricsCollector::new());
            let (mut state, key, _queue_depth, _rx) = metered_state(Arc::clone(&metrics)).await;
            let health = Arc::new(crate::rpc_health::RpcHealth::new(
                1,
                [ChainRole::L1],
                Some(Arc::clone(&metrics)),
            ));
            health.record_failure(ChainRole::L1);
            state.rpc_health = Some(Arc::clone(&health));
            let app = build_app().with_state(state);

            let refused = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);

            // The background probe's next success is what reopens the gate; nothing about the
            // refusal itself latches it shut.
            health.record_success(ChainRole::L1);
            let accepted = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(accepted.status(), StatusCode::ACCEPTED);
            assert_eq!(key_count(&metrics.ingress_rpc_unavailable, &key.id), 1);
            assert_eq!(key_count(&metrics.ingress_accepted, &key.id), 1);
        }

        #[tokio::test]
        async fn a_degraded_secondary_chain_does_not_shed_submissions() {
            let metrics = Arc::new(MetricsCollector::new());
            let (mut state, key, _queue_depth, _rx) = metered_state(Arc::clone(&metrics)).await;
            let health = Arc::new(crate::rpc_health::RpcHealth::new(
                1,
                [ChainRole::L1, ChainRole::L2],
                Some(Arc::clone(&metrics)),
            ));
            health.record_failure(ChainRole::L2);
            state.rpc_health = Some(health);
            let app = build_app().with_state(state);

            let resp = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::ACCEPTED,
                "an L1 target is unaffected by an L2 outage"
            );
        }

        // -- queue capacity tests --

        #[tokio::test]
        async fn test_full_queue_returns_503() {
            let (mut state, key, queue_depth, _rx) = keyed_state().await;
            state.max_queue_depth = 1;
            queue_depth.store(1, std::sync::atomic::Ordering::Relaxed);
            let app = build_app().with_state(state);

            let resp = app
                .oneshot(bearer_request(&valid_body(), &key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::QueueFull);
            assert!(body.error.message.to_lowercase().contains("capacity"));
        }

        #[tokio::test]
        async fn test_full_queue_response_carries_retry_after() {
            let (mut state, key, queue_depth, _rx) = keyed_state().await;
            state.max_queue_depth = 1;
            queue_depth.store(1, std::sync::atomic::Ordering::Relaxed);
            let app = build_app().with_state(state);

            let resp = app
                .oneshot(bearer_request(&valid_body(), &key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            let retry_after = resp
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .expect("503 QUEUE_FULL must carry a Retry-After header");
            assert!(
                retry_after
                    .to_str()
                    .unwrap()
                    .parse::<u64>()
                    .is_ok_and(|secs| secs > 0),
                "Retry-After must be a positive number of seconds"
            );
        }

        #[tokio::test]
        async fn test_full_queue_increments_at_capacity_metric() {
            let metrics = Arc::new(MetricsCollector::new());
            let (mut state, key, queue_depth, _rx) = metered_state(Arc::clone(&metrics)).await;
            state.max_queue_depth = 1;
            queue_depth.store(1, std::sync::atomic::Ordering::Relaxed);
            let app = build_app().with_state(state);

            let resp = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(key_count(&metrics.ingress_at_capacity, &key.id), 1);
            assert_eq!(
                key_count(&metrics.ingress_rejected, &key.id),
                0,
                "load shedding runs before validation, so nothing counts as rejected"
            );
        }

        #[tokio::test]
        async fn test_queue_one_below_limit_still_accepts() {
            let (mut state, key, queue_depth, _rx) = keyed_state().await;
            state.max_queue_depth = 2;
            queue_depth.store(1, std::sync::atomic::Ordering::Relaxed);
            let app = build_app().with_state(state);

            let resp = app
                .oneshot(bearer_request(&valid_body(), &key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }

        #[tokio::test]
        async fn test_rejected_request_does_not_enqueue() {
            let (app, key, mut receiver) = make_app_with_key().await;
            let payload = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000000",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            app.oneshot(bearer_request(&payload, &key)).await.unwrap();
            assert!(
                receiver.try_recv().is_err(),
                "invalid task must not be pushed to queue"
            );
        }

        #[tokio::test]
        async fn test_rejected_request_releases_reserved_slot() {
            let (state, key, queue_depth, _rx) = keyed_state().await;
            let app = build_app().with_state(state);

            // A zero-target request reserves a slot, fails validation, and must release it on the
            // way out so a rejected submission never permanently shrinks capacity.
            let invalid = serde_json::json!({
                "body": {
                    "target_address": "0x0000000000000000000000000000000000000000",
                    "from_address":   "0x0000000000000000000000000000000000000002",
                    "call_data":      [0xAB, 0xCD, 0xEF, 0x01],
                    "transition_index": 0,
                    "value": "0x0",
                    "block_height": 1
                }
            })
            .to_string();

            let resp = app.oneshot(bearer_request(&invalid, &key)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                queue_depth.load(std::sync::atomic::Ordering::Relaxed),
                0,
                "a rejected request must not leave its reserved slot occupied"
            );
        }

        // -- per-key rate limit tests --

        /// Builds an app backed by an in-memory store whose ingress limits every key at
        /// `default_rpm` requests per minute (a `governor` burst of `default_rpm`). Returns the
        /// store, so tests can mint keys, and the receiver, so accepted tasks have somewhere to
        /// land. Optional metrics let a test assert the rejection counter.
        async fn make_rate_limited_app(
            default_rpm: u32,
            metrics: Option<Arc<MetricsCollector>>,
        ) -> (Router, SqliteStore, crate::sequencer::TaskReceiver) {
            let (sender, receiver) = crate::sequencer::task_channel();
            let queue_depth = crate::sequencer::task_queue_depth();
            let store = SqliteStore::connect_in_memory()
                .await
                .expect("in-memory store should open");
            let mut state =
                IngressState::without_metrics(sender, queue_depth).with_store(store.clone());
            state.rate_limiter = Arc::new(crate::rate_limit::KeyRateLimiter::new(
                std::num::NonZeroU32::new(default_rpm).unwrap(),
            ));
            state.metrics = metrics;
            let app = build_app().with_state(state);
            (app, store, receiver)
        }

        #[tokio::test]
        async fn rate_limit_rejects_second_request_with_429_and_retry_after() {
            let (app, store, _rx) = make_rate_limited_app(1, None).await;
            let key = store.create_api_key(None, None).await.unwrap();

            // First request is within the one-per-minute budget.
            let first = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(first.status(), StatusCode::ACCEPTED);

            // Second immediate request from the same key exceeds it.
            let second = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
            let retry_after = second
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .expect("429 must carry a Retry-After header");
            assert!(
                retry_after
                    .to_str()
                    .unwrap()
                    .parse::<u64>()
                    .is_ok_and(|secs| secs > 0),
                "Retry-After must be a positive number of seconds"
            );
            let body = error_envelope(second).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::RateLimited);
        }

        #[tokio::test]
        async fn rate_limit_increments_metric() {
            let metrics = Arc::new(MetricsCollector::new());
            let (app, store, _rx) = make_rate_limited_app(1, Some(Arc::clone(&metrics))).await;
            let key = store.create_api_key(None, None).await.unwrap();

            app.clone()
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            let resp = app
                .oneshot(bearer_request(&valid_body(), &key.key))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(key_count(&metrics.ingress_rate_limited, &key.id), 1);
        }

        #[tokio::test]
        async fn rate_limit_is_scoped_per_key() {
            let (app, store, _rx) = make_rate_limited_app(1, None).await;
            let key_a = store.create_api_key(None, None).await.unwrap();
            let key_b = store.create_api_key(None, None).await.unwrap();

            // Exhaust key A's budget.
            app.clone()
                .oneshot(bearer_request(&valid_body(), &key_a.key))
                .await
                .unwrap();
            let a_second = app
                .clone()
                .oneshot(bearer_request(&valid_body(), &key_a.key))
                .await
                .unwrap();
            assert_eq!(a_second.status(), StatusCode::TOO_MANY_REQUESTS);

            // Key B has its own independent budget and is unaffected.
            let b_first = app
                .oneshot(bearer_request(&valid_body(), &key_b.key))
                .await
                .unwrap();
            assert_eq!(b_first.status(), StatusCode::ACCEPTED);
        }

        #[tokio::test]
        async fn rate_limit_override_widens_a_key_budget() {
            // Global default is one per minute, but this key is issued a generous override, so a
            // burst that would 429 a default key is accepted.
            let (app, store, _rx) = make_rate_limited_app(1, None).await;
            let key = store
                .create_api_key_with_rpm(None, None, Some(600))
                .await
                .unwrap();

            for i in 0..5 {
                let resp = app
                    .clone()
                    .oneshot(bearer_request(&valid_body_with_index(i), &key.key))
                    .await
                    .unwrap();
                assert_eq!(
                    resp.status(),
                    StatusCode::ACCEPTED,
                    "a key with a high rpm override should not be limited at the default rate"
                );
            }
        }

        // -- status polling tests (GET /tasks/{id}, GET /tasks) --

        fn get_request(uri: &str, token: Option<&str>) -> Request<Body> {
            let mut builder = Request::builder().method(Method::GET).uri(uri);
            if let Some(token) = token {
                builder = builder.header("Authorization", format!("Bearer {token}"));
            }
            builder.body(Body::empty()).unwrap()
        }

        async fn task_view(resp: axum::response::Response) -> TaskView {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).expect("task view should deserialize")
        }

        async fn task_views(resp: axum::response::Response) -> Vec<TaskView> {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).expect("task list should deserialize")
        }

        /// Persists a task for `key_id` directly through the store, returning its id.
        async fn seed_task(store: &SqliteStore, key_id: &str) -> String {
            store
                .create_task(key_id, &valid_request().body)
                .await
                .expect("seeding a task should succeed")
                .id
        }

        #[tokio::test]
        async fn get_task_returns_owner_task() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let id = seed_task(&store, &key.id).await;

            let resp = app
                .oneshot(get_request(&format!("/tasks/{id}"), Some(&key.key)))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let view = task_view(resp).await;
            assert_eq!(view.task_id, id);
            assert_eq!(view.status, TaskStatus::Queued);
            assert!(view.payload.is_none());
            assert!(view.error.is_none());
            assert!(view.created_at > 0);
        }

        #[tokio::test]
        async fn get_task_ready_includes_payload() {
            use alloy_primitives::Bytes;
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let id = seed_task(&store, &key.id).await;

            let payload = PayloadView {
                to: Address::from([0x11; 20]),
                data: Bytes::from(vec![0x93, 0xde, 0x45, 0x31]),
                value: U256::ZERO,
                chain_id: 31337,
                estimated_gas: 234_000,
                valid_until_block: 22_345_678,
            };
            // The store-less test harness has no providers, so the freshness check is skipped and
            // the stored payload is returned as-is (the bundle is not consulted here).
            store
                .mark_task_ready_with_bundle(&id, &serde_json::to_string(&payload).unwrap(), "{}")
                .await
                .unwrap();

            let view = task_view(
                app.oneshot(get_request(&format!("/tasks/{id}"), Some(&key.key)))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(view.status, TaskStatus::Ready);
            // The payload comes back as a structured object, not a string.
            assert_eq!(
                view.payload.expect("ready task should carry a payload"),
                payload
            );
        }

        // -- payload freshness (stale re-request) check --

        fn fresh_payload(valid_until_block: u64) -> PayloadView {
            PayloadView {
                to: Address::from([0x11; 20]),
                data: alloy_primitives::Bytes::new(),
                value: U256::ZERO,
                chain_id: 31337,
                estimated_gas: 21_000,
                valid_until_block,
            }
        }

        fn fresh_bundle(transition_index: u64, valid_until_block: u64) -> TaskBundle {
            TaskBundle {
                msg_hash: alloy_primitives::B256::ZERO,
                reference_block_number: 10,
                transition_index,
                target_address: Address::from([0x11; 20]),
                target_function: alloy_primitives::FixedBytes::<4>::ZERO,
                storage_updates: alloy_primitives::Bytes::new(),
                chain_id: 31337,
                value: U256::ZERO,
                valid_until_block,
                proof: gas_killer_common::BundleProof::Bls {
                    quorum_numbers: alloy_primitives::Bytes::new(),
                    non_signer_stakes_and_signature: alloy_primitives::Bytes::new(),
                },
            }
        }

        /// In-memory store holding a single `ready` task carrying the given payload and bundle.
        async fn ready_store_task(
            payload: &PayloadView,
            bundle: &TaskBundle,
        ) -> (SqliteStore, String) {
            let store = SqliteStore::connect_in_memory().await.unwrap();
            let key = store.create_api_key(None, None).await.unwrap();
            let task = store
                .create_task(&key.id, &valid_request().body)
                .await
                .unwrap();
            store
                .mark_task_ready_with_bundle(
                    &task.id,
                    &serde_json::to_string(payload).unwrap(),
                    &serde_json::to_string(bundle).unwrap(),
                )
                .await
                .unwrap();
            (store, task.id)
        }

        #[tokio::test]
        async fn stale_check_serves_payload_in_window_with_matching_index() {
            use alloy::sol_types::SolValue;
            use alloy_primitives::{Bytes, U64};
            use alloy_provider::{ProviderBuilder, mock::Asserter};

            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            let asserter = Asserter::new();
            asserter.push_success(&U64::from(90u64)); // block-window: L1 eth_blockNumber, within window
            asserter.push_success(&Bytes::from(vec![0x60, 0x00])); // detect_contract_chain: target code
            asserter.push_success(&Bytes::from(U256::from(3u64).abi_encode())); // stateTransitionCount == index

            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);
            let freshness = FreshnessCache::default();

            let result =
                render_or_reject_payload(&providers, &freshness, None, &store, &task).await;
            assert_eq!(result.unwrap(), Some(payload));
        }

        #[tokio::test]
        async fn stale_check_reuses_cache_across_repeat_polls() {
            use alloy::sol_types::SolValue;
            use alloy_primitives::{Bytes, U64};
            use alloy_provider::{ProviderBuilder, mock::Asserter};

            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            // Exactly one poll's worth of responses: block number, chain-detection code, and
            // transition count. A second poll that issued any RPC would drain the empty asserter
            // and fail, so the two passing calls prove every read — including chain detection — is
            // served from the freshness cache.
            let asserter = Asserter::new();
            asserter.push_success(&U64::from(90u64));
            asserter.push_success(&Bytes::from(vec![0x60, 0x00]));
            asserter.push_success(&Bytes::from(U256::from(3u64).abi_encode()));

            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);
            let freshness = FreshnessCache::default();

            let first = render_or_reject_payload(&providers, &freshness, None, &store, &task).await;
            assert_eq!(first.unwrap(), Some(payload.clone()));
            let second =
                render_or_reject_payload(&providers, &freshness, None, &store, &task).await;
            assert_eq!(second.unwrap(), Some(payload));
        }

        #[tokio::test]
        async fn stale_check_rejects_when_block_past_valid_until() {
            use alloy_primitives::U64;
            use alloy_provider::{ProviderBuilder, mock::Asserter};

            let payload = fresh_payload(50);
            let bundle = fresh_bundle(3, 50);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            let asserter = Asserter::new();
            asserter.push_success(&U64::from(51u64)); // block-window: L1 block past valid_until_block (50)
            // Nothing else is queued: the block-window check short-circuits before detecting the
            // target chain or reading stateTransitionCount, so any further RPC would drain the
            // empty asserter and fail the test.

            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);
            let freshness = FreshnessCache::default();

            let err = render_or_reject_payload(&providers, &freshness, None, &store, &task)
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::PayloadExpired);
            assert_eq!(err.status, StatusCode::CONFLICT);

            // The task is now recorded expired so later polls short-circuit without a chain read.
            let settled = store.get_task(&id).await.unwrap().unwrap();
            assert_eq!(settled.status, TaskStatus::Expired);
        }

        #[tokio::test]
        async fn stale_check_rejects_on_transition_mismatch() {
            use alloy::sol_types::SolValue;
            use alloy_primitives::{Bytes, U64};
            use alloy_provider::{ProviderBuilder, mock::Asserter};

            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            let asserter = Asserter::new();
            asserter.push_success(&U64::from(40u64)); // block-window: L1 block within window
            asserter.push_success(&Bytes::from(vec![0x60, 0x00])); // detect_contract_chain: target code
            asserter.push_success(&Bytes::from(U256::from(9u64).abi_encode())); // stateTransitionCount 9 != index 3

            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);
            let freshness = FreshnessCache::default();

            let err = render_or_reject_payload(&providers, &freshness, None, &store, &task)
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::PayloadExpired);
            let settled = store.get_task(&id).await.unwrap().unwrap();
            assert_eq!(settled.status, TaskStatus::Expired);
        }

        #[tokio::test]
        async fn stale_check_maps_rpc_error_to_503() {
            use alloy_provider::{ProviderBuilder, mock::Asserter};

            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            let asserter = Asserter::new();
            asserter.push_failure_msg("rpc down"); // block-window: L1 get_block_number fails

            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);
            let freshness = FreshnessCache::default();

            let err = render_or_reject_payload(&providers, &freshness, None, &store, &task)
                .await
                .unwrap_err();
            assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(err.code, ErrorCode::RpcUnavailable);
            // A transient RPC failure must not expire the task.
            let settled = store.get_task(&id).await.unwrap().unwrap();
            assert_eq!(settled.status, TaskStatus::Ready);
        }

        #[tokio::test]
        async fn stale_check_skipped_without_providers() {
            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            let task = store.get_task(&id).await.unwrap().unwrap();

            let providers: HashMap<ChainRole, ReadOnlyProvider> = HashMap::new();
            let freshness = FreshnessCache::default();
            let result =
                render_or_reject_payload(&providers, &freshness, None, &store, &task).await;
            assert_eq!(result.unwrap(), Some(payload));
        }

        #[tokio::test]
        async fn stale_check_returns_none_for_non_ready_task() {
            let store = SqliteStore::connect_in_memory().await.unwrap();
            let key = store.create_api_key(None, None).await.unwrap();
            let task = store
                .create_task(&key.id, &valid_request().body)
                .await
                .unwrap();

            let providers: HashMap<ChainRole, ReadOnlyProvider> = HashMap::new();
            let freshness = FreshnessCache::default();
            let result =
                render_or_reject_payload(&providers, &freshness, None, &store, &task).await;
            assert_eq!(result.unwrap(), None);
        }

        #[tokio::test]
        async fn stale_check_short_circuits_when_already_expired() {
            let payload = fresh_payload(100);
            let bundle = fresh_bundle(3, 100);
            let (store, id) = ready_store_task(&payload, &bundle).await;
            store
                .mark_task_expired(&id, "payload valid_until_block passed; re-request")
                .await
                .unwrap();
            let task = store.get_task(&id).await.unwrap().unwrap();

            // Even with providers configured, an already-expired task never touches the chain.
            let providers: HashMap<ChainRole, ReadOnlyProvider> = HashMap::new();
            let freshness = FreshnessCache::default();
            let err = render_or_reject_payload(&providers, &freshness, None, &store, &task)
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::PayloadExpired);
            assert_eq!(err.status, StatusCode::CONFLICT);
        }

        #[tokio::test]
        async fn get_task_failed_includes_error() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let id = seed_task(&store, &key.id).await;
            store
                .mark_task_failed(&id, "aggregation timed out")
                .await
                .unwrap();

            let view = task_view(
                app.oneshot(get_request(&format!("/tasks/{id}"), Some(&key.key)))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(view.status, TaskStatus::Failed);
            assert_eq!(view.error.as_deref(), Some("aggregation timed out"));
        }

        #[tokio::test]
        async fn get_unknown_task_returns_404() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let resp = app
                .oneshot(get_request("/tasks/does-not-exist", Some(&key.key)))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::NotFound);
        }

        #[tokio::test]
        async fn get_task_owned_by_other_key_returns_403() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let owner = store.create_api_key(None, None).await.unwrap();
            let other = store.create_api_key(None, None).await.unwrap();
            let id = seed_task(&store, &owner.id).await;

            let resp = app
                .oneshot(get_request(&format!("/tasks/{id}"), Some(&other.key)))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::Forbidden);
        }

        #[tokio::test]
        async fn get_task_without_token_returns_401() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let id = seed_task(&store, &key.id).await;
            let resp = app
                .oneshot(get_request(&format!("/tasks/{id}"), None))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn list_tasks_is_scoped_to_key_newest_first() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key_a = store.create_api_key(None, None).await.unwrap();
            let key_b = store.create_api_key(None, None).await.unwrap();
            seed_task(&store, &key_a.id).await;
            seed_task(&store, &key_a.id).await;
            seed_task(&store, &key_b.id).await;

            let resp = app
                .oneshot(get_request("/tasks", Some(&key_a.key)))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let views = task_views(resp).await;
            assert_eq!(views.len(), 2, "list must be scoped to the calling key");
            assert!(
                views.windows(2).all(|w| w[0].created_at >= w[1].created_at),
                "tasks must be newest first"
            );
        }

        #[tokio::test]
        async fn list_tasks_filters_by_status() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let ready = seed_task(&store, &key.id).await;
            seed_task(&store, &key.id).await;
            store.mark_task_ready(&ready, "0x00").await.unwrap();

            let views = task_views(
                app.oneshot(get_request("/tasks?status=ready", Some(&key.key)))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(views.len(), 1);
            assert_eq!(views[0].task_id, ready);
            assert_eq!(views[0].status, TaskStatus::Ready);
        }

        #[tokio::test]
        async fn list_tasks_paginates() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            for _ in 0..3 {
                seed_task(&store, &key.id).await;
            }

            let first = task_views(
                app.clone()
                    .oneshot(get_request("/tasks?limit=2", Some(&key.key)))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(first.len(), 2);

            let second = task_views(
                app.oneshot(get_request("/tasks?limit=2&offset=2", Some(&key.key)))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(second.len(), 1);
        }

        #[tokio::test]
        async fn list_tasks_rejects_bad_status_with_envelope() {
            let (app, store, _rx) = make_app_with_store(None).await;
            let key = store.create_api_key(None, None).await.unwrap();
            let resp = app
                .oneshot(get_request("/tasks?status=bogus", Some(&key.key)))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let body = error_envelope(resp).await;
            assert_eq!(body.error.code, crate::error::ErrorCode::InvalidRequest);
        }

        #[tokio::test]
        async fn list_tasks_without_token_returns_401() {
            let (app, _store, _rx) = make_app_with_store(None).await;
            let resp = app.oneshot(get_request("/tasks", None)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }
    }

    // -- transition_index deserialization tests --

    mod transition_index_deser {
        use crate::ingress::GasKillerTaskRequestBody;

        fn deser(json: &str) -> Result<Option<u64>, serde_json::Error> {
            let body: GasKillerTaskRequestBody = serde_json::from_str(json)?;
            Ok(body.transition_index)
        }

        fn body_with(transition_index_json: &str) -> String {
            format!(
                r#"{{"target_address":"0x0000000000000000000000000000000000000001","call_data":[1,2,3,4],"from_address":"0x0000000000000000000000000000000000000002","value":"0x0","block_height":1,"transition_index":{}}}"#,
                transition_index_json
            )
        }

        #[test]
        fn test_numeric_gives_some() {
            assert_eq!(deser(&body_with("42")).unwrap(), Some(42));
        }

        #[test]
        fn test_zero_gives_some_zero() {
            assert_eq!(deser(&body_with("0")).unwrap(), Some(0));
        }

        #[test]
        fn test_null_gives_none() {
            assert_eq!(deser(&body_with("null")).unwrap(), None);
        }

        #[test]
        fn test_auto_string_gives_none() {
            assert_eq!(deser(&body_with(r#""auto""#)).unwrap(), None);
        }

        #[test]
        fn test_missing_field_gives_none() {
            let json = r#"{"target_address":"0x0000000000000000000000000000000000000001","call_data":[1,2,3,4],"from_address":"0x0000000000000000000000000000000000000002","value":"0x0","block_height":1}"#;
            assert_eq!(deser(json).unwrap(), None);
        }

        #[test]
        fn test_unknown_string_is_rejected() {
            assert!(deser(&body_with(r#""foo""#)).is_err());
        }

        #[test]
        fn test_empty_string_is_rejected() {
            assert!(deser(&body_with(r#""""#)).is_err());
        }

        #[test]
        fn test_negative_integer_is_rejected() {
            assert!(deser(&body_with("-1")).is_err());
        }

        #[test]
        fn test_boolean_is_rejected() {
            assert!(deser(&body_with("true")).is_err());
        }
    }

    // -- onchain validation unit tests --
    //
    // These tests exercise validate_onchain / detect_contract_chain directly using
    // alloy's built-in mock transport (alloy_provider::mock::Asserter).  No live
    // chain or forked node is required; responses are queued FIFO and consumed by
    // each RPC call in order:
    //   1. eth_getCode        (detect_contract_chain, at latest)
    //   2. eth_blockNumber    (block-height check)
    //   3. eth_getCode        (target code at the requested block_height)
    //   4. eth_call           (stateTransitionCount view call)

    mod onchain {
        use super::*;
        use alloy::sol_types::SolValue;
        use alloy_primitives::{Bytes, U64};
        use alloy_provider::{ProviderBuilder, mock::Asserter};

        fn mock_provider() -> (impl Provider + Clone, Asserter) {
            let asserter = Asserter::new();
            let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
            (provider, asserter)
        }

        fn valid_body() -> GasKillerTaskRequestBody {
            GasKillerTaskRequestBody {
                target_address: "0x0000000000000000000000000000000000000001"
                    .parse()
                    .unwrap(),
                from_address: "0x0000000000000000000000000000000000000002"
                    .parse()
                    .unwrap(),
                call_data: vec![0xAB, 0xCD, 0xEF, 0x01],
                transition_index: Some(5),
                value: U256::ZERO,
                block_height: 50,
            }
        }

        fn push_code_exists(asserter: &Asserter) {
            asserter.push_success(&Bytes::from(vec![0x60u8]));
        }

        fn push_code_empty(asserter: &Asserter) {
            asserter.push_success(&Bytes::new());
        }

        fn push_block_number(asserter: &Asserter, n: u64) {
            asserter.push_success(&U64::from(n));
        }

        fn push_state_transition_count(asserter: &Asserter, count: u64) {
            asserter.push_success(&Bytes::from(U256::from(count).abi_encode()));
        }

        #[tokio::test]
        async fn test_contract_not_found() {
            let (provider, asserter) = mock_provider();
            push_code_empty(&asserter);

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::ContractNotFound),
                "expected ContractNotFound, got {err}"
            );
        }

        #[tokio::test]
        async fn test_block_height_in_future() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 40); // chain is at 40, request wants 50

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    OnchainValidationError::BlockHeightInFuture {
                        provided: 50,
                        current: 40
                    }
                ),
                "expected BlockHeightInFuture, got {err}"
            );
        }

        /// The configured admission window, which every staleness assertion is measured against.
        /// The check is skipped entirely when it is disabled, so a test asserting a rejection has
        /// nothing to assert in that configuration.
        fn admission_window() -> u64 {
            gas_killer_common::ingress_staleness_window()
                .window()
                .expect("admission window must be enabled for the staleness tests")
        }

        #[tokio::test]
        async fn test_block_height_too_stale() {
            let body = valid_body(); // block_height = 50
            let window = admission_window();
            // head one block past the admission window → age = window + 1, rejected
            let head = body.block_height + window + 1;

            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, head);

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &body).await.unwrap_err();
            assert!(
                matches!(
                    err,
                    OnchainValidationError::BlockHeightTooStale { provided: 50, max_age, .. }
                        if max_age == window
                ),
                "expected BlockHeightTooStale reporting the admission window, got {err}"
            );
            // The rejection is what a client sees: a 400 naming the window it breached.
            let api_err = ApiError::from(err);
            assert_eq!(api_err.status, StatusCode::BAD_REQUEST);
            assert_eq!(api_err.code, ErrorCode::StaleBlock);
            assert!(
                api_err.message.contains(&window.to_string()),
                "the message should name the window: {}",
                api_err.message
            );
        }

        #[tokio::test]
        async fn test_block_height_at_staleness_boundary_passes() {
            let body = valid_body(); // block_height = 50, transition_index = Some(5)
            let window = admission_window();
            // head exactly at the window edge → age == window, still valid
            let head = body.block_height + window;

            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, head);
            push_code_exists(&asserter); // deployed at the requested block_height
            push_state_transition_count(&asserter, 5); // matches transition_index → passes

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            assert!(
                validate_onchain(&providers, None, &body).await.is_ok(),
                "age == admission window should be accepted"
            );
        }

        /// Admission is deliberately tighter than the contract's own staleness window: it holds
        /// back the payload buffer so a task it accepts still has room to aggregate and render a
        /// submittable payload. An analysis at the contract's edge is therefore rejected.
        #[tokio::test]
        async fn test_admission_window_is_tighter_than_the_contract_window() {
            let window = admission_window();
            let measure = gas_killer_common::block_stale_measure();
            assert!(
                window < measure,
                "this test describes the derived default, which holds the payload buffer back \
                 from the contract's window; INGRESS_STALENESS_WINDOW_BLOCKS resolves to {window} \
                 against a staleness measure of {measure} in this environment"
            );

            let body = valid_body(); // block_height = 50
            let head = body.block_height + measure;

            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, head);

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &body).await.unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::BlockHeightTooStale { .. }),
                "an analysis at the contract's staleness edge leaves no room to finish, got {err}"
            );
        }

        /// A `block_height` that predates the target's deployment is refused even though the
        /// address holds code now: chain detection sees the deployed contract at `latest`, and
        /// only the probe at the requested height sees the empty account the analysis would have
        /// traced into.
        #[tokio::test]
        async fn test_target_not_deployed_at_requested_block() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter); // detection at latest finds the deployed target
            push_block_number(&asserter, 100);
            push_code_empty(&asserter); // nothing was deployed at the requested block 50

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    OnchainValidationError::TargetNotDeployedAtBlock {
                        provided: 50,
                        current: 100
                    }
                ),
                "expected TargetNotDeployedAtBlock, got {err}"
            );
            // The code is distinct from CONTRACT_NOT_FOUND so a client can tell a wrong address
            // from a block_height behind the target's deployment.
            let api_err = ApiError::from(err);
            assert_eq!(api_err.status, StatusCode::BAD_REQUEST);
            assert_eq!(api_err.code, ErrorCode::TargetNotDeployed);
        }

        /// A provider that cannot serve state at the requested height leaves the request neither
        /// proven good nor proven bad, so it is a transient failure rather than a rejection.
        #[tokio::test]
        async fn test_rpc_error_on_historical_get_code() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100);
            asserter.push_failure_msg("missing trie node");

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::RpcError(_)),
                "expected RpcError, got {err}"
            );
            assert_eq!(
                ApiError::from(err).status,
                StatusCode::SERVICE_UNAVAILABLE,
                "an unanswerable probe is a 503, not a client error"
            );
        }

        #[tokio::test]
        async fn test_transition_index_behind() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100); // chain at 100, request wants 50 ✓
            push_code_exists(&asserter); // deployed at the requested block_height
            push_state_transition_count(&asserter, 10); // contract at 10, request provides 5 ✗

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    OnchainValidationError::TransitionIndexMismatch {
                        provided: 5,
                        current: 10
                    }
                ),
                "expected TransitionIndexMismatch, got {err}"
            );
        }

        #[tokio::test]
        async fn test_valid_onchain_state_passes() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100); // chain at 100 >= request 50 ✓
            push_code_exists(&asserter); // deployed at the requested block_height
            push_state_transition_count(&asserter, 5); // contract at 5, request at 5 ✓

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            validate_onchain(&providers, None, &valid_body())
                .await
                .expect("valid onchain state should pass");
        }

        #[tokio::test]
        async fn test_transition_index_ahead() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100);
            push_code_exists(&asserter); // deployed at the requested block_height
            push_state_transition_count(&asserter, 3); // contract at 3, request at 5 → ahead ✗

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    OnchainValidationError::TransitionIndexMismatch {
                        provided: 5,
                        current: 3
                    }
                ),
                "expected TransitionIndexMismatch, got {err}"
            );
        }

        #[tokio::test]
        async fn test_rpc_error_on_get_code_treated_as_rpc_error() {
            let (provider, asserter) = mock_provider();
            asserter.push_failure_msg("connection refused");

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::RpcError(_)),
                "expected RpcError, got {err}"
            );
        }

        #[tokio::test]
        async fn test_rpc_error_on_block_number() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            asserter.push_failure_msg("node overloaded");

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::RpcError(_)),
                "expected RpcError, got {err}"
            );
        }

        #[tokio::test]
        async fn test_rpc_error_on_state_transition_count() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100);
            push_code_exists(&asserter); // deployed at the requested block_height
            asserter.push_failure_msg("call reverted");

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::RpcError(_)),
                "expected RpcError, got {err}"
            );
        }

        #[tokio::test]
        async fn test_auto_transition_index_skips_count_check() {
            let (provider, asserter) = mock_provider();
            push_code_exists(&asserter);
            push_block_number(&asserter, 100);
            push_code_exists(&asserter); // deployed at the requested block_height
            // No push_state_transition_count — the mock asserter would fail if it were called.

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, provider);

            let mut body = valid_body();
            body.transition_index = None;

            validate_onchain(&providers, None, &body)
                .await
                .expect("auto transition_index should skip count check and pass");
        }

        #[tokio::test]
        async fn test_l2_fallback_when_l1_has_no_code() {
            let (l1_provider, l1_asserter) = mock_provider();
            let (l2_provider, l2_asserter) = mock_provider();

            push_code_empty(&l1_asserter); // L1 has no code
            push_code_exists(&l2_asserter); // L2 does
            push_block_number(&l2_asserter, 100);
            push_code_exists(&l2_asserter); // deployed at the requested block_height
            push_state_transition_count(&l2_asserter, 5);

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, l1_provider);
            providers.insert(ChainRole::L2, l2_provider);

            validate_onchain(&providers, None, &valid_body())
                .await
                .expect("should find contract on L2");
        }

        #[tokio::test]
        async fn test_not_found_when_all_chains_empty() {
            let (l1_provider, l1_asserter) = mock_provider();
            let (l2_provider, l2_asserter) = mock_provider();

            push_code_empty(&l1_asserter);
            push_code_empty(&l2_asserter);

            let mut providers = HashMap::new();
            providers.insert(ChainRole::L1, l1_provider);
            providers.insert(ChainRole::L2, l2_provider);

            let err = validate_onchain(&providers, None, &valid_body())
                .await
                .unwrap_err();
            assert!(
                matches!(err, OnchainValidationError::ContractNotFound),
                "expected ContractNotFound, got {err}"
            );
        }
    }

    // -- error envelope mapping --
    //
    // Locks the status code and machine-readable ErrorCode each validation error maps to,
    // including that transient RPC failures are sanitized so internal detail never reaches
    // the client.

    mod error_mapping {
        use super::*;
        use crate::error::ErrorCode;

        #[test]
        fn validation_errors_map_to_code_and_400() {
            let cases = [
                (
                    ValidationError::ZeroTargetAddress,
                    ErrorCode::InvalidAddress,
                ),
                (ValidationError::ZeroFromAddress, ErrorCode::InvalidAddress),
                (ValidationError::EmptyCallData, ErrorCode::InvalidRequest),
                (
                    ValidationError::CallDataTooShort { len: 2 },
                    ErrorCode::InvalidRequest,
                ),
                (
                    ValidationError::CallDataTooLarge { len: 1, max: 0 },
                    ErrorCode::CalldataTooLarge,
                ),
                (ValidationError::ZeroBlockHeight, ErrorCode::InvalidRequest),
            ];
            for (err, code) in cases {
                let api = ApiError::from(err);
                assert_eq!(api.status, StatusCode::BAD_REQUEST);
                assert_eq!(api.code, code);
            }
        }

        #[test]
        fn onchain_errors_map_to_code_and_status() {
            let cases = [
                (
                    OnchainValidationError::ContractNotFound,
                    StatusCode::BAD_REQUEST,
                    ErrorCode::ContractNotFound,
                ),
                (
                    OnchainValidationError::TransitionIndexMismatch {
                        provided: 5,
                        current: 6,
                    },
                    StatusCode::BAD_REQUEST,
                    ErrorCode::TransitionMismatch,
                ),
                (
                    OnchainValidationError::BlockHeightInFuture {
                        provided: 10,
                        current: 9,
                    },
                    StatusCode::BAD_REQUEST,
                    ErrorCode::InvalidRequest,
                ),
                (
                    OnchainValidationError::BlockHeightTooStale {
                        provided: 1,
                        current: 500,
                        max_age: 300,
                    },
                    StatusCode::BAD_REQUEST,
                    ErrorCode::StaleBlock,
                ),
                (
                    OnchainValidationError::TargetNotDeployedAtBlock {
                        provided: 100,
                        current: 500,
                    },
                    StatusCode::BAD_REQUEST,
                    ErrorCode::TargetNotDeployed,
                ),
            ];
            for (err, status, code) in cases {
                let api = ApiError::from(err);
                assert_eq!(api.status, status);
                assert_eq!(api.code, code);
            }
        }

        #[test]
        fn rpc_error_is_503_and_message_is_sanitized() {
            let api = ApiError::from(OnchainValidationError::RpcError(
                "connection refused at 10.0.0.5".to_string(),
            ));
            assert_eq!(api.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(api.code, ErrorCode::RpcUnavailable);
            assert_eq!(api.message, "Service temporarily unavailable");
            assert!(
                !api.message.contains("10.0.0.5"),
                "internal RPC detail must not leak to clients"
            );
        }
    }
}
