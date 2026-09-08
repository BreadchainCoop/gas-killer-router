use alloy_provider::Provider;
use anyhow::Result;
use commonware_cryptography::sha256::Digest;
use commonware_runtime::telemetry::metrics::encoding::text::encode;
use commonware_runtime::telemetry::metrics::raw::{Counter, Family, Histogram};
use commonware_runtime::telemetry::metrics::registry::Registry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use alloy_primitives::{Address, U256};

use crate::ReadOnlyProvider;
use crate::config::{ChainRole, SpeculativePrebuildConfig};
use crate::task_data::GasKillerTaskData;

use alloy::rpc::types::TransactionRequest;

/// Key identifying a task's expected digest in [`GasKillerValidator`]'s digest cache.
///
/// The digest is `sha256(abi.encode(transition_index, target_address, selector,
/// storage_updates))`, and `storage_updates` is derived by EVMSketch from
/// `(target_address, call_data, from_address, value, block_height)`. `transition_index`
/// is a *per-contract* counter, so keying on `(transition_index, block_height)` alone
/// would collide two tasks for *different* contracts that share the same index and
/// block — returning the wrong contract's digest. The key covers every field the
/// digest depends on.
type DigestCacheKey = (u64, u64, Address, Address, U256, Vec<u8>);

fn digest_cache_key(task: &GasKillerTaskData) -> DigestCacheKey {
    (
        task.transition_index,
        task.block_height,
        task.target_address,
        task.from_address,
        task.value,
        task.call_data.clone(),
    )
}
use gas_analyzer::{
    EncodePhaseTimings, EvmSketchExecutorCache, Extraction,
    call_to_encoded_state_updates_with_evmsketch_profiled,
};

/// Label set scoping a phase histogram to the extractor that ran, rendered as
/// `extraction="prestate_net"`.
type ExtractionLabels = [(&'static str, String); 1];

/// A phase-duration histogram broken down by extractor. Cardinality is the three
/// `gas_analyzer::Extraction` variants.
pub type PerExtractionHistogram = Family<ExtractionLabels, Histogram>;

/// The label set naming `extraction`.
fn extraction_labels(extraction: Extraction) -> ExtractionLabels {
    [("extraction", extraction.as_str().to_string())]
}

/// Label set scoping a counter to a cache outcome, rendered as `result="hit"`.
type CacheResultLabels = [(&'static str, String); 1];

/// A counter broken down by cache outcome. Cardinality is two.
pub type PerCacheResultCounter = Family<CacheResultLabels, Counter<u64, AtomicU64>>;

/// The label set naming `result`, for a cache hit or miss.
fn cache_result_labels(hit: bool) -> CacheResultLabels {
    [("result", if hit { "hit" } else { "miss" }.to_string())]
}

// Per-phase bucket constructors. A `Family` of histograms needs a plain `fn` to build each new
// series (`Histogram` has no meaningful default), and each phase gets its own buckets because
// they span very different scales — one shared set would leave most samples in a single bucket.

/// Network plus remote node CPU, and a struct-log trace can be enormous.
fn trace_fetch_buckets() -> Histogram {
    Histogram::new([0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0, 120.0])
}

/// Local CPU, `O(execution steps)`.
fn parse_buckets() -> Histogram {
    Histogram::new([
        0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0,
    ])
}

/// Tens of milliseconds on a miss, near zero on a hit — so the low end needs resolution.
fn executor_build_buckets() -> Histogram {
    Histogram::new([0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0])
}

/// One `eth_getProof` round-trip per hinted address.
fn state_prefetch_buckets() -> Histogram {
    Histogram::new([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0])
}

/// Local revm, plus any cold-miss state reads the prefetch did not cover.
fn revm_estimate_buckets() -> Histogram {
    Histogram::new([0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0])
}

/// Prometheus metrics for validator timing, exposed on the /metrics endpoint of whichever
/// binary owns the validator.
///
/// Both the router and the operators run gas analysis, and both attach one of these, so every
/// series here carries the same names on both sides and is separated by the scrape target.
///
/// The per-phase histograms exist because the total tells you a task was slow without telling
/// you what to do about it. The fetch phases are network plus *remote* node CPU; the parse and
/// estimate phases are local CPU with no yield points. Which dominates for a given workload
/// decides whether to buy bandwidth or cores, and whether `prestate-net` is worth defaulting to.
pub struct ValidatorMetrics {
    registry: Registry,
    /// Duration of the EVMSketch gas-analysis call (cache-miss path only).
    pub evmsketch_duration_seconds: Histogram,
    /// Time awaiting trace RPCs: `debug_traceCall` on the struct-log path, or the two cheap
    /// tracers on the prestate path.
    pub trace_fetch_seconds: PerExtractionHistogram,
    /// Time turning struct logs into state updates. Never observed on the net form, which
    /// fetches no struct-log trace, so an empty `prestate_net` series is the expected shape.
    pub parse_seconds: PerExtractionHistogram,
    /// Time building the revm executor. Read alongside
    /// [`Self::executor_cache`] — a hit makes this near zero.
    pub executor_build_seconds: PerExtractionHistogram,
    /// Time prefetching account and slot state for the gas estimate.
    pub state_prefetch_seconds: PerExtractionHistogram,
    /// Time executing the payload under revm to price it.
    pub revm_estimate_seconds: PerExtractionHistogram,
    /// Executor-cache outcomes. The speculative pre-build's whole purpose is to turn these into
    /// hits, so this is how its contribution becomes visible.
    pub executor_cache: PerCacheResultCounter,
    /// Digest-cache outcomes. A hit skips the entire analysis, so the phase histograms cannot be
    /// interpreted without knowing how often that happened.
    pub digest_cache: PerCacheResultCounter,
}

impl ValidatorMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let evmsketch_duration_seconds =
            Histogram::new([0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0, 120.0]);
        registry.register(
            "gas_killer_node_evmsketch_duration_seconds",
            "Duration of gas analysis (EVMSketch + RPC calls), cache-miss path only. Excludes chain detection.",
            evmsketch_duration_seconds.clone(),
        );

        let trace_fetch_seconds =
            Family::new_with_constructor(trace_fetch_buckets as fn() -> Histogram);
        registry.register(
            "gas_killer_evmsketch_trace_fetch_seconds",
            "Time awaiting trace RPCs during gas analysis, by extractor",
            trace_fetch_seconds.clone(),
        );

        let parse_seconds = Family::new_with_constructor(parse_buckets as fn() -> Histogram);
        registry.register(
            "gas_killer_evmsketch_parse_seconds",
            "Time parsing struct logs into state updates, by extractor",
            parse_seconds.clone(),
        );

        let executor_build_seconds =
            Family::new_with_constructor(executor_build_buckets as fn() -> Histogram);
        registry.register(
            "gas_killer_evmsketch_executor_build_seconds",
            "Time resolving the revm executor from the cache or building it, by extractor",
            executor_build_seconds.clone(),
        );

        let state_prefetch_seconds =
            Family::new_with_constructor(state_prefetch_buckets as fn() -> Histogram);
        registry.register(
            "gas_killer_evmsketch_state_prefetch_seconds",
            "Time prefetching account and slot state for the gas estimate, by extractor",
            state_prefetch_seconds.clone(),
        );

        let revm_estimate_seconds =
            Family::new_with_constructor(revm_estimate_buckets as fn() -> Histogram);
        registry.register(
            "gas_killer_evmsketch_revm_estimate_seconds",
            "Time executing the payload under revm to price it, by extractor",
            revm_estimate_seconds.clone(),
        );

        let executor_cache = Family::default();
        registry.register(
            "gas_killer_evmsketch_executor_cache",
            "Total executor-cache lookups by outcome",
            executor_cache.clone(),
        );

        let digest_cache = Family::default();
        registry.register(
            "gas_killer_evmsketch_digest_cache",
            "Total digest-cache lookups by outcome; a hit skips the whole analysis",
            digest_cache.clone(),
        );

        Self {
            registry,
            evmsketch_duration_seconds,
            trace_fetch_seconds,
            parse_seconds,
            executor_build_seconds,
            state_prefetch_seconds,
            revm_estimate_seconds,
            executor_cache,
            digest_cache,
        }
    }

    /// Records one analysis run's phase costs and cache outcome.
    ///
    /// Kept here rather than at the call site so the router and the operators cannot drift in
    /// what they observe.
    pub fn observe_analysis(&self, phases: &AnalysisPhases) {
        let labels = extraction_labels(phases.extraction);
        self.trace_fetch_seconds
            .get_or_create(&labels)
            .observe(phases.timings.trace_fetch.as_secs_f64());
        // The net form never fetches a struct-log trace, so recording a zero would invent a
        // data point for work that did not happen.
        if phases.extraction != Extraction::PrestateNet {
            self.parse_seconds
                .get_or_create(&labels)
                .observe(phases.timings.parse.as_secs_f64());
        }
        self.executor_build_seconds
            .get_or_create(&labels)
            .observe(phases.timings.executor_build.as_secs_f64());
        self.state_prefetch_seconds
            .get_or_create(&labels)
            .observe(phases.timings.prefetch.as_secs_f64());
        self.revm_estimate_seconds
            .get_or_create(&labels)
            .observe(phases.timings.revm_estimate.as_secs_f64());
        self.executor_cache
            .get_or_create(&cache_result_labels(phases.executor_cache_hit))
            .inc();
    }

    /// Records one digest-cache lookup.
    pub fn observe_digest_cache(&self, hit: bool) {
        self.digest_cache
            .get_or_create(&cache_result_labels(hit))
            .inc();
    }

    pub fn encode(&self) -> String {
        let mut output = String::new();
        encode(&mut output, &self.registry).expect("metrics encoding failed");
        output
    }
}

impl Default for ValidatorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Refuses an analysis whose program applies nothing.
///
/// Applying such a program is a no-op on the target's storage, yet it still costs gas and —
/// because `verifyAndUpdate` is itself tracked — still advances `stateTransitionCount`,
/// invalidating every other payload outstanding for that target. There is no reason to ask the
/// quorum to sign one.
///
/// The count comes from the analyzer, which reports it alongside the encoded program: emptiness
/// is not visible in the payload itself, since a program with no operations still encodes to a
/// fixed non-empty blob.
fn ensure_program_applies_something(
    update_count: usize,
    contract_address: Address,
    block_height: u64,
) -> Result<()> {
    if update_count == 0 {
        anyhow::bail!(
            "analysis produced no state updates: the call to {contract_address} at block \
             {block_height} changes nothing, so its payload would spend gas to apply nothing \
             while still advancing the target's transition count"
        );
    }
    Ok(())
}

/// What one gas-analysis run cost, and which extractor produced it.
///
/// `trace_fetch` and `executor_build` in [`Self::timings`] overlap — they are the two branches of
/// one `try_join!` inside the analyzer — so they must never be summed. See
/// [`gas_analyzer::EncodePhaseTimings`].
#[derive(Debug, Clone, Copy)]
pub struct AnalysisPhases {
    /// Which extractor produced the state-update program.
    pub extraction: Extraction,
    /// Whether the revm executor came from the cache instead of being built.
    pub executor_cache_hit: bool,
    /// What each phase of the run cost.
    pub timings: EncodePhaseTimings,
}

/// Result of gas analysis containing storage updates and gas information
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// The storage updates extracted from the transaction. Never a program that applies
    /// nothing: such an analysis is refused rather than returned.
    pub storage_updates: Vec<u8>,
    /// The gas estimate from gas-analyzer
    #[allow(dead_code)]
    pub gas_estimate: u64,
    /// The block height at which the analysis was performed
    pub block_height: u64,
    /// What the run cost, by phase.
    pub phases: AnalysisPhases,
}

/// Extra executor-cache slots per chain beyond the staleness window.
///
/// Covers on-demand entries (a freshly requested block not yet pre-built) without
/// evicting the speculative window.
const EXECUTOR_CACHE_SLACK_PER_CHAIN: usize = 4;

/// LRU capacity for the executor cache.
///
/// Sized to retain a full `BLOCK_STALE_MEASURE` window per chain so any in-window
/// `block_height` — whether pre-built by the speculative loop or requested on demand —
/// hits the cache. Entries are small (anchor header + provider handle, a few KB), so a
/// few-hundred-entry window costs single-digit MB.
fn executor_cache_capacity(num_chains: usize) -> usize {
    let per_chain = crate::config::block_stale_measure() as usize + EXECUTOR_CACHE_SLACK_PER_CHAIN;
    per_chain * num_chains.max(1)
}

/// Validator implementation for the gas killer use case with multi-chain support
#[derive(Clone)]
pub struct GasKillerValidator {
    /// RPC URLs per chain for the gas analyzer
    chain_rpc_urls: HashMap<ChainRole, String>,
    /// Read-only providers per chain for chain detection and `stateTransitionCount` reads.
    providers: Arc<HashMap<ChainRole, ReadOnlyProvider>>,
    /// Default chain for backwards compatibility
    default_chain: ChainRole,
    /// Cache: task identity ([`DigestCacheKey`]) -> computed digest.
    /// Prevents re-running expensive EVMSketch for the same task when the
    /// orchestrator validates multiple signatures for identical task data.
    digest_cache: Arc<Mutex<HashMap<DigestCacheKey, Digest>>>,
    /// LRU cache of pre-built EvmSketch executors keyed by (rpc_url, block_number).
    /// Eliminates the 2× eth_getBlockByNumber build cost (~80–120 ms) for the
    /// 2nd…Nth request at the same block height.
    executor_cache: Arc<EvmSketchExecutorCache>,
    /// Optional Prometheus metrics — injected on the node, absent on the router.
    validator_metrics: Option<Arc<ValidatorMetrics>>,
    /// Storage-update encoding. Must be identical on the node and router (it
    /// changes `storage_updates`, hence the digest); the production `new()` path
    /// reads it from `STATE_ENCODING` on both binaries. See
    /// [`crate::config::state_encoding`].
    state_encoding: gas_analyzer::StateEncoding,
    /// Gas limits the tracked function is simulated under. Like `state_encoding` it must be
    /// identical on the node and router (it changes the derived `storage_updates`, hence the
    /// digest); the production `new()` path reads it from `GK_SIM_PROFILE` on both binaries.
    /// See [`crate::config::sim_profile`].
    sim_profile: gas_analyzer::SimProfile,
}

impl GasKillerValidator {
    /// Creates a new GasKillerValidator with multi-chain support.
    ///
    /// Reads RPC URLs from environment variables:
    /// - `HTTP_RPC` for L1 (required)
    /// - `L2_HTTP_RPC` for L2 (optional)
    ///
    /// Returns an error if L1 RPC is not set.
    pub fn new() -> Result<Self> {
        let chain_rpc_urls = crate::chain_rpc_urls_from_env()?;
        let capacity = executor_cache_capacity(chain_rpc_urls.len());
        let providers = Arc::new(crate::build_read_providers(&chain_rpc_urls));
        if !providers.contains_key(&ChainRole::L1) {
            anyhow::bail!("HTTP_RPC is set but is not a valid URL");
        }

        Ok(Self {
            chain_rpc_urls,
            providers,
            default_chain: ChainRole::L1,
            digest_cache: Arc::new(Mutex::new(HashMap::new())),
            executor_cache: Arc::new(EvmSketchExecutorCache::new(capacity)),
            validator_metrics: None,
            // Production path: node and router both call `new()`, so reading the
            // env here keeps their encoding (and therefore their digests) in sync.
            state_encoding: crate::config::state_encoding(),
            sim_profile: crate::config::sim_profile(),
        })
    }

    /// Creates a new GasKillerValidator with a specific RPC URL (for default chain).
    ///
    /// Useful for testing without modifying environment variables.
    pub fn with_rpc_url(rpc_url: impl Into<String>) -> Self {
        let mut chain_rpc_urls = HashMap::new();
        chain_rpc_urls.insert(ChainRole::L1, rpc_url.into());
        let capacity = executor_cache_capacity(chain_rpc_urls.len());
        let providers = Arc::new(crate::build_read_providers(&chain_rpc_urls));
        Self {
            chain_rpc_urls,
            providers,
            default_chain: ChainRole::L1,
            digest_cache: Arc::new(Mutex::new(HashMap::new())),
            executor_cache: Arc::new(EvmSketchExecutorCache::new(capacity)),
            validator_metrics: None,
            state_encoding: gas_analyzer::StateEncoding::Legacy,
            sim_profile: gas_analyzer::SimProfile::Chain,
        }
    }

    /// Creates a new GasKillerValidator with RPC URLs for multiple chains.
    pub fn with_chain_rpc_urls(chain_rpc_urls: HashMap<ChainRole, String>) -> Self {
        let capacity = executor_cache_capacity(chain_rpc_urls.len());
        let providers = Arc::new(crate::build_read_providers(&chain_rpc_urls));
        Self {
            chain_rpc_urls,
            providers,
            default_chain: ChainRole::L1,
            digest_cache: Arc::new(Mutex::new(HashMap::new())),
            executor_cache: Arc::new(EvmSketchExecutorCache::new(capacity)),
            validator_metrics: None,
            state_encoding: gas_analyzer::StateEncoding::Legacy,
            sim_profile: gas_analyzer::SimProfile::Chain,
        }
    }

    /// Attaches Prometheus metrics; call this on the node before passing the validator to the contributor.
    pub fn with_validator_metrics(mut self, metrics: Arc<ValidatorMetrics>) -> Self {
        self.validator_metrics = Some(metrics);
        self
    }

    /// Returns the RPC URL for the default chain
    pub fn rpc_url(&self) -> &str {
        self.chain_rpc_urls
            .get(&self.default_chain)
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    /// Returns the RPC URL for a specific chain
    pub fn rpc_url_for_chain(&self, chain_id: ChainRole) -> Option<&str> {
        self.chain_rpc_urls.get(&chain_id).map(|s| s.as_str())
    }

    /// Returns whether a chain is supported
    pub fn supports_chain(&self, chain_id: ChainRole) -> bool {
        self.chain_rpc_urls.contains_key(&chain_id)
    }

    /// Returns the actual EVM chain ID (from `eth_chainId`) for the given chain role's RPC.
    pub async fn get_chain_id_for(&self, chain: ChainRole) -> Result<u64> {
        self.providers
            .get(&chain)
            .ok_or_else(|| anyhow::anyhow!("No provider configured for chain role: {}", chain))?
            .get_chain_id()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch chain ID for chain {}: {}", chain, e))
    }

    /// Returns all supported chains
    pub fn supported_chains(&self) -> Vec<ChainRole> {
        self.chain_rpc_urls.keys().copied().collect()
    }

    /// Detects which chain has code deployed at the given address.
    ///
    /// Checks each supported chain to see if the address has contract code.
    /// Returns the first chain where code is found, or an error if no chain has code.
    pub async fn detect_chain_for_address(
        &self,
        address: alloy::primitives::Address,
    ) -> Result<ChainRole> {
        debug!(
            address = %address,
            "Detecting chain for address"
        );

        let supported = self.supported_chains();
        // Clone the Arc so the closure doesn't borrow self
        let providers = Arc::clone(&self.providers);

        crate::config::detect_chain_for_address(address, &supported, |chain_id, addr| {
            let providers = Arc::clone(&providers);
            async move {
                let provider = providers
                    .get(&chain_id)
                    .ok_or_else(|| anyhow::anyhow!("No provider for chain {}", chain_id))?;
                let code = provider.get_code_at(addr).await?;
                Ok(code)
            }
        })
        .await
    }

    /// Fetches the current `stateTransitionCount()` from the contract on a known chain.
    ///
    /// Skips chain detection — use this when the chain has already been identified (e.g.
    /// from `compute_storage_updates_for_tx`) to avoid a redundant `eth_getCode` round-trip.
    pub async fn get_state_transition_count_on_chain(
        &self,
        address: alloy::primitives::Address,
        chain_id: ChainRole,
    ) -> Result<u64> {
        use crate::bindings::gaskillersdk::GasKillerSDK;

        let provider = match self.providers.get(&chain_id) {
            Some(p) => p.clone(),
            None => {
                if let Some(rpc_url) = self.chain_rpc_urls.get(&chain_id) {
                    anyhow::bail!(
                        "RPC URL for chain {} is not a valid URL (provider was not built): {}",
                        chain_id,
                        rpc_url
                    );
                }
                anyhow::bail!("No RPC URL configured for chain {}", chain_id);
            }
        };
        let count = GasKillerSDK::new(address, provider)
            .stateTransitionCount()
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("stateTransitionCount call failed: {}", e))?;
        count
            .try_into()
            .map_err(|_| anyhow::anyhow!("stateTransitionCount overflow"))
    }

    /// Fetches the current `stateTransitionCount()` from the contract.
    ///
    /// Detects which chain the contract lives on, then calls the view function.
    /// Prefer [`get_state_transition_count_on_chain`] when the chain is already known.
    pub async fn get_state_transition_count(
        &self,
        address: alloy::primitives::Address,
    ) -> Result<u64> {
        let chain_id = self.detect_chain_for_address(address).await?;
        self.get_state_transition_count_on_chain(address, chain_id)
            .await
    }

    /// Computes storage updates for a transaction using gas-analyzer.
    ///
    /// Automatically detects which chain the contract is on, then computes storage updates.
    /// Returns the storage updates, block height, and the actual EVM chain ID (u64).
    pub async fn compute_storage_updates_for_tx(
        &self,
        contract_address: alloy::primitives::Address,
        call_data: &[u8],
        from_address: Option<alloy::primitives::Address>,
        value: Option<alloy::primitives::U256>,
        block_height: u64,
    ) -> Result<(Vec<u8>, u64, u64)> {
        let chain_role = self.detect_chain_for_address(contract_address).await?;

        debug!(
            chain = %chain_role,
            address = %contract_address,
            "Detected chain for contract"
        );

        let rpc_url = self
            .rpc_url_for_chain(chain_role)
            .ok_or_else(|| anyhow::anyhow!("No RPC URL configured for chain: {}", chain_role))?;

        // Fetch the actual EVM chain ID from the RPC we're already using for EVMSketch.
        let numeric_chain_id = self.get_chain_id_for(chain_role).await?;

        let result = self
            .analyze_transaction(
                rpc_url,
                contract_address,
                call_data,
                from_address,
                value,
                block_height,
            )
            .await?;
        Ok((
            result.storage_updates,
            result.block_height,
            numeric_chain_id,
        ))
    }

    /// Precomputes and caches the payload digest using already-computed storage updates.
    ///
    /// Call this from the task creator after it runs EVMSketch to build the payload, so that
    /// the orchestrator's validator can skip running EVMSketch again when verifying each incoming
    /// node signature for the same round.
    pub async fn prime_cache(&self, task_data: &GasKillerTaskData, storage_updates: &[u8]) {
        let digest = task_data.build_payload_hash(storage_updates);
        let cache_key = digest_cache_key(task_data);
        let mut cache = self.digest_cache.lock().await;
        cache.insert(cache_key, digest);
        debug!(
            transition_index = task_data.transition_index,
            block_height = task_data.block_height,
            "Primed validator digest cache from creator (verification will skip EVMSketch)"
        );
    }

    /// Performs the core gas analysis using gas-analyzer.
    ///
    /// Uses the shared executor cache to skip the 2× `eth_getBlockByNumber` build
    /// cost (~80–120 ms) when a request arrives at the same block height as a
    /// recent prior request.
    ///
    /// Takes an explicit RPC URL parameter for flexibility.
    /// Forks at the specified block for deterministic results.
    ///
    /// An analysis that extracts no state updates is an error rather than a result: see
    /// [`ensure_program_applies_something`].
    pub async fn analyze_transaction(
        &self,
        rpc_url: &str,
        contract_address: alloy::primitives::Address,
        call_data: &[u8],
        from_address: Option<alloy::primitives::Address>,
        value: Option<alloy::primitives::U256>,
        block_height: u64,
    ) -> Result<AnalysisResult> {
        debug!(
            block_number = block_height,
            contract = %contract_address,
            call_data_len = call_data.len(),
            "Analyzing transaction at block"
        );

        // Build transaction request
        let from = from_address.unwrap_or(alloy::primitives::Address::ZERO);
        let tx_value = value.unwrap_or(alloy::primitives::U256::ZERO);

        let tx_request = TransactionRequest::default()
            .from(from)
            .to(contract_address)
            .value(tx_value)
            .input(alloy::primitives::Bytes::copy_from_slice(call_data).into());

        // Call gas-analyzer to get storage updates and gas estimate using EvmSketch.
        // The executor cache eliminates the build cost on repeated requests at the
        // same block height.
        let started = Instant::now();
        let analysis = call_to_encoded_state_updates_with_evmsketch_profiled(
            &self.executor_cache,
            rpc_url,
            tx_request,
            block_height,
            self.state_encoding,
            self.sim_profile,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Gas analysis failed: {}", e))?;
        let phases = AnalysisPhases {
            extraction: analysis.extraction,
            executor_cache_hit: analysis.executor_cache_hit,
            timings: analysis.timings,
        };
        // Observed here rather than in either caller so the router and the operators cannot
        // measure different intervals for the same work.
        if let Some(metrics) = &self.validator_metrics {
            metrics
                .evmsketch_duration_seconds
                .observe(started.elapsed().as_secs_f64());
            metrics.observe_analysis(&phases);
        }

        // Every path that produces a signable diff — the router's task creation and each node's
        // independent recomputation — comes through here, so refusing an empty program once keeps
        // a no-op out of the round without the two sides being able to disagree about it.
        ensure_program_applies_something(analysis.update_count, contract_address, block_height)?;

        debug!(
            "Analysis complete: storage_updates_len={}, update_count={}, gas_estimate={}, block_height={}",
            analysis.storage_updates.len(),
            analysis.update_count,
            analysis.gas_estimate,
            block_height
        );

        Ok(AnalysisResult {
            storage_updates: analysis.storage_updates.to_vec(),
            gas_estimate: analysis.gas_estimate,
            block_height,
            phases,
        })
    }

    /// Watches each chain's head and speculatively pre-builds the EVMSketch executor for the
    /// latest block, populating the shared executor cache so a task's first validation skips the
    /// live `build()` cost (~80–120 ms) on the critical path.
    ///
    /// Runs forever; intended to be spawned as a background task. Per-chain loops run
    /// concurrently, each with at most one build in flight. Build failures are logged at `WARN`
    /// and never propagate — a miss simply falls back to the on-demand build path.
    ///
    /// The cached executor only feeds the (discarded) gas estimate, never the signed
    /// `storage_updates`, so pre-building at the unconfirmed tip cannot affect consensus.
    pub async fn run_speculative_prebuild(&self, config: SpeculativePrebuildConfig) {
        if !config.enabled {
            debug!("Speculative executor pre-build disabled");
            return;
        }

        let loops = self
            .chain_rpc_urls
            .iter()
            .filter_map(|(chain, rpc_url)| {
                let provider = self.providers.get(chain)?;
                Some(self.prebuild_chain_loop(*chain, rpc_url, provider, config))
            })
            .collect::<Vec<_>>();

        if loops.is_empty() {
            warn!("Speculative pre-build: no chains with providers; loop not started");
            return;
        }

        info!(
            chains = loops.len(),
            poll_ms = config.poll_interval.as_millis() as u64,
            confirmations = config.confirmation_depth,
            "Starting speculative executor pre-build"
        );
        futures::future::join_all(loops).await;
    }

    /// Per-chain pre-build loop: poll the head, build the target block's executor if it changed.
    async fn prebuild_chain_loop(
        &self,
        chain: ChainRole,
        rpc_url: &str,
        provider: &ReadOnlyProvider,
        config: SpeculativePrebuildConfig,
    ) {
        let mut last_built: Option<u64> = None;
        loop {
            match provider.get_block_number().await {
                Ok(head) => {
                    if let Some(target) = Self::speculative_target(head, config.confirmation_depth)
                        && last_built != Some(target)
                    {
                        match self.executor_cache.get_or_build(rpc_url, target).await {
                            Ok(_) => {
                                last_built = Some(target);
                                debug!(chain = %chain, block = target, "Speculative pre-build cached executor");
                            }
                            Err(e) => {
                                warn!(chain = %chain, block = target, error = %e, "Speculative pre-build failed");
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(chain = %chain, error = %e, "Speculative pre-build: failed to read chain head");
                }
            }
            tokio::time::sleep(config.poll_interval).await;
        }
    }

    /// The block to pre-build for a given chain `head` and confirmation depth.
    ///
    /// Returns `None` when the depth would reach at or below genesis (nothing useful to build).
    fn speculative_target(head: u64, confirmation_depth: u64) -> Option<u64> {
        head.checked_sub(confirmation_depth).filter(|&b| b > 0)
    }

    /// Computes storage updates by running local analysis.
    /// Automatically detects which chain the target address is on.
    /// Uses the block_height from task_data to ensure deterministic results matching the router.
    async fn compute_storage_updates(&self, task_data: &GasKillerTaskData) -> Result<Vec<u8>> {
        if task_data.block_height == 0 {
            return Err(anyhow::anyhow!("block_height is required for validation"));
        }

        // Detect which chain has the contract
        let chain_id = self
            .detect_chain_for_address(task_data.target_address)
            .await?;

        // Get the RPC URL for the detected chain
        let rpc_url = self
            .rpc_url_for_chain(chain_id)
            .ok_or_else(|| anyhow::anyhow!("No RPC URL configured for chain: {}", chain_id))?;

        debug!(
            chain_id = %chain_id,
            target_address = %task_data.target_address,
            "Computing storage updates for detected chain"
        );

        let result = self
            .analyze_transaction(
                rpc_url,
                task_data.target_address,
                &task_data.call_data,
                Some(task_data.from_address),
                Some(task_data.value),
                task_data.block_height,
            )
            .await?;
        Ok(result.storage_updates)
    }

    /// Validates a task and returns the digest a correct node is expected to sign for it.
    ///
    /// This is the single place where storage updates are recomputed (via EVMSketch at
    /// `task.block_height`) to avoid double computation: the recomputed updates are hashed
    /// with [`GasKillerTaskData::build_payload_hash`], so a task whose announced
    /// `storage_updates` diverge from local re-execution yields a different digest and the
    /// dishonest announcement never reaches quorum.
    ///
    /// Results are cached by (transition_index, block_height) so that repeated calls for the
    /// same task (e.g. the router resolving its automaton digest after [`Self::prime_cache`],
    /// or a node re-proposing a height after restart) only run the expensive EVMSketch
    /// computation once. Errors are NOT cached: transient RPC failures surface to the caller,
    /// which retries with backoff (deterministic failures are the caller's cue to skip).
    pub async fn expected_digest_for_task(&self, task: &GasKillerTaskData) -> Result<Digest> {
        let task_data = task;

        let cache_key = digest_cache_key(task_data);

        // Check cache before running expensive EVMSketch
        {
            let cache = self.digest_cache.lock().await;
            if let Some(cached) = cache.get(&cache_key) {
                if let Some(metrics) = &self.validator_metrics {
                    metrics.observe_digest_cache(true);
                }
                debug!(
                    transition_index = task_data.transition_index,
                    block_height = task_data.block_height,
                    "Returning cached digest (skipping EVMSketch)"
                );
                return Ok(*cached);
            }
        }

        if let Some(metrics) = &self.validator_metrics {
            metrics.observe_digest_cache(false);
        }

        // Not cached — compute storage updates (the expensive EVMSketch path)
        let storage_updates = self.compute_storage_updates(task_data).await?;

        // Build expected payload hash using computed storage updates
        let payload_hash = task_data.build_payload_hash(&storage_updates);

        // Store in cache for subsequent calls with the same round
        {
            let mut cache = self.digest_cache.lock().await;
            cache.insert(cache_key, payload_hash);
        }

        debug!("Built and cached payload hash: {:?}", payload_hash);
        Ok(payload_hash)
    }
}

#[async_trait::async_trait]
impl commonware_avs_core::validator::ValidatorTrait<GasKillerTaskData> for GasKillerValidator {
    async fn expected_digest(&self, task: &GasKillerTaskData) -> Result<Digest> {
        self.expected_digest_for_task(task).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};
    use std::time::Duration;

    fn create_test_task_data() -> GasKillerTaskData {
        GasKillerTaskData {
            storage_updates: vec![0x01, 0x02, 0x03, 0x04].into(),
            transition_index: 1,
            target_address: Address::from([1u8; 20]),
            call_data: vec![0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x01],
            from_address: Address::from([2u8; 20]),
            value: U256::from(1000),
            block_height: 12345,
            chain_id: 1u64,
        }
    }

    #[tokio::test]
    async fn test_validator_creation() {
        let _validator =
            GasKillerValidator::with_rpc_url("https://ethereum-sepolia.publicnode.com");
    }

    #[test]
    fn digest_cache_key_distinguishes_different_contracts() {
        // Two tasks for DIFFERENT contracts at the same (transition_index,
        // block_height) — the exact collision the key must avoid so one task's
        // cached digest is never returned for the other.
        let a = create_test_task_data();
        let mut b = a.clone();
        b.target_address = Address::from([9u8; 20]);
        assert_ne!(digest_cache_key(&a), digest_cache_key(&b));

        // Differing call_data (same contract) must also key distinctly, since it
        // changes the computed storage updates and therefore the digest.
        let mut c = a.clone();
        c.call_data = vec![0xde, 0xad, 0xbe, 0xef];
        assert_ne!(digest_cache_key(&a), digest_cache_key(&c));

        // Identical task identity keys identically (cache hit is intended here).
        assert_eq!(digest_cache_key(&a), digest_cache_key(&a.clone()));
    }

    #[test]
    fn test_providers_prebuilt_for_each_chain() {
        let mut urls = HashMap::new();
        urls.insert(ChainRole::L1, "https://example.com".to_string());
        urls.insert(ChainRole::L2, "https://l2.example.com".to_string());
        let validator = GasKillerValidator::with_chain_rpc_urls(urls);

        assert!(validator.providers.contains_key(&ChainRole::L1));
        assert!(validator.providers.contains_key(&ChainRole::L2));
    }

    #[test]
    fn test_speculative_target() {
        // depth 0 → build the tip
        assert_eq!(GasKillerValidator::speculative_target(100, 0), Some(100));
        // depth N → N blocks behind head
        assert_eq!(GasKillerValidator::speculative_target(100, 3), Some(97));
        // head - depth == 0 (genesis) → nothing to build
        assert_eq!(GasKillerValidator::speculative_target(2, 2), None);
        // depth deeper than head → no underflow
        assert_eq!(GasKillerValidator::speculative_target(1, 5), None);
    }

    #[test]
    fn test_executor_cache_capacity_covers_window_per_chain() {
        let window = crate::config::block_stale_measure() as usize;
        let one = executor_cache_capacity(1);
        let two = executor_cache_capacity(2);
        // Each chain gets at least a full staleness window of slots.
        assert!(one >= window);
        assert_eq!(two, one * 2);
    }

    #[tokio::test]
    async fn test_expected_digest_uses_primed_cache() {
        // prime_cache stores the digest keyed by (transition_index, block_height), so
        // expected_digest_for_task must return it without hitting any RPC. This is the
        // router-side flow: the sequencer primes after EVMSketch, the automaton looks up.
        let validator = GasKillerValidator::with_rpc_url("https://example.com");
        let task_data = create_test_task_data();
        let storage_updates = vec![0x01, 0x02, 0x03, 0x04];

        validator.prime_cache(&task_data, &storage_updates).await;

        let digest = validator
            .expected_digest_for_task(&task_data)
            .await
            .expect("cached digest lookup must not require RPC");
        assert_eq!(digest, task_data.build_payload_hash(&storage_updates));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires RPC - run with: cargo test -- --ignored"]
    async fn test_full_validation_with_rpc() {
        // Integration test: full validation including storage update computation
        // This test is ignored by default as it requires RPC access and Anvil
        let validator = GasKillerValidator::with_rpc_url("https://ethereum-sepolia.publicnode.com");
        let task_data = create_test_task_data();

        let result = validator.expected_digest_for_task(&task_data).await;

        // With proper RPC/Anvil setup, this should succeed
        let hash = result.expect("Full validation should succeed with RPC access");
        let zero_hash = Digest::from([0u8; 32]);
        assert_ne!(hash, zero_hash, "Hash should not be all zeros");
    }

    #[test]
    fn an_analysis_that_applies_nothing_is_refused() {
        let target = Address::from([7u8; 20]);
        let err = ensure_program_applies_something(0, target, 4242)
            .expect_err("a program with no operations must not reach the quorum");
        let message = err.to_string();
        // The task's stored error is what the client reads, so it has to name the call it refused.
        assert!(
            message.contains(&target.to_string()) && message.contains("4242"),
            "the refusal should name the call it applies to: {message}"
        );
    }

    #[test]
    fn an_analysis_that_applies_one_operation_is_accepted() {
        ensure_program_applies_something(1, Address::from([7u8; 20]), 4242)
            .expect("a single operation is work worth signing");
    }

    #[test]
    fn test_build_payload_hash_deterministic() {
        let task_data = create_test_task_data();
        let storage_updates = vec![0x01, 0x02, 0x03, 0x04];

        let hash1 = task_data.build_payload_hash(&storage_updates);
        let hash2 = task_data.build_payload_hash(&storage_updates);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, Digest::from([0u8; 32]));
    }

    #[test]
    fn test_build_payload_hash_different_inputs() {
        let task_data = create_test_task_data();

        let hash1 = task_data.build_payload_hash(&[0x01, 0x02]);
        let hash2 = task_data.build_payload_hash(&[0x03, 0x04]);

        assert_ne!(hash1, hash2);
    }

    fn phases(extraction: Extraction, executor_cache_hit: bool) -> AnalysisPhases {
        AnalysisPhases {
            extraction,
            executor_cache_hit,
            timings: EncodePhaseTimings {
                trace_fetch: Duration::from_millis(1500),
                parse: Duration::from_millis(300),
                executor_build: Duration::from_millis(80),
                prefetch: Duration::from_millis(40),
                revm_estimate: Duration::from_millis(60),
            },
        }
    }

    #[test]
    fn the_struct_log_path_reports_every_phase_under_its_extraction_label() {
        let metrics = ValidatorMetrics::new();
        metrics.observe_analysis(&phases(Extraction::StructLog, false));

        let output = metrics.encode();
        for phase in [
            "trace_fetch",
            "parse",
            "executor_build",
            "state_prefetch",
            "revm_estimate",
        ] {
            assert!(
                output.contains(&format!(
                    "gas_killer_evmsketch_{phase}_seconds_count{{extraction=\"struct_log\"}} 1"
                )),
                "{phase} must be observed on the struct-log path"
            );
        }
        assert!(output.contains("gas_killer_evmsketch_executor_cache_total{result=\"miss\"} 1"));
    }

    /// The net form fetches no struct-log trace, so it must leave the parse series untouched
    /// rather than observing a zero. An invented zero would drag the parse percentiles down and
    /// hide exactly the saving this label exists to measure.
    #[test]
    fn the_net_form_leaves_the_parse_series_empty() {
        let metrics = ValidatorMetrics::new();
        metrics.observe_analysis(&phases(Extraction::PrestateNet, true));

        let output = metrics.encode();
        assert!(
            output.contains(
                "gas_killer_evmsketch_trace_fetch_seconds_count{extraction=\"prestate_net\"} 1"
            ),
            "the net form still reads two tracers"
        );
        assert!(
            !output
                .contains("gas_killer_evmsketch_parse_seconds_count{extraction=\"prestate_net\"}"),
            "no parse series may exist for a path that never parses"
        );
        assert!(output.contains("gas_killer_evmsketch_executor_cache_total{result=\"hit\"} 1"));
    }

    /// A fallback pays for both paths, so it must report a parse cost — under its own label, so
    /// it never masquerades as a cheap net-form run.
    #[test]
    fn a_prestate_fallback_reports_a_parse_cost_under_its_own_label() {
        let metrics = ValidatorMetrics::new();
        metrics.observe_analysis(&phases(Extraction::PrestateFallback, false));

        let output = metrics.encode();
        assert!(output.contains(
            "gas_killer_evmsketch_parse_seconds_count{extraction=\"prestate_fallback\"} 1"
        ));
        assert!(
            !output.contains("extraction=\"prestate_net\""),
            "a fallback is not a net-form run"
        );
    }

    #[test]
    fn digest_cache_outcomes_are_counted_separately() {
        let metrics = ValidatorMetrics::new();
        metrics.observe_digest_cache(true);
        metrics.observe_digest_cache(true);
        metrics.observe_digest_cache(false);

        let output = metrics.encode();
        assert!(output.contains("gas_killer_evmsketch_digest_cache_total{result=\"hit\"} 2"));
        assert!(output.contains("gas_killer_evmsketch_digest_cache_total{result=\"miss\"} 1"));
    }
}
