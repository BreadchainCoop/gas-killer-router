//! Construction of the router's environment-configured components: alloy
//! providers, the HTTP ingress, and the on-chain submitter.

use crate::GasKillerHandler;
use crate::ingress::{
    AvsMetadata, AvsOperatorSetMetadata, AvsOperatorSetSoftware, GasKillerTaskRequest,
    IngressState, start_gas_killer_http_server,
};
use crate::metrics::MetricsCollector;
use crate::rate_limit::KeyRateLimiter;
use crate::rpc_health::RpcHealth;
use crate::schnorr_coordinator::SchnorrCertifiedReceiver;
use crate::schnorr_submitter::SchnorrSubmitter;
use crate::sequencer::{
    InFlightTask, QueuedTask, TaskQueueDepth, TaskReceiver, TaskSender, task_channel,
    task_queue_depth,
};
use crate::store::{SqliteStore, TaskCursor};
use alloy::network::{Ethereum, EthereumWallet};
use alloy_primitives::{Address, Bytes};
use alloy_provider::{
    Identity, Provider, ProviderBuilder, RootProvider,
    fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
        SimpleNonceManager, WalletFiller,
    },
};
use alloy_signer_local::PrivateKeySigner;
use anyhow::Result;
use commonware_avs_core::bn254::Bn254Scheme;
use commonware_avs_eigenlayer::AvsDeployment;
use commonware_avs_router::reporter::CertifiedReceiver;
use commonware_avs_router::sequencer::{DispatchTime, ResolutionSender, SharedAssignments};
use commonware_avs_router::submitter::Submitter;
use gas_killer_common::avs_contracts::{
    self, ContractsConfig, ResolvedContracts, SCHNORR_STAKE_REGISTRY_KEY,
};
use gas_killer_common::bindings::bls_apk_registry::BLSApkRegistry;
use gas_killer_common::bindings::bls_sig_check_operator_state_retriever::BLSSigCheckOperatorStateRetriever;
use gas_killer_common::task_data::GasKillerTaskData;
use gas_killer_common::{ChainRole, GasKillerValidator};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::{env, str::FromStr, sync::Arc};
use tracing::{debug, error, info, warn};

/// Quorum 0 — the only quorum this deployment operates on.
const QUORUM_NUMBERS: &[u8] = &[0x00];

/// How often the background loop re-checks SQLite store liveness for the `gas_killer_db_up`
/// metric. Aligned with a typical Prometheus scrape interval so the gauge stays fresh.
const DB_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// How often the background loop probes each chain's RPC for the circuit breaker.
///
/// The probe is what makes the breaker independent of traffic: it trips on an outage even while
/// the router is idle, and — because a degraded chain refuses new submissions — it is the only
/// thing guaranteed to still be calling that chain, so it is also what lets the breaker clear.
/// Without it a chain that degraded under load would stay degraded, having rejected the very
/// requests whose success would have cleared it.
const RPC_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// How long the startup re-queue may spend reading `stateTransitionCount()` before it stops
/// checking and re-queues the rest unchecked. Bounds the delay a large recovered backlog spread
/// across many distinct targets can add before the sequencer starts.
const TRANSITION_CHECK_BUDGET: Duration = Duration::from_secs(30);

/// Consecutive unreadable targets taken as the chain being unavailable rather than those targets
/// being individually unresolvable, after which the re-queue stops checking.
const MAX_CONSECUTIVE_COUNT_FAILURES: u32 = 3;

/// Rows the startup re-queue loads per query, repeating until the backlog is drained.
///
/// Bounds the rows a single query materializes, not the re-queue's peak footprint: the task
/// channel is unbounded and the sequencer dispatches one task at a time, so everything the walk
/// sends stays resident until the sequencer reaches it.
///
/// Sized above [`gas_killer_common::max_queue_depth`]'s default, which the ingress caps
/// admissions against, so an unraised deployment drains in one page. Raising that cap past this
/// is what puts the walk on its second page.
const REQUEUE_PAGE_SIZE: u32 = 256;

/// Wallet provider that uses SimpleNonceManager to always fetch the pending nonce from the
/// chain rather than caching it locally. This prevents nonce corruption when a transaction
/// fails during gas estimation (e.g., due to a stale transition_index from double-execution),
/// because the cached counter is never pre-incremented before the tx is actually broadcast.
pub type SimpleWalletProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<
                GasFiller,
                JoinFill<BlobGasFiller, JoinFill<NonceFiller<SimpleNonceManager>, ChainIdFiller>>,
            >,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider,
    Ethereum,
>;

/// The ingress side of the task pipeline: the sequencer consumes `receiver`;
/// `sender` is retained by the caller so the channel stays open even when the
/// HTTP server is disabled (`INGRESS != true`) — a closed channel would shut the
/// sequencer down.
pub struct IngressHandles {
    pub sender: TaskSender,
    pub receiver: TaskReceiver,
    pub queue_depth: TaskQueueDepth,
    /// The durable store opened for the HTTP ingress, shared with the task source
    /// and executor so they can drive task status transitions. `None` when
    /// `INGRESS != true` (no store is opened without the HTTP server).
    pub store: Option<SqliteStore>,
    /// Last task a previous router life left in flight, read before this process began
    /// accepting submissions. `None` when there was no backlog, or when no store was opened.
    ///
    /// Taken here rather than by the re-queue itself because it is only an honest upper bound on
    /// "what a previous life left" while no row the running ingress created can exist yet. Read
    /// after the server starts, a submission accepted in between would sort at or below it, and
    /// the walk would enqueue a task the ingress had already put in the channel.
    ///
    /// The separation it buys resolves to one wall second, since `created_at` is `unixepoch()`.
    /// A submission accepted in the same second as this read, whose v4 uuid happens to sort below
    /// the bound's, is inside the walk and reaches the channel twice. Reaching it takes the
    /// previous life's newest row, this read, and a first request inside one second, which is
    /// shorter than the bind-and-connect path that separates them, and the claim guard in
    /// `GasKillerTaskSource` drops the duplicate without spending a round. Closing it needs a
    /// monotonic insertion key rather than a timestamp.
    pub requeue_bound: Option<TaskCursor>,
}

/// Creates the ingress task channel and, when `INGRESS=true`, spawns the HTTP
/// server (`/tasks`, `/healthz`, `/avs-metadata`) on `INGRESS_ADDRESS`
/// (default `0.0.0.0:8080`).
pub async fn create_ingress(metrics: Arc<MetricsCollector>) -> Result<IngressHandles> {
    let (sender, receiver) = task_channel();
    let queue_depth = task_queue_depth();

    let use_ingress = env::var("INGRESS").unwrap_or_default().to_lowercase() == "true";
    if !use_ingress {
        info!("INGRESS is not 'true'; HTTP ingress disabled (task channel stays idle)");
        return Ok(IngressHandles {
            sender,
            receiver,
            queue_depth,
            store: None,
            requeue_bound: None,
        });
    }

    let addr = env::var("INGRESS_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    info!(address = %addr, "starting GasKiller HTTP ingress");

    let providers = build_ingress_providers()?;
    let admin_key = env::var("ADMIN_KEY").ok().filter(|k| !k.is_empty());
    if admin_key.is_none() {
        tracing::warn!(
            "ADMIN_KEY is not set — /admin/keys endpoints are disabled; set ADMIN_KEY to manage API keys"
        );
    }
    let operator_sets = {
        let opset_name = env::var("AVS_OPSET_NAME").unwrap_or_default();
        if opset_name.is_empty() {
            None
        } else {
            let slashing_conditions = env::var("AVS_OPSET_SLASHING_CONDITIONS")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Some(vec![AvsOperatorSetMetadata {
                name: opset_name,
                id: env::var("AVS_OPSET_ID").unwrap_or_else(|_| "0".to_string()),
                description: env::var("AVS_OPSET_DESCRIPTION").unwrap_or_default(),
                software: vec![AvsOperatorSetSoftware {
                    name: env::var("AVS_OPSET_SOFTWARE_NAME")
                        .unwrap_or_else(|_| "gas-killer-node".to_string()),
                    description: env::var("AVS_OPSET_SOFTWARE_DESCRIPTION").unwrap_or_default(),
                    url: env::var("AVS_OPSET_SOFTWARE_URL").unwrap_or_default(),
                }],
                slashing_conditions,
            }])
        }
    };
    let contracts = ResolvedContracts::default();
    spawn_contracts_resolver(&providers, contracts.clone());

    let avs_metadata = AvsMetadata {
        name: env::var("AVS_METADATA_NAME").unwrap_or_else(|_| "Gas Killer".to_string()),
        website: env::var("AVS_METADATA_WEBSITE")
            .unwrap_or_else(|_| "https://gaskiller.xyz".to_string()),
        description: env::var("AVS_METADATA_DESCRIPTION").unwrap_or_else(|_| {
            "Verifiable off-chain compute service for EVM smart contracts via EigenLayer"
                .to_string()
        }),
        logo: env::var("AVS_METADATA_LOGO").ok().filter(|s| !s.is_empty()),
        twitter: env::var("AVS_METADATA_TWITTER")
            .ok()
            .filter(|s| !s.is_empty()),
        operator_sets,
        contracts,
    };
    // Open the durable store and apply migrations before serving traffic. A failure here
    // aborts router startup rather than running against an unmigrated or unwritable store.
    let store = SqliteStore::connect().await?;

    // Fix the re-queue's upper bound while the backlog is still exactly what a previous router
    // life left: nothing below is served yet, so no row here can be one this process accepted.
    let requeue_bound = store.last_incomplete_task().await?;

    // Publish store liveness as `gas_killer_db_up`. connect() already proved the store
    // answers, so seed the gauge to 1; a background loop then re-checks so a later volume
    // loss (detached PVC, full or read-only disk) surfaces as db_up=0 on the dashboard.
    metrics.db_up.set(1);
    {
        let store = store.clone();
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(DB_HEALTH_CHECK_INTERVAL);
            loop {
                ticker.tick().await;
                metrics.db_up.set(store.health_check().await.is_ok() as i64);
            }
        });
    }

    // Watch every chain the ingress can read, so an outage on either surfaces on the
    // `gas_killer_rpc_healthy` gauge whether or not traffic happens to touch that chain.
    let rpc_health = Arc::new(RpcHealth::new(
        gas_killer_common::rpc_failure_threshold(),
        providers.keys().copied().collect::<Vec<_>>(),
        Some(Arc::clone(&metrics)),
    ));
    {
        let rpc_health = Arc::clone(&rpc_health);
        let probe_providers: Vec<_> = providers
            .iter()
            .map(|(&chain, provider)| (chain, provider.clone()))
            .collect();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(RPC_HEALTH_CHECK_INTERVAL);
            loop {
                ticker.tick().await;
                for (chain, provider) in &probe_providers {
                    // `eth_blockNumber` is the cheapest call that proves a provider is answering.
                    // It does not prove the provider can serve historical state, which is why the
                    // request path reports its own outcomes too.
                    match provider.get_block_number().await {
                        Ok(_) => rpc_health.record_success(*chain),
                        Err(e) => {
                            tracing::debug!(chain = %chain.name(), error = %e, "RPC health probe failed");
                            rpc_health.record_failure(*chain);
                        }
                    }
                }
            }
        });
    }

    let store_for_return = store.clone();
    let rate_limiter = Arc::new(KeyRateLimiter::new(gas_killer_common::rate_limit_rpm()));
    let ingress_state = IngressState::new(
        sender.clone(),
        queue_depth.clone(),
        gas_killer_common::max_queue_depth(),
        rate_limiter,
        metrics,
        providers,
        avs_metadata,
    )
    .with_store(store)
    .with_admin_key(admin_key)
    .with_rpc_health(rpc_health);
    // Plain tokio::spawn works: the commonware tokio runtime shares the ambient
    // reactor with axum.
    tokio::spawn(async move {
        start_gas_killer_http_server(ingress_state, &addr).await;
    });

    Ok(IngressHandles {
        sender,
        receiver,
        queue_depth,
        store: Some(store_for_return),
        requeue_bound,
    })
}

/// Reads a target contract's current `stateTransitionCount()`.
#[async_trait::async_trait]
pub trait TransitionCountReader: Send + Sync {
    async fn state_transition_count(&self, target: Address) -> Result<u64>;
}

#[async_trait::async_trait]
impl TransitionCountReader for GasKillerValidator {
    async fn state_transition_count(&self, target: Address) -> Result<u64> {
        self.get_state_transition_count(target).await
    }
}

/// Re-enqueues every task left `queued` or `processing` by a previous router life,
/// so a restart resumes work already acknowledged to a client rather than losing
/// it. Called once at startup, before the sequencer starts pulling from `sender`.
///
/// Each task is rebuilt from its persisted request and pushed through the same
/// channel a fresh `POST /tasks` submission uses, so it flows through the normal
/// dequeue → processing → ready/failed pipeline indistinguishably from new work.
/// The backlog is walked in pages of [`REQUEUE_PAGE_SIZE`], bounded above by `through`,
/// the last row in flight before this process began accepting submissions.
///
/// A task whose `transition_index` the contract has already passed settles `expired`:
/// `verifyAndUpdate` orders on that index, so aggregating it could only reach a payload the
/// contract rejects.
pub async fn requeue_incomplete_tasks(
    store: &SqliteStore,
    sender: &TaskSender,
    queue_depth: &TaskQueueDepth,
    through: Option<&TaskCursor>,
    transitions: &dyn TransitionCountReader,
    metrics: Option<&MetricsCollector>,
) -> Result<()> {
    let Some(through) = through else {
        return Ok(());
    };

    info!("re-enqueuing incomplete tasks from a previous router life");

    // One read per distinct target: a restart with a full window in flight recovers many tasks
    // pointing at the same contract.
    let mut counts: HashMap<Address, u64> = HashMap::new();
    // A read failure is not attributable to the chain: `get_state_transition_count` detects the
    // target's chain first, and that step reports a target it cannot place the same way it
    // reports an RPC that will not answer. So a single failure only skips its own target, and
    // only a run of them is taken as the chain being down. The budget bounds the remaining case
    // the run does not cover, reads that succeed but are slow across many distinct targets.
    let mut consecutive_failures = 0u32;
    let started = Instant::now();
    let mut checking = true;

    let mut cursor: Option<TaskCursor> = None;
    let mut requeued = 0usize;
    let mut settled = 0usize;

    loop {
        let page = store
            .incomplete_tasks_page(cursor.as_ref(), through, REQUEUE_PAGE_SIZE)
            .await?;
        let Some(last) = page.last() else { break };
        cursor = Some(TaskCursor {
            created_at: last.created_at,
            id: last.id.clone(),
        });

        let page_len = page.len();
        for task in page {
            let task_id = task.id;
            let target = task.request.target_address;

            // A server-resolved (`None`) index has nothing to compare against: the sequencer reads
            // the current value when it dequeues.
            if let Some(index) = task.request.transition_index
                && checking
            {
                let count = match counts.get(&target) {
                    Some(count) => Some(*count),
                    None if started.elapsed() >= TRANSITION_CHECK_BUDGET => {
                        warn!(
                            budget_secs = TRANSITION_CHECK_BUDGET.as_secs(),
                            "transition-index check budget spent; re-queueing the remaining recovered tasks unchecked"
                        );
                        checking = false;
                        None
                    }
                    None => match transitions.state_transition_count(target).await {
                        Ok(count) => {
                            consecutive_failures = 0;
                            counts.insert(target, count);
                            Some(count)
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            warn!(
                                %target,
                                error = %e,
                                consecutive_failures,
                                "could not read stateTransitionCount; re-queueing this task unchecked"
                            );
                            if consecutive_failures >= MAX_CONSECUTIVE_COUNT_FAILURES {
                                warn!(
                                    "stateTransitionCount unreadable for {consecutive_failures} targets in a row; re-queueing the remaining recovered tasks unchecked"
                                );
                                checking = false;
                            }
                            None
                        }
                    },
                };

                if let Some(count) = count
                    && count > index
                {
                    let reason = format!(
                        "transition index {index} is already applied on chain (the contract reports \
                         {count}); this task's own effect may already have landed, so check the \
                         target's state before re-requesting"
                    );
                    match store.mark_task_expired(&task_id, &reason).await {
                        Ok(true) => {
                            if let Some(m) = metrics {
                                m.tasks_expired_at_requeue.inc();
                            }
                            info!(
                                task_id,
                                %target,
                                transition_index = index,
                                onchain_count = count,
                                "recovered task's transition index is already consumed; settled expired"
                            );
                            settled += 1;
                            continue;
                        }
                        Ok(false) => {
                            warn!(
                                task_id,
                                "recovered task vanished before it could be settled expired"
                            );
                            continue;
                        }
                        // Fall through to re-enqueue: a row left `processing` with nothing in the
                        // channel is stranded until the TTL sweep.
                        Err(e) => error!(
                            task_id,
                            error = %e,
                            "failed to settle an already-applied recovered task; re-queueing it"
                        ),
                    }
                }
            }

            let queued = QueuedTask {
                task_id: task_id.clone(),
                request: GasKillerTaskRequest { body: task.request },
            };
            if sender.send(queued).is_err() {
                error!(
                    task_id,
                    "failed to re-enqueue incomplete task: channel closed"
                );
                continue;
            }
            queue_depth.fetch_add(1, Ordering::Relaxed);
            requeued += 1;
        }

        if page_len < REQUEUE_PAGE_SIZE as usize {
            break;
        }
    }

    info!(
        requeued,
        settled, "finished re-enqueuing incomplete tasks from a previous router life"
    );
    Ok(())
}

/// Starts background resolution of the `contracts` block on `GET /avs-metadata`, filling `slot`
/// once the addresses are established. See [`gas_killer_common::avs_contracts`].
///
/// Everything it needs comes from the running deployment: the operators' registry coordinator and
/// the Schnorr stake registry through the same `avs_deploy.json` loader the submitter reads, and the
/// AVS/checker pair from a live target's own getters. Anything missing leaves the block off, because
/// the endpoint's other fields are identity information still worth serving, and an integrator reads
/// an absent block as "no authoritative answer" rather than being handed a wrong one.
fn spawn_contracts_resolver(
    providers: &HashMap<ChainRole, gas_killer_common::ReadOnlyProvider>,
    slot: ResolvedContracts,
) {
    let Some(deployment_path) = avs_contracts::deployment_path() else {
        tracing::warn!(
            "AVS_DEPLOYMENT_PATH is not set; /avs-metadata will omit settlement contract addresses"
        );
        return;
    };
    let Some(provider) = providers.get(&ChainRole::L1) else {
        tracing::warn!(
            "no L1 provider configured; /avs-metadata will omit settlement contract addresses"
        );
        return;
    };
    let deployment = match AvsDeployment::load() {
        Ok(deployment) => deployment,
        Err(e) => {
            error!(
                error = %e,
                "could not read the AVS deployment; /avs-metadata will omit settlement contract \
                 addresses"
            );
            return;
        }
    };
    let registry_coordinator = match deployment.registry_coordinator_address() {
        Ok(address) => address,
        Err(e) => {
            error!(
                error = %e,
                "could not read the registry coordinator from the AVS deployment; /avs-metadata \
                 will omit settlement contract addresses"
            );
            return;
        }
    };
    // The operator-set job records the Schnorr registry here after deploying it. An absent key is
    // the normal state for a deployment that has provisioned none, and `custom_address` reports
    // that the same way it reports a malformed value, so this degrades to "none" either way and
    // logs the reason for anyone chasing a field that did not appear.
    let schnorr_stake_registry = match deployment.custom_address(SCHNORR_STAKE_REGISTRY_KEY) {
        Ok(address) => Some(address),
        Err(e) => {
            debug!(
                error = %e,
                "no {SCHNORR_STAKE_REGISTRY_KEY} in the AVS deployment; /avs-metadata publishes \
                 one only if SCHNORR_STAKE_REGISTRY_ADDRESS is set"
            );
            None
        }
    };
    let config = match ContractsConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            error!(
                error = %e,
                "malformed reference target configuration; /avs-metadata will omit settlement \
                 contract addresses"
            );
            return;
        }
    };
    avs_contracts::spawn_resolver(
        provider.clone(),
        registry_coordinator,
        schnorr_stake_registry,
        deployment_path,
        config,
        slot,
    );
}

fn build_ingress_providers()
-> anyhow::Result<HashMap<ChainRole, gas_killer_common::ReadOnlyProvider>> {
    let chain_rpc_urls = gas_killer_common::chain_rpc_urls_from_env()?;
    let providers = gas_killer_common::build_read_providers(&chain_rpc_urls);

    if providers.is_empty() {
        anyhow::bail!("no ingress providers could be created: set HTTP_RPC and/or L2_HTTP_RPC");
    }

    Ok(providers)
}

/// Creates a wallet provider for a specific chain using SimpleNonceManager.
///
/// SimpleNonceManager always fetches the pending nonce from the node on every transaction
/// rather than caching it locally. This ensures that if a transaction fails during gas
/// estimation (e.g., double-execution with a stale transition_index), the local nonce counter
/// is never corrupted, keeping subsequent rounds healthy.
async fn create_wallet_provider_for_chain(
    chain_role: ChainRole,
    private_key: &str,
) -> Result<SimpleWalletProvider> {
    let http_rpc = chain_role.rpc_url()?;

    let ecdsa_signer = PrivateKeySigner::from_str(private_key)
        .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

    let provider = ProviderBuilder::default()
        .filler(JoinFill::new(
            GasFiller,
            JoinFill::new(
                BlobGasFiller::default(),
                JoinFill::new(
                    NonceFiller::<SimpleNonceManager>::default(),
                    ChainIdFiller::default(),
                ),
            ),
        ))
        .wallet(ecdsa_signer)
        .connect(&http_rpc)
        .await
        .map_err(|e| {
            anyhow::anyhow!("Failed to connect write provider for {}: {}", chain_role, e)
        })?;

    Ok(provider)
}

/// Builds the pieces every submitter shares: the L1 read-side provider (for
/// block-number / operator-state reads) and the multi-chain [`GasKillerHandler`]
/// that executes `verifyAndUpdate` on the write side.
///
/// The read side always points at L1 via `HTTP_RPC`. `L2_HTTP_RPC` is used
/// exclusively for the write side: submitting `verifyAndUpdate` transactions on L2
/// when the target contract lives there.
async fn create_handler_parts(
    metrics: Arc<MetricsCollector>,
    dispatch_time: DispatchTime,
    store: Option<SqliteStore>,
    in_flight: InFlightTask,
) -> Result<(
    gas_killer_common::bindings::ReadOnlyProvider,
    GasKillerHandler<SimpleWalletProvider>,
)> {
    let http_rpc = env::var("HTTP_RPC").expect("HTTP_RPC must be set");
    let private_key = env::var("PRIVATE_KEY").expect("PRIVATE_KEY must be set");

    let l2_http_rpc = env::var("L2_HTTP_RPC").ok();

    let view_only_provider = ProviderBuilder::new().connect_http(
        url::Url::parse(&http_rpc)
            .map_err(|e| anyhow::anyhow!("Failed to parse RPC URL '{}': {}", http_rpc, e))?,
    );

    // Create wallet providers for each supported chain, keyed by actual EVM chain ID.
    // `chain_roles` records the role behind each numeric ID so the executor can pick
    // the per-role receipt timeout from the chain ID carried in task data.
    let mut providers: HashMap<u64, SimpleWalletProvider> = HashMap::new();
    let mut chain_roles: HashMap<u64, ChainRole> = HashMap::new();

    // L1 provider (required)
    let l1_provider = create_wallet_provider_for_chain(ChainRole::L1, &private_key).await?;
    let l1_chain_id = l1_provider
        .get_chain_id()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get L1 chain ID: {}", e))?;
    providers.insert(l1_chain_id, l1_provider);
    chain_roles.insert(l1_chain_id, ChainRole::L1);
    info!(
        chain_id = l1_chain_id,
        chain = "l1",
        "Created L1 wallet provider"
    );

    // L2 provider — optional, only used for write-side tx execution on L2
    if l2_http_rpc.is_some() {
        match create_wallet_provider_for_chain(ChainRole::L2, &private_key).await {
            Ok(l2_provider) => match l2_provider.get_chain_id().await {
                Ok(l2_chain_id) if l2_chain_id == l1_chain_id => {
                    tracing::warn!(
                        chain_id = l2_chain_id,
                        "L2_HTTP_RPC resolves to the same EVM chain ID as HTTP_RPC (L1); \
                         skipping L2 provider to avoid overwriting L1. Check that HTTP_RPC \
                         and L2_HTTP_RPC point at different chains"
                    );
                }
                Ok(l2_chain_id) => {
                    providers.insert(l2_chain_id, l2_provider);
                    chain_roles.insert(l2_chain_id, ChainRole::L2);
                    info!(
                        chain_id = l2_chain_id,
                        chain = "l2",
                        "Created L2 wallet provider"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        chain = "l2",
                        error = %e,
                        "Failed to get L2 chain ID, L2 chain will be unavailable"
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    chain = "l2",
                    error = %e,
                    "Failed to create L2 wallet provider, L2 chain will be unavailable"
                );
            }
        }
    } else {
        info!("L2_HTTP_RPC not set, L2 chain support disabled");
    }

    // Optional override (seconds) for the verifyAndUpdate receipt-wait timeout.
    // Unset falls back to the executor's per-chain defaults.
    let receipt_timeout_override = env::var("EXECUTOR_RECEIPT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    // Create handler with multi-chain providers. The store and in-flight task slot
    // let a settled height advance its task's terminal status; the slot is shared
    // with the task source (see `GasKillerTaskSource`).
    let mut gas_killer_handler = GasKillerHandler::with_providers(providers)
        .with_chain_roles(chain_roles)
        .with_metrics(metrics)
        .with_dispatch_time(dispatch_time)
        .with_receipt_timeout(receipt_timeout_override)
        .with_in_flight_task(in_flight)
        .with_payload_block_buffer(gas_killer_common::payload_block_buffer());
    if let Some(store) = store {
        gas_killer_handler = gas_killer_handler.with_store(store);
    }

    Ok((view_only_provider, gas_killer_handler))
}

/// Creates the BLS [`Submitter`] with multi-chain support (`bls` mode).
///
/// The read side (view_only_provider, BLS contracts) always points at L1 via
/// `HTTP_RPC` and `AVS_DEPLOYMENT_PATH`. Operator state lives on L1 and is not
/// available on the L2 mimic contract.
#[allow(clippy::too_many_arguments)]
pub async fn create_submitter(
    scheme: Bn254Scheme,
    assignments: SharedAssignments<GasKillerTaskData>,
    certified: CertifiedReceiver<Bn254Scheme>,
    resolutions: ResolutionSender,
    metrics: Arc<MetricsCollector>,
    dispatch_time: DispatchTime,
    namespace: Vec<u8>,
    store: Option<SqliteStore>,
    in_flight: InFlightTask,
) -> Result<Submitter<GasKillerTaskData, GasKillerHandler<SimpleWalletProvider>>> {
    let (view_only_provider, gas_killer_handler) =
        create_handler_parts(metrics, dispatch_time, store, in_flight).await?;

    let deployment =
        AvsDeployment::load().map_err(|e| anyhow::anyhow!("Failed to load deployment: {}", e))?;
    info!("Submitter reads operator state from L1 (HTTP_RPC)");

    let bls_apk_registry_address = deployment
        .bls_apk_registry_address()
        .map_err(|e| anyhow::anyhow!("Failed to get BLS APK registry address: {}", e))?;
    let registry_coordinator_address = deployment
        .registry_coordinator_address()
        .map_err(|e| anyhow::anyhow!("Failed to get registry coordinator address: {}", e))?;
    let bls_operator_state_retriever_address = deployment
        .bls_sig_check_operator_state_retriever_address()
        .map_err(|e| {
            anyhow::anyhow!("Failed to get BLS operator state retriever address: {}", e)
        })?;

    let bls_apk_registry =
        BLSApkRegistry::new(bls_apk_registry_address, view_only_provider.clone());
    let bls_operator_state_retriever = BLSSigCheckOperatorStateRetriever::new(
        bls_operator_state_retriever_address,
        view_only_provider.clone(),
    );

    Ok(Submitter::new(
        scheme,
        view_only_provider,
        bls_apk_registry,
        bls_operator_state_retriever,
        registry_coordinator_address,
        gas_killer_handler,
        assignments,
        certified,
        resolutions,
        namespace,
        Bytes::from_static(QUORUM_NUMBERS),
    ))
}

/// Creates the [`SchnorrSubmitter`] (`schnorr` mode). Same environment surface as
/// [`create_submitter`]; only the certified-observation source and the on-chain
/// calling convention differ (no BLS registry — the Schnorr registry is read
/// on-chain by the target contract itself).
#[allow(clippy::too_many_arguments)]
pub async fn create_schnorr_submitter(
    assignments: SharedAssignments<GasKillerTaskData>,
    certified: SchnorrCertifiedReceiver,
    resolutions: ResolutionSender,
    metrics: Arc<MetricsCollector>,
    dispatch_time: DispatchTime,
    namespace: Vec<u8>,
    store: Option<SqliteStore>,
    in_flight: InFlightTask,
) -> Result<SchnorrSubmitter> {
    let (view_only_provider, gas_killer_handler) =
        create_handler_parts(metrics, dispatch_time, store, in_flight).await?;
    Ok(SchnorrSubmitter::new(
        view_only_provider,
        gas_killer_handler,
        assignments,
        certified,
        resolutions,
        namespace,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GasKillerTaskRequestBody;
    use crate::metrics::MetricsCollector;
    use crate::store::TaskStatus;
    use alloy_primitives::U256;
    use std::sync::Mutex;

    /// Scripted `stateTransitionCount()` source, recording every target it is asked about.
    struct FakeCounts {
        counts: HashMap<Address, u64>,
        /// Every read fails, standing in for an unreachable chain.
        fails: bool,
        reads: Mutex<Vec<Address>>,
    }

    impl FakeCounts {
        fn new(counts: impl IntoIterator<Item = (Address, u64)>) -> Self {
            Self {
                counts: counts.into_iter().collect(),
                fails: false,
                reads: Mutex::new(Vec::new()),
            }
        }

        fn failing() -> Self {
            Self {
                counts: HashMap::new(),
                fails: true,
                reads: Mutex::new(Vec::new()),
            }
        }

        fn reads(&self) -> Vec<Address> {
            self.reads.lock().expect("reads lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl TransitionCountReader for FakeCounts {
        async fn state_transition_count(&self, target: Address) -> Result<u64> {
            self.reads.lock().expect("reads lock").push(target);
            if self.fails {
                anyhow::bail!("rpc unreachable");
            }
            self.counts
                .get(&target)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("no count scripted for {target}"))
        }
    }

    /// Deletes a task row when asked for a count, so the settle that follows finds nothing.
    struct DeletingReader {
        store: SqliteStore,
        task_id: String,
        count: u64,
    }

    #[async_trait::async_trait]
    impl TransitionCountReader for DeletingReader {
        async fn state_transition_count(&self, _target: Address) -> Result<u64> {
            sqlx::query("DELETE FROM tasks WHERE id = ?1")
                .bind(&self.task_id)
                .execute(self.store.pool())
                .await?;
            Ok(self.count)
        }
    }

    /// Closes the store's pool when asked for a count, so the settle that follows errors.
    struct ClosingReader {
        store: SqliteStore,
        count: u64,
    }

    #[async_trait::async_trait]
    impl TransitionCountReader for ClosingReader {
        async fn state_transition_count(&self, _target: Address) -> Result<u64> {
            self.store.pool().close().await;
            Ok(self.count)
        }
    }

    /// Moves a task's `created_at` back a minute, fixing its position in the re-queue walk.
    async fn backdate(store: &SqliteStore, id: &str) {
        sqlx::query("UPDATE tasks SET created_at = created_at - 60 WHERE id = ?1")
            .bind(id)
            .execute(store.pool())
            .await
            .expect("backdating should succeed");
    }

    async fn status_of(store: &SqliteStore, id: &str) -> TaskStatus {
        store
            .get_task(id)
            .await
            .expect("get_task should succeed")
            .expect("task should exist")
            .status
    }

    fn target(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    fn request(target_address: Address, transition_index: Option<u64>) -> GasKillerTaskRequestBody {
        GasKillerTaskRequestBody {
            target_address,
            call_data: vec![0xab, 0xcd, 0xef, 0x01],
            transition_index,
            from_address: Address::from([0x22; 20]),
            value: U256::ZERO,
            block_height: 21_000_000,
        }
    }

    async fn store_with_key() -> (SqliteStore, String) {
        let store = SqliteStore::connect_in_memory()
            .await
            .expect("in-memory store should open and migrate");
        let key_id = store
            .create_api_key(None, None)
            .await
            .expect("key creation should succeed")
            .id;
        (store, key_id)
    }

    /// Persists a task and leaves it `processing`.
    async fn processing_task(
        store: &SqliteStore,
        key_id: &str,
        request: &GasKillerTaskRequestBody,
    ) -> String {
        let task = store
            .create_task(key_id, request)
            .await
            .expect("task creation should succeed");
        assert!(
            store
                .claim_task_for_processing(&task.id)
                .await
                .expect("claim should succeed"),
            "a freshly queued task should be claimable"
        );
        task.id
    }

    /// Drains everything the re-queue pushed, as task ids.
    fn drained(receiver: &mut TaskReceiver) -> Vec<String> {
        let mut ids = Vec::new();
        while let Ok(task) = receiver.try_recv() {
            ids.push(task.task_id);
        }
        ids
    }

    /// A `processing` task whose transition already landed settles `expired`, naming the index
    /// and the on-chain count.
    #[tokio::test]
    async fn already_applied_task_is_settled_expired_instead_of_requeued() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task_id = processing_task(&store, &key_id, &request(addr, Some(7))).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::new([(addr, 8)]);
        let metrics = MetricsCollector::new();

        requeue_incomplete_tasks(
            &store,
            &sender,
            &depth,
            bound.as_ref(),
            &counts,
            Some(&metrics),
        )
        .await
        .expect("re-queue should succeed");

        assert!(
            metrics
                .encode()
                .contains("gas_killer_tasks_expired_at_requeue_total 1"),
            "the re-queue should count what it settled"
        );

        assert!(
            drained(&mut receiver).is_empty(),
            "an already-applied task must not be re-enqueued"
        );
        assert_eq!(depth.load(Ordering::Relaxed), 0);

        let task = store
            .get_task(&task_id)
            .await
            .expect("get_task should succeed")
            .expect("task should still exist");
        assert_eq!(task.status, TaskStatus::Expired);
        let reason = task.error.expect("expired task should carry a reason");
        assert!(
            reason.contains('7') && reason.contains('8'),
            "the reason should name both the task's index and the on-chain count, got {reason:?}"
        );
    }

    /// A contract still sitting at the task's index leaves the task re-queueable.
    #[tokio::test]
    async fn task_whose_index_is_still_current_is_requeued() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task_id = processing_task(&store, &key_id, &request(addr, Some(7))).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::new([(addr, 7)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert_eq!(drained(&mut receiver), vec![task_id.clone()]);
        assert_eq!(depth.load(Ordering::Relaxed), 1);
        let task = store
            .get_task(&task_id)
            .await
            .expect("get_task should succeed")
            .expect("task should still exist");
        assert_eq!(task.status, TaskStatus::Processing);
    }

    /// A server-resolved index is re-queued without a read.
    #[tokio::test]
    async fn auto_index_task_is_requeued_without_reading_the_chain() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task_id = processing_task(&store, &key_id, &request(addr, None)).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::new([(addr, 99)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert_eq!(drained(&mut receiver), vec![task_id]);
        assert!(
            counts.reads().is_empty(),
            "an auto-index task should cost no stateTransitionCount read"
        );
    }

    /// An unreadable chain re-queues every task, and stops reading once a run of targets has
    /// failed rather than retrying for each of them.
    #[tokio::test]
    async fn unreadable_chain_requeues_everything_and_latches_off() {
        let (store, key_id) = store_with_key().await;
        let mut expected = Vec::new();
        for byte in [0x11u8, 0x22, 0x33, 0x44, 0x55] {
            expected.push(processing_task(&store, &key_id, &request(target(byte), Some(7))).await);
        }

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::failing();

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed despite the reads failing");

        let mut drained_ids = drained(&mut receiver);
        drained_ids.sort();
        expected.sort();
        assert_eq!(
            drained_ids, expected,
            "every task should still be re-queued"
        );
        assert_eq!(depth.load(Ordering::Relaxed), 5);
        assert_eq!(
            counts.reads().len(),
            MAX_CONSECUTIVE_COUNT_FAILURES as usize,
            "a run of failures should stop the check rather than retry for every remaining task"
        );
    }

    /// One target the chain cannot place is not the chain being down, so the targets after it are
    /// still checked.
    #[tokio::test]
    async fn one_unreadable_target_does_not_disable_the_check_for_the_rest() {
        let (store, key_id) = store_with_key().await;
        let unreadable = target(0x11);
        let readable = target(0x22);
        let first = processing_task(&store, &key_id, &request(unreadable, Some(7))).await;
        let spent = processing_task(&store, &key_id, &request(readable, Some(7))).await;
        // The walk orders by `created_at, id`, and both rows land in the same second,
        // so backdate the unreadable one to pin it ahead of the readable one.
        backdate(&store, &first).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        // `readable` is scripted; `unreadable` is not, so its read errors.
        let counts = FakeCounts::new([(readable, 9)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert_eq!(
            drained(&mut receiver).len(),
            1,
            "only the unreadable target's task should be re-queued"
        );
        assert_eq!(
            status_of(&store, &spent).await,
            TaskStatus::Expired,
            "the readable target's spent index should still be caught"
        );
    }

    /// A row removed between the load and the settle is left alone rather than re-queued.
    #[tokio::test]
    async fn a_row_that_disappears_before_the_settle_is_not_requeued() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task_id = processing_task(&store, &key_id, &request(addr, Some(7))).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = DeletingReader {
            store: store.clone(),
            task_id: task_id.clone(),
            count: 9,
        };

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert!(drained(&mut receiver).is_empty());
        assert!(
            store
                .get_task(&task_id)
                .await
                .expect("get_task should succeed")
                .is_none()
        );
    }

    /// A settle the store refuses falls through to re-queueing, so the row is not left
    /// `processing` with nothing in the channel.
    #[tokio::test]
    async fn a_failed_settle_falls_through_to_requeueing() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task_id = processing_task(&store, &key_id, &request(addr, Some(7))).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = ClosingReader {
            store: store.clone(),
            count: 9,
        };

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed despite the settle failing");

        assert_eq!(drained(&mut receiver), vec![task_id]);
        assert_eq!(depth.load(Ordering::Relaxed), 1);
    }

    /// Many tasks against one contract cost one read.
    #[tokio::test]
    async fn one_read_per_distinct_target() {
        let (store, key_id) = store_with_key().await;
        let shared = target(0x11);
        let other = target(0x22);
        for _ in 0..3 {
            processing_task(&store, &key_id, &request(shared, Some(7))).await;
        }
        processing_task(&store, &key_id, &request(other, Some(7))).await;

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::new([(shared, 7), (other, 7)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert_eq!(drained(&mut receiver).len(), 4);
        let mut reads = counts.reads();
        reads.sort();
        let mut want = vec![shared, other];
        want.sort();
        assert_eq!(reads, want, "each target should be read exactly once");
    }

    /// A backlog larger than one page drains completely.
    #[tokio::test]
    async fn a_backlog_larger_than_one_page_is_fully_requeued() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let total = REQUEUE_PAGE_SIZE as usize + 5;
        for _ in 0..total {
            processing_task(&store, &key_id, &request(addr, Some(7))).await;
        }

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        // The contract still sits at the tasks' index, so none of them settle.
        let counts = FakeCounts::new([(addr, 7)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert_eq!(drained(&mut receiver).len(), total);
        assert_eq!(depth.load(Ordering::Relaxed), total);
        assert_eq!(
            counts.reads().len(),
            1,
            "the per-target cache should span pages"
        );
    }

    /// Settling rows removes them from the set the walk is reading, which is what an offset-paged
    /// walk cannot survive. Every row across every page must still be visited exactly once.
    #[tokio::test]
    async fn a_multi_page_backlog_settles_every_row_as_the_set_shrinks() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let total = REQUEUE_PAGE_SIZE as usize + 5;
        let mut ids = Vec::new();
        for _ in 0..total {
            ids.push(processing_task(&store, &key_id, &request(addr, Some(7))).await);
        }

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        // Every task's index is spent, so every row leaves the set as the walk reaches it.
        let counts = FakeCounts::new([(addr, 8)]);
        let metrics = MetricsCollector::new();

        requeue_incomplete_tasks(
            &store,
            &sender,
            &depth,
            bound.as_ref(),
            &counts,
            Some(&metrics),
        )
        .await
        .expect("re-queue should succeed");

        assert!(drained(&mut receiver).is_empty());
        for id in &ids {
            assert_eq!(
                status_of(&store, id).await,
                TaskStatus::Expired,
                "every row should be visited, including those past the first page"
            );
        }
        assert!(
            metrics.encode().contains(&format!(
                "gas_killer_tasks_expired_at_requeue_total {total}"
            )),
            "the counter should span pages"
        );
    }

    /// A `queued` row with a spent index settles `expired` on the same terms as a `processing` one.
    #[tokio::test]
    async fn queued_task_with_a_spent_index_is_also_settled_expired() {
        let (store, key_id) = store_with_key().await;
        let addr = target(0x11);
        let task = store
            .create_task(&key_id, &request(addr, Some(7)))
            .await
            .expect("task creation should succeed");

        let bound = store
            .last_incomplete_task()
            .await
            .expect("bound read should succeed");
        let (sender, mut receiver) = task_channel();
        let depth = task_queue_depth();
        let counts = FakeCounts::new([(addr, 12)]);

        requeue_incomplete_tasks(&store, &sender, &depth, bound.as_ref(), &counts, None)
            .await
            .expect("re-queue should succeed");

        assert!(drained(&mut receiver).is_empty());
        let settled = store
            .get_task(&task.id)
            .await
            .expect("get_task should succeed")
            .expect("task should still exist");
        assert_eq!(settled.status, TaskStatus::Expired);
    }
}
