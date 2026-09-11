//! Task source: pulls tasks off the ingress queue and enriches them (EVMSketch)
//! into [`GasKillerTaskData`] ready for aggregation.
//!
//! Height assignment, directive broadcast/rebroadcast, and resolution tracking are
//! generic and live in [`commonware_avs_router::sequencer`]; this module supplies
//! only the [`commonware_avs_router::sequencer::TaskSource`] implementation that
//! feeds it.

use crate::ingress::GasKillerTaskRequest;
use crate::metrics::MetricsCollector;
use crate::store::SqliteStore;
use commonware_avs_router::sequencer::{SequencedTask, TaskSource};
use gas_killer_common::GasKillerValidator;
use gas_killer_common::task_data::GasKillerTaskData;
use gas_killer_common::{PayloadView, TaskBundle};

use alloy_primitives::Bytes;
use anyhow::Result;
use commonware_cryptography::{Hasher, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info};

/// A task queued for the sequencer, carrying the persisted task id alongside the
/// request so status transitions can be attributed back to the right store row.
#[derive(Debug, Clone)]
pub struct QueuedTask {
    pub task_id: String,
    pub request: GasKillerTaskRequest,
}

pub type TaskSender = UnboundedSender<QueuedTask>;
pub type TaskReceiver = UnboundedReceiver<QueuedTask>;
/// Shared atomic counter tracking tasks in flight between the ingress sender and
/// the task source's receiver.
pub type TaskQueueDepth = Arc<AtomicUsize>;

pub fn task_channel() -> (TaskSender, TaskReceiver) {
    mpsc::unbounded_channel()
}

pub fn task_queue_depth() -> TaskQueueDepth {
    Arc::new(AtomicUsize::new(0))
}

/// Id of the task currently dispatched through the sequencer (dequeued but not yet
/// settled), shared between [`GasKillerTaskSource`] and [`crate::executor::GasKillerHandler`]
/// so a certified height's execution result can be attributed back to its task.
///
/// Exactly one task is ever in flight: the upstream [`commonware_avs_router::sequencer::Sequencer`]
/// drives one height at a time and only calls [`TaskSource::next_task`] again once the
/// current task resolves, so a single slot suffices — no keying by height or round
/// is needed.
///
/// `next_task` sets the slot when a task starts. `GasKillerHandler::handle_verification`
/// takes it when execution settles (ready or failed) — the only path that calls
/// `handle_verification` is a certificate carrying the task's own expected digest, so
/// a settling execution always belongs to the task presently in the slot. If the slot
/// is still occupied the next time `next_task` runs, the previous task's height was
/// skipped by the quorum instead — the one path that resolves a height without ever
/// calling `handle_verification` — and `next_task` settles it as failed.
pub type InFlightTask = Arc<Mutex<Option<String>>>;

pub fn in_flight_task() -> InFlightTask {
    Arc::new(Mutex::new(None))
}

/// Claims a dequeued task for this round, moving it to `processing`, and reports whether the
/// claim succeeded. `false` means the task has already settled — the TTL sweep expires tasks
/// while they sit in this channel — so the caller must drop it rather than spend a round
/// producing a payload too stale to land.
///
/// Best-effort task status bookkeeping shared by the task source and the executor: a store error
/// is logged rather than propagated, because failing to record a status transition must never
/// derail aggregation, which is the real work — a missed transition is recoverable (the startup
/// re-queue picks up anything left `queued` or `processing`), whereas aborting the pipeline is
/// not. An unreachable store therefore claims the task: the guard sheds doomed work, it is not
/// the correctness gate — the round's own on-chain validation still rejects a stale payload.
async fn claim_task_for_processing(store: &SqliteStore, task_id: &str) -> bool {
    match store.claim_task_for_processing(task_id).await {
        Ok(claimed) => claimed,
        Err(e) => {
            error!(task_id, error = %e, "failed to mark task processing");
            true
        }
    }
}

/// Settles a task ready, persisting both the rendered transaction-request `payload` and the
/// structured `bundle` it was derived from (each as JSON). A serialization failure is logged and
/// the transition skipped rather than propagated, following the best-effort convention above.
pub(crate) async fn set_task_ready(
    store: &SqliteStore,
    metrics: Option<&MetricsCollector>,
    task_id: &str,
    payload: &PayloadView,
    bundle: &TaskBundle,
) {
    let payload_json = match serde_json::to_string(payload) {
        Ok(json) => json,
        Err(e) => {
            error!(task_id, error = %e, "failed to serialize payload; task not marked ready");
            return;
        }
    };
    let bundle_json = match serde_json::to_string(bundle) {
        Ok(json) => json,
        Err(e) => {
            error!(task_id, error = %e, "failed to serialize bundle; task not marked ready");
            return;
        }
    };
    match store
        .mark_task_ready_with_bundle(task_id, &payload_json, &bundle_json)
        .await
    {
        Ok(elapsed) => observe_task_e2e(metrics, elapsed),
        Err(e) => error!(task_id, error = %e, "failed to mark task ready"),
    }
}

pub(crate) async fn set_task_failed(
    store: &SqliteStore,
    metrics: Option<&MetricsCollector>,
    task_id: &str,
    reason: &str,
) {
    match store.mark_task_failed(task_id, reason).await {
        Ok(elapsed) => observe_task_e2e(metrics, elapsed),
        Err(e) => error!(task_id, error = %e, "failed to mark task failed"),
    }
}

/// Records a settled task's end-to-end latency — ingress acceptance to terminal status — from the
/// elapsed seconds the settling statement reported. `None` means no task carried that id, so
/// there is nothing to time. A clock that moved backwards between the two timestamps is clamped
/// to zero rather than observed as a negative duration.
fn observe_task_e2e(metrics: Option<&MetricsCollector>, elapsed_secs: Option<i64>) {
    if let (Some(m), Some(secs)) = (metrics, elapsed_secs) {
        m.task_e2e_seconds.observe(secs.max(0) as f64);
    }
}

/// Enriched task data that includes computed storage updates and block height.
struct EnrichedTask {
    task: GasKillerTaskRequest,
    storage_updates: Bytes,
    block_height: u64,
    /// Resolved transition index (sentinel `None` → concrete count from chain).
    transition_index: u64,
    /// Actual EVM chain ID (e.g. 1 = Ethereum mainnet, 100 = Gnosis, 31337 = Anvil).
    chain_id: u64,
}

impl EnrichedTask {
    fn into_task_data(self) -> GasKillerTaskData {
        GasKillerTaskData {
            storage_updates: self.storage_updates,
            transition_index: self.transition_index,
            target_address: self.task.body.target_address,
            call_data: self.task.body.call_data,
            from_address: self.task.body.from_address,
            value: self.task.body.value,
            block_height: self.block_height,
            chain_id: self.chain_id,
        }
    }
}

/// Pulls tasks from the ingress queue and enriches them into [`GasKillerTaskData`]
/// for the aggregation [`commonware_avs_router::sequencer::Sequencer`].
pub struct GasKillerTaskSource {
    receiver: TaskReceiver,
    queue_depth: TaskQueueDepth,
    validator: Arc<GasKillerValidator>,
    metrics: Option<Arc<MetricsCollector>>,
    /// Durable store used to advance task status as work progresses. `None` in
    /// store-less test/dev harnesses, where status transitions are simply skipped.
    store: Option<SqliteStore>,
    /// Shared with [`crate::executor::GasKillerHandler`]; see [`InFlightTask`].
    in_flight: InFlightTask,
}

impl GasKillerTaskSource {
    pub fn new(
        receiver: TaskReceiver,
        queue_depth: TaskQueueDepth,
        validator: Arc<GasKillerValidator>,
        metrics: Option<Arc<MetricsCollector>>,
        store: Option<SqliteStore>,
        in_flight: InFlightTask,
    ) -> Self {
        Self {
            receiver,
            queue_depth,
            validator,
            metrics,
            store,
            in_flight,
        }
    }

    /// Blocks until a task arrives, maintaining the queue-depth metric.
    ///
    /// Returns `None` when the ingress side of the channel closed.
    async fn wait_for_task(&mut self) -> Option<QueuedTask> {
        let task = self.receiver.recv().await?;
        let depth = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1);
        if let Some(m) = &self.metrics {
            m.task_queue_depth.set(depth as i64);
        }
        Some(task)
    }

    /// Computes storage updates (EVMSketch) and resolves the transition index for a
    /// dequeued task. Lifted nearly verbatim from the old creator.
    async fn enrich(&self, task: GasKillerTaskRequest) -> Result<EnrichedTask> {
        info!(
            target = format!("{:?}", task.body.target_address),
            from = format!("{:?}", task.body.from_address),
            transition_index = ?task.body.transition_index,
            call_data_len = task.body.call_data.len(),
            "Sequencer received task"
        );

        if let Some(m) = &self.metrics {
            m.tasks_created.inc();
        }

        debug!(
            "Computing storage updates for target {}",
            task.body.target_address
        );

        // For explicit indices, run storage computation alone (count not needed).
        // For auto mode, run stateTransitionCount() concurrently with EVMSketch: the
        // count RPC call (~200ms) is fully hidden behind EVMSketch (seconds), so the
        // auto path adds zero observable latency compared to the explicit-index path.
        let (
            storage_updates,
            block_height,
            numeric_chain_id,
            resolved_transition_index,
            storage_elapsed,
        ) = if let Some(idx) = task.body.transition_index {
            let start = Instant::now();
            // compute_storage_updates_for_tx detects the chain, runs EVMSketch, and also
            // calls eth_chainId on the same RPC — returns the numeric chain ID directly.
            let (updates, height, chain_id) = self
                .validator
                .compute_storage_updates_for_tx(
                    task.body.target_address,
                    &task.body.call_data,
                    Some(task.body.from_address),
                    Some(task.body.value),
                    task.body.block_height,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Failed to compute storage updates: {}", e))?;
            (updates, height, chain_id, idx, start.elapsed())
        } else {
            // Timed from here, before chain detection, so both branches report the same span:
            // everything the sequencer does to turn a request into storage updates. The
            // explicit-index branch detects the chain inside
            // `compute_storage_updates_for_tx`, so starting the clock after detection here
            // would report a shorter interval for the same work.
            let start = Instant::now();
            // Detect chain once so all concurrent futures skip redundant eth_getCode probes.
            let chain_role = self
                .validator
                .detect_chain_for_address(task.body.target_address)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to detect chain: {}", e))?;
            let rpc_url = self
                .validator
                .sim_rpc_url_for_chain(chain_role)
                .ok_or_else(|| anyhow::anyhow!("No RPC URL for chain {}", chain_role))?
                .to_owned();

            info!(
                target_address = %task.body.target_address,
                chain = %chain_role,
                "Resolving auto transition_index concurrently with EVMSketch"
            );

            let count_validator = Arc::clone(&self.validator);
            let chain_id_validator = Arc::clone(&self.validator);
            let target = task.body.target_address;
            let count_fut = async move {
                count_validator
                    .get_state_transition_count_on_chain(target, chain_role)
                    .await
            };
            let storage_fut = async {
                self.validator
                    .analyze_transaction(
                        &rpc_url,
                        task.body.target_address,
                        &task.body.call_data,
                        Some(task.body.from_address),
                        Some(task.body.value),
                        task.body.block_height,
                    )
                    .await
                    .map(|r| (r.storage_updates, r.block_height))
            };
            // eth_chainId runs concurrently — completes in ~50ms, well before EVMSketch.
            let chain_id_fut = async move { chain_id_validator.get_chain_id_for(chain_role).await };
            let (count, (updates, height), chain_id) =
                tokio::try_join!(count_fut, storage_fut, chain_id_fut)?;

            info!(
                target_address = %task.body.target_address,
                chain = %chain_role,
                count,
                "Resolved auto transition_index from chain"
            );
            (updates, height, chain_id, count, start.elapsed())
        };

        if let Some(m) = &self.metrics {
            m.storage_computation_seconds
                .observe(storage_elapsed.as_secs_f64());
        }

        // Debug: Log hash of full storage_updates to detect differences vs validators
        let mut storage_hasher = Sha256::new();
        storage_hasher.update(&storage_updates);
        let storage_hash = storage_hasher.finalize();
        let storage_hash_hex = hex::encode(&storage_hash[..8]);
        info!(
            storage_updates_len = storage_updates.len(),
            storage_updates_hash = %storage_hash_hex,
            block_height = block_height,
            transition_index = resolved_transition_index,
            target_address = %task.body.target_address,
            target_function = %task.body.call_data.get(..4).map(hex::encode).unwrap_or_default(),
            chain_id = numeric_chain_id,
            "Sequencer computed storage updates"
        );

        Ok(EnrichedTask {
            task,
            storage_updates: storage_updates.into(),
            block_height,
            transition_index: resolved_transition_index,
            chain_id: numeric_chain_id,
        })
    }
}

impl GasKillerTaskSource {
    /// Settles the previous in-flight task as `failed` if the slot is still
    /// occupied — the quorum skipped that task's height without ever reaching
    /// `GasKillerHandler::handle_verification`. A no-op when the slot is already
    /// empty (the common case: the previous task settled normally).
    async fn settle_orphaned_task(&self) {
        if let Some(store) = &self.store
            && let Some(task_id) = self.in_flight.lock().ok().and_then(|mut slot| slot.take())
        {
            set_task_failed(
                store,
                self.metrics.as_deref(),
                &task_id,
                "aggregation height skipped by quorum",
            )
            .await;
        }
    }
}

#[async_trait::async_trait]
impl TaskSource<GasKillerTaskData> for GasKillerTaskSource {
    /// Dequeues the next ingress task and enriches it. Enrichment failures are
    /// logged, settled as `failed`, and dropped; the loop keeps waiting for the
    /// next task rather than shutting the sequencer down.
    async fn next_task(&mut self) -> Option<SequencedTask<GasKillerTaskData>> {
        loop {
            self.settle_orphaned_task().await;

            let QueuedTask { task_id, request } = self.wait_for_task().await?;

            // A task the expiry sweep settled while it waited here is dropped rather than
            // aggregated: its pinned block is stale enough that the round could not produce a
            // submittable payload, so the height goes to the next task instead.
            if let Some(store) = &self.store
                && !claim_task_for_processing(store, &task_id).await
            {
                info!(task_id, "task settled while queued, skipping dispatch");
                continue;
            }

            let enriched = match self.enrich(request).await {
                Ok(enriched) => enriched,
                Err(e) => {
                    error!(error = %e, task_id, "failed to enrich task, dropping request");
                    if let Some(store) = &self.store {
                        set_task_failed(
                            store,
                            self.metrics.as_deref(),
                            &task_id,
                            &format!("task enrichment failed: {e}"),
                        )
                        .await;
                    }
                    continue;
                }
            };
            let task_data = enriched.into_task_data();

            // The wire codec asserts the combined calldata + storage-updates size
            // (ingress only bounds the calldata; EVMSketch produces the updates),
            // so reject gracefully here instead of panicking mid-broadcast.
            if let Err(e) = task_data.validate() {
                error!(
                    error = %e,
                    task_id,
                    target = %task_data.target_address,
                    "enriched task exceeds wire limits, dropping request"
                );
                if let Some(store) = &self.store {
                    set_task_failed(
                        store,
                        self.metrics.as_deref(),
                        &task_id,
                        &format!("task exceeds wire limits: {e}"),
                    )
                    .await;
                }
                continue;
            }

            // Prime the validator's digest cache so the router automaton (and any
            // digest re-derivation) skips EVMSketch: this source already ran it.
            self.validator
                .prime_cache(&task_data, &task_data.storage_updates)
                .await;
            let digest = task_data.build_payload_hash(&task_data.storage_updates);

            // Announce WITHOUT the router's computed storage_updates: nodes
            // independently recompute them with EVMSketch (that is the whole trust
            // model — see GasKillerValidator::expected_digest_for_task), so shipping
            // them is both a validation-bypass smell and dead weight. Dropping the
            // ~700-byte diff shrinks the Announce ~4x, which is the difference between
            // it reliably fitting a single unreliable p2p frame and being dropped in
            // favor of the tiny Skip. The digest is unaffected (the node builds it
            // from its own recomputed updates).
            let announce = GasKillerTaskData {
                storage_updates: Bytes::new(),
                ..task_data.clone()
            };

            // Record this task as in flight so `GasKillerHandler::handle_verification`
            // can settle it once its height executes.
            if let Ok(mut slot) = self.in_flight.lock() {
                *slot = Some(task_id);
            }

            return Some(SequencedTask {
                task: task_data,
                announce,
                digest,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::TaskStatus;
    use alloy::primitives::{Address, B256, FixedBytes, U256};
    use gas_killer_common::BundleProof;

    fn sample_request(transition_index: Option<u64>) -> GasKillerTaskRequest {
        GasKillerTaskRequest {
            body: crate::ingress::GasKillerTaskRequestBody {
                target_address: Address::from([1u8; 20]),
                call_data: vec![0x12, 0x34, 0x56, 0x78],
                transition_index,
                from_address: Address::from([2u8; 20]),
                value: U256::from(1000),
                block_height: 12345,
            },
        }
    }

    #[tokio::test]
    async fn test_channel_send_recv() {
        let (sender, mut receiver) = task_channel();
        let queued = QueuedTask {
            task_id: "task-1".to_string(),
            request: sample_request(Some(1)),
        };

        sender.send(queued.clone()).unwrap();
        let received = receiver.try_recv().unwrap();
        assert_eq!(received.task_id, "task-1");
        assert_eq!(received.request.body.transition_index, Some(1));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn test_task_data_from_request() {
        let task = GasKillerTaskRequest {
            body: crate::ingress::GasKillerTaskRequestBody {
                target_address: Address::from([1u8; 20]),
                call_data: vec![0x12, 0x34, 0x56, 0x78],
                transition_index: Some(42),
                from_address: Address::from([2u8; 20]),
                value: U256::from(1000),
                block_height: 12345,
            },
        };

        let enriched = EnrichedTask {
            task,
            storage_updates: vec![0x01, 0x02, 0x03, 0x04].into(), // computed by GasAnalyzer
            block_height: 12345,
            transition_index: 42,
            chain_id: 1u64,
        };
        let task_data = enriched.into_task_data();

        assert_eq!(task_data.transition_index, 42);
        assert_eq!(task_data.target_address, Address::from([1u8; 20]));
        assert_eq!(task_data.chain_id, 1);
    }

    // -- task lifecycle transitions --

    async fn store() -> SqliteStore {
        SqliteStore::connect_in_memory()
            .await
            .expect("in-memory store should open and migrate")
    }

    async fn key_id(store: &SqliteStore) -> String {
        store
            .create_api_key(None, None)
            .await
            .expect("key creation should succeed")
            .id
    }

    fn request_body() -> crate::ingress::GasKillerTaskRequestBody {
        crate::ingress::GasKillerTaskRequestBody {
            target_address: Address::from([0x11; 20]),
            call_data: vec![0x12, 0x34, 0x56, 0x78],
            transition_index: Some(0),
            from_address: Address::from([0x22; 20]),
            value: U256::ZERO,
            block_height: 1,
        }
    }

    fn sample_payload() -> PayloadView {
        PayloadView {
            to: Address::from([0x11; 20]),
            data: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
            value: U256::ZERO,
            chain_id: 31337,
            estimated_gas: 21_000,
            valid_until_block: 100,
        }
    }

    fn sample_bundle() -> TaskBundle {
        TaskBundle {
            msg_hash: B256::ZERO,
            reference_block_number: 50,
            transition_index: 0,
            target_address: Address::from([0x11; 20]),
            target_function: FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]),
            storage_updates: Bytes::new(),
            chain_id: 31337,
            value: U256::ZERO,
            valid_until_block: 100,
            proof: BundleProof::Bls {
                quorum_numbers: Bytes::from(vec![0x00]),
                non_signer_stakes_and_signature: Bytes::new(),
            },
        }
    }

    fn unreachable_validator() -> Arc<GasKillerValidator> {
        // Nothing listens on this port; RPC calls fail fast with connection refused
        // rather than hanging, so `enrich` errors quickly and deterministically.
        Arc::new(GasKillerValidator::with_rpc_url("http://localhost:8545"))
    }

    #[tokio::test]
    async fn set_helpers_persist_status_transitions() {
        let store = store().await;
        let key = key_id(&store).await;
        let done = store.create_task(&key, &request_body()).await.unwrap();
        let doomed = store.create_task(&key, &request_body()).await.unwrap();

        assert!(claim_task_for_processing(&store, &done.id).await);
        assert_eq!(
            store.get_task(&done.id).await.unwrap().unwrap().status,
            TaskStatus::Processing
        );

        set_task_ready(&store, None, &done.id, &sample_payload(), &sample_bundle()).await;
        let ready = store.get_task(&done.id).await.unwrap().unwrap();
        assert_eq!(ready.status, TaskStatus::Ready);
        // Both the rendered payload and the structured bundle are persisted as JSON.
        let payload: PayloadView = serde_json::from_str(ready.payload.as_deref().unwrap()).unwrap();
        assert_eq!(payload, sample_payload());
        let bundle: TaskBundle = serde_json::from_str(ready.bundle.as_deref().unwrap()).unwrap();
        assert_eq!(bundle, sample_bundle());

        set_task_failed(&store, None, &doomed.id, "boom").await;
        let failed = store.get_task(&doomed.id).await.unwrap().unwrap();
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(failed.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn settling_a_task_records_its_end_to_end_latency() {
        let store = store().await;
        let key = key_id(&store).await;
        let metrics = MetricsCollector::new();
        let ready = store.create_task(&key, &request_body()).await.unwrap();
        let failed = store.create_task(&key, &request_body()).await.unwrap();

        // Both terminal transitions feed the histogram: a client waits just as long for a failure
        // as for a payload, so leaving failures out would flatter the observed latency.
        set_task_ready(
            &store,
            Some(&metrics),
            &ready.id,
            &sample_payload(),
            &sample_bundle(),
        )
        .await;
        set_task_failed(&store, Some(&metrics), &failed.id, "boom").await;

        let output = metrics.encode();
        assert!(
            output.contains("gas_killer_task_e2e_seconds_count 2"),
            "both settled tasks should be observed, got:\n{output}"
        );

        // Settling a task that does not exist reports no latency, so nothing is observed for it.
        set_task_failed(&store, Some(&metrics), "no-such-task", "boom").await;
        assert!(
            metrics
                .encode()
                .contains("gas_killer_task_e2e_seconds_count 2"),
            "a settle that matched no task must not be observed"
        );
    }

    #[tokio::test]
    async fn settle_orphaned_task_marks_failed_and_clears_slot() {
        let store = store().await;
        let key = key_id(&store).await;
        let task = store.create_task(&key, &request_body()).await.unwrap();

        let in_flight = in_flight_task();
        *in_flight.lock().unwrap() = Some(task.id.clone());

        let (_sender, receiver) = task_channel();
        let source = GasKillerTaskSource::new(
            receiver,
            task_queue_depth(),
            unreachable_validator(),
            None,
            Some(store.clone()),
            in_flight.clone(),
        );

        source.settle_orphaned_task().await;

        assert!(in_flight.lock().unwrap().is_none());
        let settled = store.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
        assert_eq!(
            settled.error.as_deref(),
            Some("aggregation height skipped by quorum")
        );
    }

    #[tokio::test]
    async fn settle_orphaned_task_is_noop_when_slot_empty() {
        let store = store().await;
        let (_sender, receiver) = task_channel();
        let source = GasKillerTaskSource::new(
            receiver,
            task_queue_depth(),
            unreachable_validator(),
            None,
            Some(store),
            in_flight_task(),
        );

        // Must not panic when there is nothing in flight to settle.
        source.settle_orphaned_task().await;
    }

    #[tokio::test]
    async fn next_task_marks_processing_then_failed_on_enrich_error() {
        let store = store().await;
        let key = key_id(&store).await;
        let task = store.create_task(&key, &request_body()).await.unwrap();

        let (sender, receiver) = task_channel();
        let mut source = GasKillerTaskSource::new(
            receiver,
            task_queue_depth(),
            unreachable_validator(),
            None,
            Some(store.clone()),
            in_flight_task(),
        );

        sender
            .send(QueuedTask {
                task_id: task.id.clone(),
                request: GasKillerTaskRequest {
                    body: request_body(),
                },
            })
            .unwrap();
        // Closing the sender lets `next_task` observe a closed channel (and return
        // `None`) once the one queued task's enrichment fails and it loops back
        // for the next task, instead of blocking forever.
        drop(sender);

        assert!(source.next_task().await.is_none());

        let settled = store.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
        assert!(
            settled
                .error
                .as_deref()
                .is_some_and(|e| e.contains("task enrichment failed"))
        );
    }

    /// The claim is the handshake between the expiry sweep and dispatch: once a task has settled,
    /// the round it was queued for must go to other work.
    #[tokio::test]
    async fn claim_refuses_a_task_that_already_settled() {
        let store = store().await;
        let key = key_id(&store).await;
        let expired = store.create_task(&key, &request_body()).await.unwrap();
        store
            .mark_task_expired(&expired.id, "QUEUE_TTL_EXCEEDED")
            .await
            .unwrap();

        assert!(!claim_task_for_processing(&store, &expired.id).await);
        assert_eq!(
            store.get_task(&expired.id).await.unwrap().unwrap().status,
            TaskStatus::Expired,
            "a refused claim must not resurrect the task"
        );
        assert!(
            !claim_task_for_processing(&store, "no-such-task").await,
            "an unknown id is not claimable"
        );
    }

    #[tokio::test]
    async fn next_task_skips_a_task_expired_while_queued() {
        let store = store().await;
        let key = key_id(&store).await;
        let task = store.create_task(&key, &request_body()).await.unwrap();
        store
            .mark_task_expired(&task.id, "QUEUE_TTL_EXCEEDED: expired by the sweep")
            .await
            .unwrap();

        let (sender, receiver) = task_channel();
        let mut source = GasKillerTaskSource::new(
            receiver,
            task_queue_depth(),
            unreachable_validator(),
            None,
            Some(store.clone()),
            in_flight_task(),
        );

        sender
            .send(QueuedTask {
                task_id: task.id.clone(),
                request: GasKillerTaskRequest {
                    body: request_body(),
                },
            })
            .unwrap();
        // As above: closing the sender lets `next_task` return once it has skipped the task
        // rather than blocking for another.
        drop(sender);

        assert!(source.next_task().await.is_none());

        // Untouched: not re-dispatched (which enrichment would have settled as `failed` against
        // the unreachable validator) and still carrying the sweep's reason.
        let settled = store.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(settled.status, TaskStatus::Expired);
        assert_eq!(
            settled.error.as_deref(),
            Some("QUEUE_TTL_EXCEEDED: expired by the sweep")
        );
    }
}
