# Gas Killer

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org)
[![Docker](https://img.shields.io/badge/docker-ghcr.io/gas--killer/service-blue.svg)](https://github.com/gas-killer/service/pkgs/container/service)

Gas Killer service implementation built on EigenLayer with BLS signature aggregation for optimized transaction execution.

## Overview

The service coordinates multiple operator nodes to sign task digests, assembles their BN254 signatures into quorum certificates via the commonware-consensus aggregation engine, and executes the result onchain via `verifyAndUpdate`.

## Repository Structure

- **`router/`** — Router service: sequences tasks, assembles certificates (verifier-only engine), and executes onchain
- **`node/`** — Operator node: validates and signs tasks
- **`common/`** — Shared types, validation logic, and EVM gas analysis
- **`config/`** — Operator and orchestrator key/config files
- **`scripts/`** — Helper binaries for deployment and end-to-end testing
- **`helm/`** — Kubernetes Helm chart for full-stack deployment
- **`docker-compose.yml`** — One-command local deployment

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Git

### Local Development

1. **Configure environment:**
```bash
cp example.env .env
```

The example.env is pre-configured for LOCAL mode with Anvil test keys. No changes are needed to run locally.

2. **Start all services:**
```bash
docker compose up -d
```

This will automatically pull the latest pre-built images from the GitHub Container Registry (ghcr.io) and start:
- Ethereum node (Anvil fork of Sepolia)
- EigenLayer contract deployment
- 3 operator nodes
- Router/orchestrator
- Signer service (Cerberus)

3. **Monitor services:**
```bash
# View logs
docker compose logs -f router

# Check service status
docker compose ps
```

### Stop Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (clean state)
docker compose down -v
```

### Building from Source (Development Only)

If you're developing locally and want to test changes:

```bash
# Build the router image
docker build -t ghcr.io/gas-killer/service:router-local -f router/Dockerfile .

# Build the node image
docker build -t ghcr.io/gas-killer/service:node-local -f node/Dockerfile .

# Run with locally built images
docker compose up -d
```

## Architecture

Aggregation is built on the `commonware-consensus` **aggregation engine** with a custom
**BN254 attributable multisig scheme** (signer bitmap + one aggregated G1 signature per
certificate). The previous hand-rolled star-topology aggregator and all
`commonware-avs-*` / `commonware-restaking` git dependencies were dropped; the pieces
still required (BN254 key/curve handling, the EigenLayer staking client, the
`BLSApkRegistry`/`BLSSigCheckOperatorStateRetriever` bindings, and the
`NonSignerStakesAndSignature` retrieval flow) are vendored into `common/`. The on-chain
verification path (`GasKillerSDK.verifyAndUpdate` + `BLSSignatureChecker`) is unchanged.

```
                         POST /tasks
                             │
                     router: HTTP ingress
                             │
                     router: sequencer
        assigns the task the next height H and broadcasts
        TaskDirective::Announce{H, task} on p2p channel 1
        (rebroadcast until certified; Skip{H} after ROUND_TIMEOUT)
                             │
  ┌──────────────────────────┼───────────────────────────┐
  │ node 1..N (signing participants)                     │ router (verifier-only)
  │  aggregation engine, p2p channel 0:                  │  aggregation engine:
  │   propose(H): wait for the directive for H,          │  validates acks, assembles a
  │   validate the task via EVMSketch, sign the          │  BN254 certificate at quorum
  │   expected digest (or the skip digest) with          │            │
  │   the node's BN254 key, gossip TipAcks               │  submitter: bitmap → operators,
  └──────────────────────────────────────────────────────┘  getNonSignerStakesAndSignature,
                                                            GasKillerSDK.verifyAndUpdate
```

- **Sequencer** (router): dequeues ingress tasks, computes the expected storage updates
  via EVMSketch, and assigns each task the next aggregation height. Exactly one height
  is outstanding at a time; the next task is assigned only after the current height
  certifies (and, for real digests, on-chain execution finishes).
- **Task-directive channel** (p2p channel 1): the router broadcasts
  `TaskDirective::Announce { height, task }` and, after `ROUND_TIMEOUT`,
  `TaskDirective::Skip { height }`. Nodes only receive on this channel. Engine `TipAck`
  gossip runs on channel 0.
- **Aggregation engine**: every process runs
  `commonware_consensus::aggregation::Engine`. Nodes run it as signing participants;
  the router runs it verifier-only (it holds no share of any signing key) — it
  validates the nodes' acks, assembles certificates at quorum, journals them, and
  hands them to the submitter.
- **BN254 multisig scheme**: nodes sign the raw 32-byte task digest with EigenLayer's
  `map_to_curve` hashing and **no namespace** — byte-identical to the pre-migration
  signatures — so certificates verify on-chain against the operators' registered BN254
  keys. The certificate binds only the digest, not the height; the digest itself binds
  `(transitionIndex, target, selector, storageUpdates)` and the contract enforces
  transition-index ordering, so replaying an identical digest across heights is
  harmless.
- **Submitter** (router): maps the certificate's signer bitmap to operator G1 keys,
  fetches `NonSignerStakesAndSignature` from `BLSSigCheckOperatorStateRetriever`, and
  calls `GasKillerSDK.verifyAndUpdate`.
- **Validator** (`common/`): EVM gas analysis (EVMSketch) computing the storage
  updates and the expected task digest on both router and nodes.

### Quorum model

The consensus engine fixes the certificate quorum at `n - floor((n - 1) / 3)` of the
`n` registered operators (Byzantine fault model, f = floor((n-1)/3)):

| n | quorum | tolerates |
|---|--------|-----------|
| 3 | 3-of-3 | 0 faulty  |
| 4 | 3-of-4 | 1 faulty  |
| 7 | 5-of-7 | 2 faulty  |

This is stricter than the old `ceil(2n/3)` threshold for n=3: **with 3 operators every
node must sign every certificate, so a single offline or divergent node halts
certification until it recovers.** Run **n ≥ 4 operators in production**. The old
`THRESHOLD` override is gone; the on-chain stake-fraction check
(`QUORUM_THRESHOLD`/`THRESHOLD_DENOMINATOR`) still runs in the contract at submission.

### Journal storage

Each process needs a writable directory (`STORAGE_DIR`) for the engine's journal — a
write-ahead log of acks and certificates replayed on restart. docker-compose mounts a
named volume per service at `/app/data`; the Helm chart mounts an `emptyDir`. A node
that loses its journal forgets what it acked (safe: it re-signs the same digests); the
router's journal only caches certificates and is likewise safe to lose. Without any
writable directory the binaries fall back to `$TMPDIR/gas-killer`, which is for
bare-metal dev runs only.

### Failure modes and recovery

The engine's tip advances only when heights certify contiguously — **every height the
router assigns must eventually resolve to a certificate**, either the task digest or
the skip digest. The router rebroadcasts `Announce` every `REBROADCAST_INTERVAL`,
switches to `Skip` after `ROUND_TIMEOUT`, and keeps rebroadcasting until the height
certifies. Nodes sign the skip digest only when told to (`Skip` directive) or when the
router has demonstrably moved past the height (a directive for a later height exists
while this one has none) — never on a bare timer.

- **Offline node (n=3)**: quorum is 3-of-3, so certification stalls until the node
  returns; acks are re-gossiped and the pipeline resumes on its own.
- **Split-digest stall**: if signers split between the task digest and the skip digest
  at the same height such that neither reaches quorum (with n=3 any single divergent
  signer is fatal; with n=4, two), the pipeline wedges at that height and **does not
  recover automatically**. Manual recovery: stop all node processes, wipe each node's
  engine journal — `docker compose down` + `docker volume rm <project>_node-{1,2,3}-data`,
  or delete the node pods in Kubernetes (journals are `emptyDir`) — and restart. Nodes
  then re-propose the height with no memory of their previous acks and follow the
  router's current directive. Running n ≥ 4 makes a single divergent signer non-fatal.
- **Router restart**: the engine replays its certificate journal, the sequencer waits
  a short settle delay, then resumes from the observed tip. A certificate whose digest
  matches neither the expected digest nor the skip digest (a node resolved the height
  from a directive issued by a previous router life) consumes the height and the
  in-flight task is re-assigned to the next one. Tasks still `queued` or `processing`
  are re-queued, except one whose `transition_index` the contract has already consumed.
  That settles `expired`, counted in `gas_killer_tasks_expired_at_requeue_total`.
- **Router journal loss**: if the router's journal is wiped while the nodes keep
  theirs (e.g. only the router pod is rescheduled), the sequencer would restart at
  height 0 — below heights the nodes will ever propose again. Nodes detect directives
  below their own tip and reply with a rate-limited `TipReport`; the router takes the
  `(f+1)`-th highest reported tip (the same trust rule as the engine's safe-tip) and
  fast-forwards its next assignment, re-assigning the in-flight task there.
- **Operator-set changes**: the participant set (and therefore every participant
  index) is frozen per process at startup from the on-chain registry. Registering or
  deregistering an operator requires restarting the router and all nodes together —
  processes with different participant sets reject (and eventually disconnect) each
  other's acks.

## Configuration

### Environment Variables

Required environment variables:
- `ENVIRONMENT`: `LOCAL` or `TESTNET`
- `HTTP_RPC`: HTTP RPC endpoint
- `WS_RPC`: WebSocket RPC endpoint
- `AVS_DEPLOYMENT_PATH`: Path to deployment JSON file
- `PRIVATE_KEY`: Private key for transactions
- `FUNDED_KEY`: Funded key for testnet ETH (required for `TESTNET` mode)

LOCAL-mode-only:
- `FORK_URL`: Sepolia RPC URL to fork from (Anvil uses this)

Optional environment variables:
- `STORAGE_DIR`: Writable directory for the aggregation engine's journal (default: `/app/data` if writable, else `$TMPDIR/gas-killer`). docker-compose and Helm mount a dedicated volume here — see "Journal storage" above.
- `AGG_WINDOW`: Heights the aggregation engine works on concurrently above its tip (default: 8).
- `AGG_ACTIVITY_TIMEOUT`: Heights below the tip the engine keeps tracking before pruning (default: 256). Keep generous — heights pruned past this window can never certify locally.
- `ROUND_TIMEOUT`: Max seconds the router waits for a certificate on its assigned height before switching from `Announce` to `Skip` broadcasts (accepts fractional seconds). Also the nodes' retry budget for transient validation errors. The engine certifies as soon as quorum signs, so this only affects heights that stall. Library default: 30; Helm deployments set 300. Must exceed worst-case node compute + sign time.
- `REBROADCAST_INTERVAL`: How often (in seconds) the router re-sends the in-flight `TaskDirective` until the height certifies (accepts fractional seconds); also the engine's internal `TipAck` rebroadcast timeout. Library default: 5; Helm deployments set 15. Must stay well below `ROUND_TIMEOUT`: a node that misses every `Announce` can only resolve the height as a skip.
- `INGRESS`: Enable HTTP ingress mode (true/false)
- `INGRESS_ADDRESS`: Address for ingress server (default: 0.0.0.0:8080)
- `INGRESS_TIMEOUT_MS`: Timeout for waiting on ingress tasks in milliseconds (default: 0, no timeout)
- `ADMIN_KEY`: Shared secret guarding the `/admin/keys` endpoints, used to mint and revoke the per-client API keys that authenticate `POST /tasks`. Omit or leave empty to disable the admin API.
- `RATE_LIMIT_RPM`: Default per-API-key request rate on `POST /tasks`, in requests per minute (default: 60). Applies to every key without a per-key override (set at creation via the `rpm_limit` field of `POST /admin/keys`). Over-limit requests get `429 Too Many Requests` with a `Retry-After` header. Counters are in-memory per router process and reset on restart.
- `AVS_REFERENCE_TARGET`, `AVS_REFERENCE_TARGET_FILE`, `DEMO_TARGET_ADDRESS`, `DEMO_TARGET_FILE`, `DEMO_FACTORY_ADDRESS`, `DEMO_FACTORY_FILE`: feed the `contracts` block on `GET /avs-metadata` — `chainId`, `avsAddress`, `blsSignatureChecker`, `registryCoordinator`, plus the demo contracts the docs point at. Publishing the live pair is what stops a target being wired to a superseded checker, which passes every router-side check and then reverts `InvalidQuorumApkHash` on chain.

  | Variable | Sets |
  |---|---|
  | `AVS_REFERENCE_TARGET` | Target whose `avsAddress()`/`blsSignatureChecker()` getters establish the published pair |
  | `AVS_REFERENCE_TARGET_FILE` | File holding that address — the chart points this at the deploy job's record |
  | `DEMO_TARGET_ADDRESS` / `DEMO_FACTORY_ADDRESS` | `demoTarget` / `demoFactory` |
  | `DEMO_TARGET_FILE` / `DEMO_FACTORY_FILE` | Files holding those — the chart points these at the playground job's records |

  Reference-target precedence: pin → playground record → deploy-job record → `demo_target.txt` beside `avs_deploy.json`; the playground record wins so the published checker is the one `demoTarget.blsSignatureChecker()` returns. `registryCoordinator` comes from `avs_deploy.json` and is cross-checked against the checker's own — on mismatch the block is omitted. Addresses publish EIP-55 checksummed so they paste into Solidity; a demo address with no code on the published chain is dropped. `example.env` covers resolution and retry behaviour.
- `RPC_FAILURE_THRESHOLD`: consecutive RPC failures against one chain before that chain is marked unavailable (default: 5). A down provider breaks the whole task lifecycle — validation, analysis, and submission all read the chain — so on reaching the threshold the router sets `gas_killer_rpc_healthy{chain="l1|l2"}` to 0, logs the transition at `WARN` with the chain, failure count, and timestamp, and refuses new submissions with `503 RPC_UNAVAILABLE` rather than queueing work that cannot complete. Only L1 sheds traffic: every round's certificate is anchored to an L1 reference block, so an L1 outage blocks submissions whichever chain the target runs on, while a degraded L2 is alerted but leaves L1 targets alone. The count is a run, not a total — a single success clears it. Every chain read on the submission and freshness paths reports its outcome, and a background probe (`eth_blockNumber`, every 15s) checks each chain independently, so the gauge reflects reality on an idle router and a recovered provider clears the breaker without waiting for traffic.
- `TASK_TTL_SECONDS`: How long a task may sit without reaching a terminal state before a background sweep settles it as `expired` (default: 600, the wall-clock life of a rendered payload on L1 — `PAYLOAD_BLOCK_BUFFER` blocks at a 12s slot time — so the sweep never withdraws a payload the chain would still accept; a target chain with faster blocks has a proportionally shorter payload window and can run a shorter TTL). A `queued` task past its TTL is expired with `QUEUE_TTL_EXCEEDED` — its pinned `block_height` has gone stale, so aggregating it would only produce a payload the contract rejects — and a `ready` payload nobody collected is expired with `READY_TTL_EXCEEDED` on the same TTL, counted from when aggregation recorded it, which frees the deduplication slot for its transition index. A `processing` task is never cancelled mid-round: its height is already assigned and must resolve either way. The sweep runs every 60 seconds and counts what it settles in `gas_killer_tasks_expired_total`.
- `INGRESS_STALENESS_WINDOW_BLOCKS`: How far behind the target chain's head a submitted `block_height` may be and still be admitted; past it `POST /tasks` returns `400 STALE_BLOCK`. Defaults to `BLOCK_STALE_MEASURE - PAYLOAD_BLOCK_BUFFER` (250 at the stock 300/50), floored at 1 block — a task's analysis is anchored at its `block_height`, and both the aggregation round and the rendered payload's validity window have to fit in what is left of the contract's staleness window, so admission holds the payload buffer back instead of accepting work that can only just finish simulating. An explicit value above `BLOCK_STALE_MEASURE` is clamped to it (and warned about at startup); `0` disables the check entirely. The effective window is logged at startup.
- `QUORUM_NUMBER`: Quorum number to use (default: 0)
- `P2P_ACK_MESSAGES_PER_SECOND`: Per-peer rate for the engine's TipAck channel. Defaults to `2 * AGG_ACTIVITY_TIMEOUT / REBROADCAST_INTERVAL + 8`, sized so steady-state ack rebroadcast never hits the p2p limiter (which silently drops over-rate messages). Only override to constrain bandwidth. The legacy `P2P_MESSAGES_PER_SECOND` knob now only governs the task-directive channel.

Removed after the aggregation migration:
- `THRESHOLD`: the minimum-signature override no longer exists — the quorum is fixed by the consensus engine at `n - floor((n-1)/3)` (see "Quorum model" above).

Operator (node) key files are generated automatically by the Docker setup and do not need to be set manually.

### Metrics

Both binaries serve Prometheus text exposition on `/metrics` at `HEALTHZ_PORT` (default 8081):
the commonware runtime's own registries, then the process's custom metrics, then its
configuration fingerprint. The Helm chart scrapes both at 15s and dashboards them.

Metrics from the runtime registries are named after the subsystem that registered them —
`engine_*` for the aggregation engine (both binaries), `reporter_*` for an operator's
certificate reporter, `network_*` for p2p. The router's own metrics are all prefixed
`gas_killer_`.

The pipeline's shape, as opposed to the cost of one round:

| Metric | Meaning |
|---|---|
| `gas_killer_in_flight_heights` | Heights assigned and not yet resolved |
| `gas_killer_window_base`, `gas_killer_highest_assigned_height` | Edges of the live window. Both pinned while work is queued is a wedge |
| `gas_killer_height_age_seconds` | Age of the oldest unresolved height |
| `gas_killer_height_outcomes_total{outcome}` | Final disposition per height: `executed`, `skipped`, `foreign`, `superseded` |
| `gas_killer_node_safe_tip` | Tip floor from the operators' reports; explains a `superseded` spike |
| `gas_killer_directive_sends_total{result}` | Per-recipient directive delivery: `delivered`, `rate_limited`, `rejected` |
| `gas_killer_settlement_conflicts_total` | Terminal-state transitions the store refused. Must be 0 |
| `gas_killer_config_fingerprint{fingerprint}` | Always 1, labelled with this process's consensus-critical config |
| `engine_tip` | The engine's contiguous tip: the lowest height without a certificate |
| `reporter_certified_total`, `reporter_skipped_total` | What the operators actually signed |
| `network_spawner_messages_rate_limited_total{peer,message}` | Messages the *receiving* peer throttled, by channel (`data_0` acks, `data_1` directives, `data_2` Schnorr) |

Two of these need reading together rather than alone.

`gas_killer_directive_sends_total` and `network_spawner_messages_rate_limited_total` are opposite
ends of the same channel and neither substitutes for the other. The send-side counter exists
because the p2p sender returns the peers it will attempt and silently omits the ones over quota,
so a partial drop and a full delivery are indistinguishable at the call site — and a dropped
`Announce` is how an operator ends up voting to skip a height everyone else signed. The
receive-side counter is the peer's own view, where a throttled message is not dropped but sleeps
the entire connection, blocking every channel on it.

`gas_killer_config_fingerprint` is a hash of the settings that must match across the router and
every operator: `GK_SIM_PROFILE`, `STATE_ENCODING`, `SIGNATURE_SCHEME`, the application
namespace, `AGG_WINDOW`, and the directive wire version. A fleet that disagrees on any of them
does not fail loudly — peers stay connected, quorum never forms, and every pod reports healthy —
so `count(count by (fingerprint) (gas_killer_config_fingerprint))` must be exactly 1. It is also
the pre-flight check for a rolling upgrade: none of these may be changed on a live fleet.

## Ingress Mode

Enable HTTP endpoints for external task requests. These endpoints are specified in
[`router/docs/openapi.json`](#regenerating-the-openapi-documents).

1. **Enable ingress in .env:**
```bash
INGRESS=true
```

2. **Restart the router:**
```bash
docker compose restart router
```

3. **Submit tasks via HTTP:**
```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "body": {
      "target_address": "0x0000000000000000000000000000000000000001",
      "from_address": "0x0000000000000000000000000000000000000002",
      "call_data": [171, 205, 239, 1],
      "transition_index": 0,
      "value": "0x0",
      "block_height": 1
    }
  }'
```

Note: `call_data` is a JSON array of bytes (not a hex string), `value` is a U256 hex string, and `block_height` must be non-zero.

When the router has a persistent store (the default), `POST /tasks` requires a valid API key, minted
through the admin API (`POST /admin/keys`) using `ADMIN_KEY`. The raw key is returned exactly
once. Locally (docker-compose) the admin API is on `localhost:8080`:
```bash
curl -X POST http://localhost:8080/admin/keys \
  -H "Authorization: Bearer <ADMIN_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"label": "my-client", "invalid_at": 1893456000}'   # invalid_at optional; unix ts, future
# → {"id":"...","key":"gk_...","label":"my-client","created_at":...,"invalid_at":1893456000}
```

Add `rpm_limit` (requests per minute) to the create body to give a key a custom rate; omit it to
use the global `RATE_LIMIT_RPM` default. Each key is rate-limited on `POST /tasks`: over-limit requests
return `429 Too Many Requests` with a `Retry-After` header (seconds until the next request is
allowed). The limiter is a token bucket, not a strict rolling window — a key may burst up to its
full per-minute allowance at once, after which requests refill at roughly `rpm / 60` per second.
Counters are in-memory per router process and reset on restart.

On a Kubernetes deployment the `/admin/*` endpoints are **not** exposed through the public Ingress
(only `/tasks`, `/avs-metadata`, `/healthz` are — see `ingress.publicPaths` in the chart).
Reach them in-cluster. The `create_api_key` tool ships in the router image, defaults to the
in-cluster `http://localhost:8080`, and reads `ADMIN_KEY` from the pod env — so `kubectl exec`
needs no target flag:
```bash
POD=$(kubectl get pods -l app.kubernetes.io/component=router -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD" -- create_api_key --label my-client --expires-at "7 days"
# fallback if the binary predates the image: curl localhost:8080/admin/keys with $ADMIN_KEY
```
Or `kubectl port-forward svc/<release>-router 8080:8080` and run the tool locally, reading
`ADMIN_KEY` from the Secret (it still defaults to `http://localhost:8080`):
```bash
ADMIN_KEY=$(kubectl get secret <release>-secret -o jsonpath='{.data.ADMIN_KEY}' | base64 -d) \
  create_api_key --label my-client --expires-at "7 days"
```
`--expires-at` accepts `never`, a relative duration like `7 days`, or a unix timestamp. The tool's
`--env prod`/`--env testnet` shortcuts target the public hostnames, so they work only if you have
deliberately added `/admin` to `ingress.publicPaths` — otherwise they 404 at the edge.

Then include the minted key as the Bearer token on task requests (revoke via the same in-cluster
admin path when no longer needed):
```bash
curl -X POST https://<host>/tasks \
  -H "Authorization: Bearer gk_..." \
  -H "Content-Type: application/json" \
  -d '...'
```

Use the `send_request` script for a complete end-to-end run against an ArraySummation contract.
Set `GAS_KILLER_API_KEY` to a minted key when the router requires auth, and
`GAS_KILLER_TASKS_URL` to submit somewhere other than the default
`http://localhost:8080/tasks`:
```bash
GAS_KILLER_API_KEY=gk_... cargo run -p scripts --bin send_request
GAS_KILLER_TASKS_URL=https://<host>/tasks GAS_KILLER_API_KEY=gk_... cargo run -p scripts --bin send_request
```

## Development

### Dependencies
- `alloy`: Ethereum interaction
- `commonware-consensus`: Aggregation engine (certificate assembly over heights)
- `commonware-cryptography`: Cryptographic primitives and the certificate `Scheme` trait implemented by our BN254 multisig scheme
- `commonware-p2p`: P2P networking
- `commonware-runtime`: Runtime utilities and journal-backed storage
- `gas-analyzer-evmsketch`: EVM gas analysis and storage update computation
- `eigen-*` / `ark-*`: EigenLayer SDK and BN254 curve arithmetic

The former `commonware-avs-*` (`commonware-restaking`) git dependencies were dropped in
the aggregation migration; the parts still needed are vendored under `common/src/`
(`bn254/`, `eigenlayer.rs`, `bindings/`).

### Code Quality
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Regenerating the OpenAPI documents

Generated from the `#[utoipa::path]` annotations on the ingress handlers. Rewrite after changing an
annotation or a type it exposes:

```bash
cargo run --bin openapi
```

- `router/docs/openapi.json`: the integrator API, rendered by the docs site.
- `router/docs/openapi.internal.json`: the same, plus `/admin/keys` and the operator port's
  `/healthz`, `/readyz` and `/metrics`.

The generator drops `Admin`- and `Health`-tagged operations from the published document, along
with the credential and types only they use, so the operator surface cannot reach the public
playground.

`cargo test` gates all of it: both committed documents against what the code produces, the
operator surface staying out of the published one, and each document's internal consistency.

### Testing

Run unit tests:
```bash
cargo test --lib
```

Run end-to-end tests:
```bash
chmod +x scripts/run_e2e_test.sh
./scripts/run_e2e_test.sh
```
