# Enabling `GK_SIM_PROFILE=unbounded` on testnet

Runbook for clearing a transaction whose tracked function costs more gas than a block can hold.
Assumes a trusted operator set: the fraud-proof path (SP1 guest, slasher) is **not** on this path
and nothing here depends on it.

## Why a fork is needed at all

`SimProfile::Unbounded` simulates the tracked function under pinned `2^40` block and tx gas
limits. The node serving `debug_traceCall` has the last word on that, and hosted providers clamp
it — silently.

Measured on the live Alchemy Sepolia endpoint, same call, same block, consecutive requests, reading
the root frame's granted gas:

```
600,000,000 / 575,000,000 / 600,000,000 / 60,000,000 / 50,000,000 / 18,446,744,073,709,551,615
```

Sending exactly what `Unbounded` sends (`tx.gas = 2^40`, `blockOverrides.gasLimit = 2^40`) is
granted 575–600M and reverts, with no error. A clamped trace is not an error anywhere downstream:
extraction returns `Ok` with a short payload, and a short payload passes the budget gate by
construction. See gas-killer/service#442 for the full measurement.

So anything needing more than ~50M gas is unreliable on a hosted endpoint, and anything above
~575M is impossible. An Anvil fork of the same chain, with `--disable-block-gas-limit`, is a node
whose cap we set.

## What this PR changes

**`SIM_HTTP_RPC` / `L2_SIM_HTTP_RPC`** (`common/src/providers.rs`) — the endpoint a tracked function
is simulated against. Defaults to `HTTP_RPC`; empty counts as unset. Only extraction and the
EVMSketch gas estimate move; settlement, chain detection and `stateTransitionCount` reads stay on
`HTTP_RPC`. `GasKillerValidator::sim_rpc_url_for_chain` is what the three `analyze_transaction`
call sites now read.

**`l1.simFork.enabled`** — renders the bundled Anvil outside LOCAL mode and points the router and
every node at it. Previously the workload was gated to `environment=LOCAL`, where it *is* the chain.

**`l1.extraArgs`** — plumbs `ANVIL_EXTRA_ARGS` through to the Anvil command. The chart could not
previously set `--disable-block-gas-limit`; `global.localAnvilUnboundedReady` existed only to
acknowledge that. That value still works, but `l1.extraArgs` is now the direct route and the
render-time guard accepts either.

**`l1.simFork.refork`** (`helm/gas-killer/files/refork-proxy.py`, on by default with the fork) — a
sidecar between `SIM_HTTP_RPC` and Anvil that re-forks to the block each request asks for. Anvil
forks at ONE block and never follows the chain, while `analyze_transaction` traces at the task's
anchor block — the chain head when the client submitted. Against a bare fork every head-anchored
task fails enrichment with `BlockOutOfRangeError`, and after any settlement the fork's prestate is
stale. The proxy reads the block parameter, issues `anvil_reset` when the fork is elsewhere, and
passes requests at the current block straight through, so the router and every node tracing the
same task share one fork. A reset waits for in-flight traces to drain rather than aborting them.
The gas-limit flag survives a reset (measured: a `2^40` request is still granted in full after
one).

## Two simulation endpoints: bundled fork, or an in-cluster Sepolia node

The chart offers two ways to give the fleet a `debug_traceCall` endpoint whose cap it controls:

| | `l1.simFork.enabled` (anvil fork + re-fork proxy) | `simRpc.url` → `helm/sim-node` (reth + lighthouse) |
|---|---|---|
| What it is | Anvil forking `secrets.forkUrl`, re-forked per task by the proxy sidecar | A Sepolia full node in the cluster, `--rpc.gascap=max` |
| Fits when | the target's working set is small (onchainLife, ArraySummation) | the target reads a lot of code/state per call — every re-fork refetches it from the upstream, which never converges for a 24k-chunk weight directory |
| Follows the chain | per request, via `anvil_reset` | natively; head-anchored tasks just work |
| Depends on a hosted RPC at simulation time | yes (lazy state fetch, rate-limited) | no |
| Cost | one 2–4 CPU pod | a dedicated node pool (e2-standard-8) + 1 Ti SSD, hours of initial sync |

`helm/sim-node` runs reth deliberately: anvil is revm too, so callTracer/prestateTracer output
matches what every e2e leg has ever produced (gas-analyzer#178: geth orders callTracer logs
differently, which forks digests across a mixed fleet). reth also has no default trace timeout
and no HTTP write timeout, both of which geth would need worked around for a multi-minute trace.

```
gcloud container node-pools create sim-node-pool --cluster gas-killer --region us-east4 \
  --node-locations us-east4-b --machine-type e2-standard-8 --disk-type pd-balanced --disk-size 100 \
  --num-nodes 1 --node-labels=gaskiller.xyz/role=sim-node \
  --node-taints=gaskiller.xyz/sim-node=true:NoSchedule
helm upgrade --install sim-node ./helm/sim-node
```

Do not sync from genesis: that is a day of execution. Seed from publicnode's reth snapshot
(base + part, storage.v2 — match `nodeVersion` to `reth.image.tag`), ~890 GB uncompressed on
2026-09-12:

```
helm upgrade sim-node ./helm/sim-node --set replicas=0 --set snapshot.restore.enabled=true
kubectl logs -f job/sim-node-snapshot-restore      # ends with RESTORE_COMPLETE
helm upgrade sim-node ./helm/sim-node --set replicas=1 --set snapshot.restore.enabled=false
```

A StatefulSet will not roll a pod that is not Ready (lighthouse crash-looping on a bad flag,
say) — `kubectl delete pod sim-node-0` to force it onto the new revision.

Then wait for `eth_syncing` to return `false` and the node's head to match the chain's — until
then a head-anchored trace fails with a missing block, exactly the fork's failure — and flip the
fleet with `--set simRpc.url=http://sim-node:8545 --set l1.simFork.enabled=false`. The two are
mutually exclusive at render time. Rolling back to the fork is the reverse flip; the node can
stay up.

Done on the testnet on 2026-09-12 (gas-killer revision 19, sim-node revision 4). Measured on the
node before the flip: `eth_syncing=false` at the same head as Alchemy; a `2^40` `debug_traceCall`
granted exactly `1099511627776`; the ~49M-gas `onchainLife.step(3)` prestate diff trace in 0.3 s.
After the flip, ArraySummation, `step(2)` and `step(3)` tasks anchored at the live head each
reached quorum in ~5 s and settled, with no re-fork step anywhere. Sync from genesis had been
running at ~10k headers/s and was abandoned for the snapshot.

One more node-level cap surfaced only under a genuinely long call: reth's MDBX read transactions
time out at 300 s by default, so a 663 Ggas `eth_call` (the on-chain Qwen3 `dryRun`) died at
336 s with `read transaction has been timed out (-96000)` — after the gas cap, this is the next
silent ceiling. `--db.read-transaction-timeout=7200` is set in `helm/sim-node`. With it, the same `dryRun`
(`GasKillerChatUnchecked` at `0xc4Fe…170A`, 16 prompt ids, 8 new tokens) completed as an
`eth_call` on the node in **1,309 s** (~0.5 Ggas/s on an e2-standard-8; a higher-clock machine
type roughly halves that) and returned the reference answer, "Ethereum is a decentralized
blockchain platform", ids `[36, 18532, 372, 374, 264, 47963, 17944, 5339]` — while the fleet
kept settling head-anchored tasks against the same node. A monolithic tracked `ask` needs
the fleet's timeouts sized for it — `router.roundTimeout=2400`, `router.payloadBlockBuffer=250`,
`router.ingress.taskTtlSeconds=3600` (`LONG_ROUNDS=1` in the rollout script) — under the one
ceiling nothing moves: the chain's `BLOCK_STALE_MEASURE`, 300 blocks (~60 min) from the anchor
block to settlement, inside which the router traces the call and then every node traces it again.

The consumer for this fleet's real quorum is `GasKillerChatUnchecked` at
`0xF89F6e949b48bc33E70f6cBb783ec837c9B2Ea6b` (deploy tx
`0xa0beebfef80a41c40b74af4229eae18b1d1c33bc7387aa654fdc4a1354cadd2b`, block 11,689,217), wired
to `avsServiceManagerWrapper` `0x4Fa4499b…` and the `IncredibleSquaringTaskManager` BLS checker
`0x7568336e…` — not the handoff's tenop-era consumer, whose checker is a mock.

**First quorum-signed on-chain inference, 2026-09-12.** Task `19e88bc7`, `ask([" Ethereum", " is"], 4)`
(prompt ids `[33946, 374]`, ~150 Ggas), anchored at the live head 11,689,375:

| | |
|---|---|
| router EVMSketch (two concurrent traces) | 12.5 min |
| node EVMSketch (3 nodes × 2 traces on 4 physical cores) | 19.5 min |
| `ready` after | 1,924 s |
| settlement | `0x0c7ef2e69f7204db5b4df2d909f0cad62f85da99a593acda2a4cc6177bf04dd9`, block 11,689,533, 327,965 gas |
| `ChatAnswered` | answer `" a decentralized digital currency"`, ids `[264, 47963, 7377, 11413]` |

All three nodes certified height 45 with the router's digest `8de7b825…`, i.e. each recomputed the
same 1,152-byte payload from the on-chain weights independently. Sizing rule that made it fit: the
trace runs at ~0.215 Ggas/s here and the router and nodes run in series (gas-killer/service#449),
so ≈150 Ggas per phase is the ceiling under the 300-block window with margin; the reference
16-in/8-out prompt (663 Ggas) needs #449 *and* a faster machine type.

## Before you start

- [ ] `secrets.forkUrl` points at the same chain as `secrets.httpRpc`. The fork tracks it; a
      mismatch means simulating against one chain and settling on another.
- [ ] The target is deployed:
      `cargo run -p scripts --bin deploy_example -- --example onchainLife`
- [ ] `global.stateEncoding` is `prestate-net` (already set in `testnet-overrides.yaml`).
      Unbounded pairs with it — a struct-log trace of a call this heavy does not complete.
- [ ] Use the value `unbounded`. `unbounded-v1` panics at startup by design
      (`common/src/config.rs`).

## What a local deploy of this chart actually showed

Run against kind with the chart's own manifests, forking Sepolia:

| | Banner `Gas Limit` | 104M-gas `debug_traceCall` |
|---|---|---|
| `l1.extraArgs` unset | `60000000` | completes, no error |
| `l1.extraArgs=--disable-block-gas-limit` | `Disabled` | completes, no error |

Two things follow.

**The chart must supply anvil's command, and now does.** The `ghcr.io/breadchaincoop/ethereum`
image hardcodes its entrypoint —
`anvil --fork-url $FORK_URL --host 0.0.0.0 --port 8545 --code-size-limit 65536` — and never reads
`ANVIL_EXTRA_ARGS`. Passing the variable as a bare env var renders green and changes nothing.

**On anvil, `--disable-block-gas-limit` is not what lifts the tracing cap.** It governs block
construction; `debug_traceCall` honours `tx.gas` either way. The cap that actually bites is
node-level — geth's `--rpc.gascap`, which is what hosted providers clamp with and what returns a
truncated trace instead of an error. Keep the flag (the render guard requires it, and it costs
nothing), but if you swap anvil for geth as the simulation endpoint, `--rpc.gascap=0` is the
setting that matters, not this one.

## The fork pod needs a priority class the cluster allows

`l1.priorityClassName` defaults to `system-cluster-critical`, which GKE only permits in
`kube-system`. Outside it the ReplicaSet never creates a pod — `helm upgrade` succeeds, the
router and nodes flip, and the fork they point at does not exist:

```
Warning  FailedCreate  replicaset/gas-killer-l1-…  Error creating: insufficient quota to match
these scopes: [{PriorityClass In [system-node-critical system-cluster-critical]}]
```

Pass `--set l1.priorityClassName=` (empty) or a class the namespace can use. Seen on the testnet
cluster on 2026-09-11; the LOCAL kind deploy did not show it because kind has no such quota.

## Size the fork before you flip

`l1.resources` defaults to 1 CPU / 2Gi, which was sized for a LOCAL Anvil serving a test chain. Under
`unbounded` this one pod executes every task's heavy simulation for the router *and* every node, and
caches forked state in memory. A 600M-gas call is CPU-bound. Raise it with the rollout rather than
after the first timeout:

```
  --set l1.resources.requests.cpu=2 --set l1.resources.limits.cpu=4 \
  --set l1.resources.requests.memory=4Gi --set l1.resources.limits.memory=8Gi
```

Watch `gas_killer_evmsketch_trace_fetch_seconds` after the flip — extraction moving from the hosted
endpoint to the fork should show up there, and a starved fork shows up as latency rather than error.

## Rollout

The profile changes the derived `storage_updates` and therefore the task digest, so the router and
every node must flip together. One `global.simProfile` value feeds both deployments through
`gas-killer.simProfile`, so a single `helm upgrade` does it — this is not a rolling update, and a
partially migrated fleet fails quorum until it converges.

```
helm upgrade --install gas-killer ./helm/gas-killer \
  -f helm/gas-killer/testnet-overrides.yaml \
  --set secrets.privateKey=0x... \
  --set secrets.fundedKey=0x... \
  --set secrets.httpRpc=https://... \
  --set secrets.forkUrl=https://... \
  --set secrets.adminKey=<admin-key> \
  --set router.image.tag=router-<sha> \
  --set node.image.tag=node-<sha> \
  --set kube-prometheus-stack.grafana.adminPassword=<password> \
  --set l1.simFork.enabled=true \
  --set-string l1.extraArgs="--disable-block-gas-limit" \
  --set l1.priorityClassName= \
  --set global.simProfile=unbounded
```

`--set-string` on `extraArgs` matters: `--set` reads the leading dashes as flags.

## Verify, in order

1. **The fork is up and uncapped.** Against the `-l1` service, fire a trace above the block limit
   and check the root frame's granted `gas` equals what was requested rather than a round number
   like 600,000,000:

   ```
   kubectl exec -it deploy/<release>-l1 -- \
     cast rpc debug_traceCall \
       '{"to":"<target>","data":"<calldata>","gas":"0x10000000000"}' \
       latest '{"tracer":"callTracer"}' --rpc-url http://localhost:8545
   ```

   Then confirm the fork follows a task's block rather than staying pinned. Through the `-l1`
   *service* (the proxy), not the pod's `localhost:8545` (bare Anvil), request a block ahead of
   the fork block; bare Anvil answers `BlockOutOfRangeError`, the proxy re-forks and answers:

   ```
   kubectl exec -it deploy/<release>-router -- \
     cast rpc eth_getCode <target> $(printf '0x%x' <current chain head>) --rpc-url http://<release>-l1:8545
   kubectl logs deploy/<release>-l1 -c refork-proxy | grep re-forked
   ```

2. **The fleet agrees.** `GK_SIM_PROFILE=unbounded` and the same `SIM_HTTP_RPC` on the router and
   all nodes:

   ```
   kubectl get pods -o json | jq -r '.items[].spec.containers[].env[]
     | select(.name=="GK_SIM_PROFILE" or .name=="SIM_HTTP_RPC") | "\(.name)=\(.value)"' | sort | uniq -c
   ```

   Every node plus the router should appear, with one distinct value each. Two values for either
   means a partial rollout: nothing will settle until it converges.

3. **A canary task reaches quorum.** Submit a 2-generation `onchainLife` task (~33M gas — over a
   30M block, so it cannot be analyzed under `chain`). Confirm quorum forms and the
   `verifyAndUpdate` receipt lands. A partial migration is safe but total, so this is the check
   that tells you fast. Anchor it at the live head (`block_height = 0` in a `run_scenario` file),
   which is what a real client does — a canary anchored at the fork block by hand would pass
   even with the re-fork proxy broken.

   Done on the testnet on 2026-09-11: `onchainLife` at
   `0x377a849a7745ad4d1d42d4e8698a118a0587f504`, `step(2)` then `step(3)` (~49M gas), each ready
   in ~5s and settled (transitions 1 and 2).

4. **Then the real target.** Step up generations until you reach the size you actually want.

## Rollback

`--set global.simProfile=chain` and upgrade. In-flight tasks signed under `unbounded` will not
verify against nodes that have flipped back, so let the round drain or expect those to fail. The
fork can stay up; with `simProfile=chain` it is simply a simulation endpoint that mirrors the chain
— but only behind the re-fork proxy. A bare fork (`l1.simFork.refork.enabled=false`) as
`SIM_HTTP_RPC` breaks every head-anchored task regardless of profile, so rolling back only the
profile is not a rollback of the simulation path; add `--set l1.simFork.enabled=false` to get
extraction back onto `HTTP_RPC`.

## Known open items

Neither blocks this rollout, but the next person should know:

- **gas-analyzer#181** — the payload gate prices applying a payload on-chain at
  `UNBOUNDED_APPLY_GAS_PER_PAYLOAD_BYTE = 14`, measured against the analyzer's estimator handler
  rather than the production `verifyAndUpdate`. The gate is dead code under `chain` and starts
  enforcing the moment you flip. It will not bind for `onchainLife`, whose diff is ≤16 words plus a
  counter against a `2^24` budget, but it governs any payload-heavy target.
- **gas-killer/service#442** — nothing reads `DefaultFrame.failed` and nothing compares granted gas
  against what was requested, so a clamped trace is indistinguishable from a real one. Owning the
  fork removes the variance rather than detecting it; the fail-closed check is still worth adding
  before trusting any endpoint the fleet does not control.
- **Fork block drift.** Every operator reads one in-cluster fork here, so they agree by
  construction, and the re-fork proxy pins that fork to each task's anchor block. Two tasks
  anchored at different blocks serialize through it (each reset drains the other's traces
  first), so mixed-block load costs latency rather than correctness. Per-operator forks would
  each need their own proxy; tasks anchor within `blockStaleMeasure` (300) blocks, and
  `anvil_reset` to any block the upstream still serves works, so that is not a constraint.
