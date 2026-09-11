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

## Before you start

- [ ] `secrets.forkUrl` points at the same chain as `secrets.httpRpc`. The fork tracks it; a
      mismatch means simulating against one chain and settling on another.
- [ ] The target is deployed:
      `cargo run -p scripts --bin deploy_example -- --example onchainLife`
- [ ] `global.stateEncoding` is `prestate-net` (already set in `testnet-overrides.yaml`).
      Unbounded pairs with it — a struct-log trace of a call this heavy does not complete.
- [ ] Use the value `unbounded`. `unbounded-v1` panics at startup by design
      (`common/src/config.rs`).

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
   that tells you fast.

4. **Then the real target.** Step up generations until you reach the size you actually want.

## Rollback

`--set global.simProfile=chain` and upgrade. In-flight tasks signed under `unbounded` will not
verify against nodes that have flipped back, so let the round drain or expect those to fail. The
fork can stay up; with `simProfile=chain` it is simply a simulation endpoint that mirrors the chain.

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
  construction. Per-operator forks would need pinning to the task's anchor block — tasks anchor
  within `blockStaleMeasure` (300) blocks, so confirm a long-lived fork still serves
  `debug_traceCall` that far back before splitting the fork per node.
