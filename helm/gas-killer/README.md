# Helm Chart

A Helm chart for deploying the Gas Killer Router AVS with EigenLayer integration.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2+
- PV provisioner support in the underlying infrastructure (for shared data volume)

## Installation

Fetch chart dependencies before installing:

```bash
helm dependency update ./helm/gas-killer
```

Then install:

```bash
helm install gas-killer ./helm/gas-killer \
  --set secrets.forkUrl="https://your-rpc-url" \
  --set secrets.privateKey="0x..." \
  --set secrets.fundedKey="0x..."
```

## Important Gotchas

### Setup Job Behavior

The setup job (`helm.sh/hook: post-install`) only runs on fresh installs, NOT on upgrades. This means:

- **Operator registration changes require reinstall**: If you modify operator configuration (number of nodes, keys, etc.), running `helm upgrade` will NOT re-register operators. You must uninstall and reinstall the chart, or manually run the setup.

- **To re-run setup after changes**:
  ```bash
  # Option 1: Uninstall and reinstall
  helm uninstall gas-killer
  helm install gas-killer ./helm/gas-killer --set ...

  # Option 2: Delete the job and PVC, then upgrade
  kubectl delete job gas-killer-setup
  kubectl delete pvc gas-killer-shared-data
  helm upgrade gas-killer ./helm/gas-killer --set ...
  ```

### DNS Label Length Limits

Kubernetes DNS labels are limited to 63 characters. If your release name is long, resource names may be truncated. The chart handles this automatically, but be aware that very long release names combined with component suffixes may result in truncated names.

### Priority Classes

The Ethereum (Anvil) pod uses `system-cluster-critical` priority class to ensure it stays running, as it holds critical blockchain state. Consider creating a custom priority class if you don't want to use system-reserved classes:

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: gas-killer-critical
value: 1000000
globalDefault: false
description: "Priority class for Gas Killer critical components"
```

Then set in values:
```yaml
ethereum:
  priorityClassName: gas-killer-critical
```

### Node Readiness

The current node readiness probe checks if the `gas-killer` process is running. For production deployments, consider implementing a proper health/readiness endpoint in the node application that verifies:
- Connection to Ethereum RPC
- BLS key loaded
- P2P network connectivity

### Init Container Timeouts

All init containers have a configurable timeout (default: 300 seconds). If your setup takes longer (e.g., slow RPC, large state), increase the timeout:

```bash
helm install gas-killer ./helm/gas-killer \
  --set global.initTimeout=600
```

## Configuration

See `values.yaml` for all available configuration options.

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global.environment` | Environment mode (LOCAL or TESTNET) | `LOCAL` |
| `global.nodeCount` | Number of operator nodes | `3` |
| `global.initTimeout` | Init container timeout in seconds | `300` |
| `global.simProfile` | Tracked-function simulation profile (`chain` or `unbounded`), shared by the router and every node so their signed payloads agree. `unbounded` simulates under the pinned unbounded gas limits, allowing functions whose direct execution exceeds the block gas limit; it needs the RPC's execution cap lifted and pairs with `global.stateEncoding=prestate-net`. **Not production-ready — see the preconditions in `values.yaml` and gas-killer/service#356.** | `chain` |
| `global.localAnvilUnboundedReady` | Confirms the ethereum image starts Anvil with `--disable-block-gas-limit`. Rendering fails on `global.environment=LOCAL` with `global.simProfile=unbounded` until this is set, since that flag lives in the image rather than the chart. | `false` |
| `secrets.forkUrl` | Anvil fork URL (required for LOCAL mode) | `""` |
| `secrets.privateKey` | Deployer private key | `""` |
| `secrets.fundedKey` | Funded account private key | `""` |
| `secrets.adminKey` | Shared secret guarding the `/admin/keys` endpoints, used to mint and revoke per-client API keys via `Authorization: Bearer <value>`. Clients then authenticate `POST /tasks` with their minted key. **Required** when `global.environment=TESTNET` and `router.ingress.enabled=true`. | `""` |
| `global.signatureScheme` | Quorum signature scheme (`bls` or `schnorr`), shared by the router and every node. See "Schnorr signatures" below: switching is a reinstall, not a rolling change. | `bls` |

## Schnorr signatures

`global.signatureScheme=schnorr` replaces the aggregation engine's BLS certificates with a
two-round MuSig2 coordinator on p2p channel 2, producing one constant-gas aggregate signature
verified against a `SchnorrStakeRegistry` rather than a `BLSSignatureChecker`.

Setting it changes three things about the deployment:

- **Each node loads a second key.** Its secp256k1 operator key becomes the Schnorr signing key,
  separate from the BN254 identity the p2p transport uses in both modes. The eigenlayer setup
  container writes these next to the BLS keys; with `secretManager.enabled` they are exported to
  and restored from Secret Manager as `<keyPrefix>-node-<n>-ecdsa-key`.
- **An extra install-time job runs.** `schnorr-operators` deploys the registry and registers every
  operator against it with a proof of possession, then records the address under
  `addresses.schnorrStakeRegistry` in `avs_deploy.json`. It runs between `setup` and
  `deploy-target`, and `deploy-target` blocks on its marker.
- **Targets change type.** A Schnorr fleet settles only against `SchnorrGasKillerSDK` consumers
  wired to that registry. `deploy-target` deploys `SchnorrArraySummation` in place of
  `ArraySummation`.

### Ordering is load-bearing

Every registration advances the registry's `effectiveBlock` watermark, and verification
fail-closes for reference blocks behind it. The whole operator set must therefore be registered
before any target deploys, which is what the marker between the two jobs enforces. A target
deployed early is not repairable: its registry is immutable.

### Publishing the registry before a cutover

The router serves the registry address as `schnorrStakeRegistry` on `GET /avs-metadata` under
**either** scheme. It never verifies against that address itself, it only publishes it, and a
target's constructor is what consumes it. So publishing it while the fleet still signs BLS is what
lets an integrator deploy a target that accepts both schemes and then needs no action from them
when the fleet switches.

The address comes from `schnorr.stakeRegistryAddress`, or failing that from what the
`schnorr-operators` job recorded in `avs_deploy.json`. It publishes only once
`nextPossibleMutationBlock()` answers at it, so a checker or a coordinator set here is omitted
rather than served.

`schnorr.provision` renders the `schnorr-operators` job under any scheme, so the cluster
provisions the registry itself:

```bash
helm upgrade gas-killer ./helm/gas-killer --reuse-values \
  --set schnorr.provision=registry \
  --set schnorr.noticeWindow=<blocks> \
  --set rerun.schnorrOperators=true
```

`rerun.schnorrOperators` is needed on an existing release because the job is otherwise
install-only. On a release that has never provisioned there is no Job object to collide with, so
this is a first run despite the flag's name. A later run needs the existing Job deleted first,
since it is kept by resource policy and its spec is immutable.

| `schnorr.provision` | Deploys and publishes | Registers the operator set |
|---|---|---|
| `""` (default) | no | no |
| `registry` | yes | no |
| `full` | yes | yes |

`registry` is what a deployment whose operator secp256k1 keys are gone can still do. `full` needs
every operator's key on the shared volume, and makes the restore of those keys fatal in the setup
job. Under `global.signatureScheme=schnorr` the value is ignored: the job always runs and always
registers, since a Schnorr fleet certifies nothing against an empty registry.

Nothing about the running fleet changes either way. It keeps signing BLS, `deploy-target` keeps
deploying a BLS target, and the job's only effect on the deployment is the address it publishes.

The job writes the address to `/app/.nodes/schnorr_stake_registry.txt`, and the router reads it
from there as a named record rather than from `avs_deploy.json`, because under Secret Manager the
router's copy of that file comes from a secrets volume the job never writes to. Being a record
also means it is retried while unwritten, so a job that finishes after the router is serving still
lands without a restart. `schnorr.stakeRegistryAddress` overrides it, for a registry provisioned
outside the chart entirely.

Three of the registry's parameters are **fixed at deployment and cannot be changed afterwards**,
so they have to be right on the run that deploys it:

| Fixed at deployment | Comes from | Getting it wrong means |
|---|---|---|
| Threshold | `eigenlayer.sdk.quorumThreshold` / `.thresholdDenominator` | An on-chain quorum that disagrees with the router's own participation floor |
| Notice window | `schnorr.noticeWindow` | See below |
| Owner | `schnorr.deployerSecretKey` | Nobody can register or deregister an operator |

The `schnorr.noticeWindow` default of `0` is correct only when the whole operator set is registered
before any target deploys, which is the e2e stack's order and **not** `provision=registry`'s. A
registry that will be filled or mutated while rounds are in flight needs a window longer than a
round plus `eigenlayer.sdk.blockStaleMeasure`, or an operator-set change can land between a round
assembling its signature and that signature settling.

The operator set itself is *not* fixed: its owner registers and deregisters through
`announceRegister` / `announceDeregister` / `commitNextChange`. That is what makes publishing an
empty registry safe. Every target wired to it keeps working when the set is filled later, so the
address does not have to be republished and no integrator has to redeploy twice.

### Switching an existing deployment

There is no rolling path from `bls` to `schnorr`. A mixed fleet certifies nothing, and every
target already deployed verifies the other scheme's proof, so it is stranded rather than
degraded. Switching means reinstalling the operator set and redeploying every target, including
any an integrator owns. Prove the change on a separate release first.

The one-way door is the registry. `rerun.schnorrOperators=true` without
`schnorr.stakeRegistryAddress` deploys a *fresh* registry and re-registers everyone, which
orphans every target wired to the previous one. The job is otherwise install-only and skips when
`avs_deploy.json` already records an address.

### Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `schnorr.deployerSecretKey` | Secret key holding the funded key that deploys the registry and submits the registrations. The deployer becomes the registry owner. | `PRIVATE_KEY` |
| `schnorr.provision` | Provision the Schnorr scaffolding while the fleet signs another scheme: `""`, `registry` or `full`. Ignored under `signatureScheme=schnorr`. | `""` |
| `schnorr.noticeWindow` | Blocks an operator-set change must be announced ahead of taking effect, fixed at registry deployment. `0` applies changes immediately, correct only when the set is registered before any target deploys. | `0` |
| `schnorr.stakeRegistryAddress` | The registry this deployment uses. The operator-set job reuses it instead of deploying one, assuming its set is complete, so it submits no registrations; the router publishes it as `schnorrStakeRegistry` on `GET /avs-metadata` in either scheme. | `""` |
| `schnorr.stageTimeoutSecs` | Per-stage timeout for the coordinator's rounds. Empty uses `min(5, ROUND_TIMEOUT/6)`. | `""` |
| `schnorr.messagesPerSecond` | Per-peer rate on the schnorr channel, rendered into both the router and the nodes. The p2p sender silently drops over-rate messages, and a dropped round message costs a whole retry. Empty uses `64`. | `""` |

The registry's on-chain threshold comes from `eigenlayer.sdk.quorumThreshold` /
`eigenlayer.sdk.thresholdDenominator`, which are also rendered into the router as its local
participation floor, so the off-chain and on-chain checks stay in lockstep.

## Operator key durability

The shared-data volume holds the operators' BLS and secp256k1 key files. The eigenlayer setup
container generates them once with a live RNG, and until the key-export job copies them to Secret
Manager they exist nowhere else. A lost operator key cannot be recovered, only replaced, and
replacing one means re-registering the operator set on chain.

Three independent guards keep that from happening quietly:

- **The claim outlives the release.** `sharedData.retainOnUninstall` (default true) puts
  `helm.sh/resource-policy: keep` on the PVC, so `helm uninstall` does not take the volume with
  it. That also makes the PV's reclaim policy moot, which matters because the default GKE
  StorageClass reclaims on delete.
- **Backups are read back before they are trusted.** The key-export job re-reads every secret it
  writes and compares the bytes, then records a manifest secret (`<keyPrefix>-key-manifest`)
  listing what it backed up, with a sha256 and the job responsible for restoring it. It also
  reports any key file on the volume it has no rule for.
- **A partial restore is never marked complete.** The setup job's restore path verifies what it
  restored against that manifest before writing `.setup_complete`. Since the marker is what tells
  the eigenlayer container to skip regeneration, writing it after an incomplete restore is what
  turns a recoverable gap into a permanent one. On a mismatch the job fails, leaves the marker
  off, and names the secrets it could not account for.

A deployment whose last export predates the manifest has none; the restore warns loudly and
proceeds, since refusing would strand a volume that is probably fine. Re-run the key-export job
to record one. Only a `NOT_FOUND` counts as absent: any other Secret Manager failure fails the
job rather than falling through to the unverified path.

When the gate fails it leaves the restored files on the volume without the marker, which the
partial-state guard then refuses to restore over. Recovery is to fix the secrets, then delete
`avs_deploy.json` and `operator_keys/` from the volume before retrying. The job's error output
says so.

## Architecture

The chart deploys the following components:

1. **Ethereum (Anvil)** - Local blockchain with forked Sepolia state
2. **Signer (Cerberus)** - BLS signature service
3. **Setup Job** - EigenLayer contract deployment and operator registration
4. **Gas Killer Nodes** - Operator nodes (configurable count)
5. **Router** - Request routing and aggregation

### Startup Order

Components start in a specific order enforced by init containers:

1. Ethereum pod starts first
2. Setup job waits for Ethereum, then deploys contracts and registers operators
3. Signer waits for setup completion (needs operator keys)
4. Nodes wait for setup completion and Ethereum availability
5. Router waits for setup, Ethereum, and all nodes

Under `global.signatureScheme=schnorr` the schnorr-operators job also waits for setup, and the
deploy-target job waits for both. See "Schnorr signatures" above.

## HTTPS / TLS Ingress

To expose the router ingress over HTTPS on a public domain, use the nginx-ingress
controller with cert-manager for automated Let's Encrypt certificates.

### One-time cluster setup

**1. Install nginx-ingress:**
```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx
```

**2. Install cert-manager:**
```bash
helm repo add jetstack https://charts.jetstack.io
helm install cert-manager jetstack/cert-manager --set crds.enabled=true
```

**3. Create a Let's Encrypt ClusterIssuer** (substitute your email):
```bash
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: dev@gaskiller.xyz
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
      - http01:
          ingress:
            class: nginx
EOF
```

**4. Get the LoadBalancer IP** assigned to the nginx-ingress controller:
```bash
kubectl get svc ingress-nginx-controller \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

**5. Create a DNS A-record** pointing your domain at that IP.

### Deploy with TLS

Enable ingress and pass your hostnames at install/upgrade time:

```bash
helm upgrade --install gas-killer ./helm/gas-killer \
  --set ingress.enabled=true \
  --set ingress.host=testnet.gaskiller.xyz \
  --set monitoring.grafana.ingress.enabled=true \
  --set monitoring.grafana.ingress.host=grafana-testnet.gaskiller.xyz \
  --set kube-prometheus-stack.grafana.adminPassword="..." \
  --set secrets.privateKey="0x..." \
  ...
```

Both ingresses default to `nginx` as the ingress class, cert-manager's `letsencrypt-prod`
cluster issuer, and `gaskiller-tls` / `grafana-tls` as their TLS secret names respectively.
Override any of these with `--set ingress.tlsSecretName=...`,
`--set monitoring.grafana.ingress.tlsSecretName=...`, etc.

cert-manager will automatically provision the TLS certificates. The nginx-ingress
controller handles HTTP → HTTPS redirects automatically.

### Public paths (admin API is not exposed)

The router Ingress routes an explicit allowlist of paths, `ingress.publicPaths` (default
`/tasks`, `/avs-metadata`, `/healthz`). Any path not listed — in particular the `/admin/*`
key-management endpoints — is **not** routed publicly and is reachable only in-cluster: via the
ClusterIP Service, `kubectl port-forward svc/<release>-router 8080:8080`, or
`kubectl exec` into the router pod. This keeps admin behind cluster access **in addition** to
`ADMIN_KEY`. Add a path to `ingress.publicPaths` only if it genuinely must be internet-facing.

## Monitoring (Prometheus + Grafana)

Metrics are exposed at `/metrics` on port 8081 of the router and node pods. The monitoring stack
(Prometheus Operator, Grafana, AlertManager) is deployed as a subchart and is off by default.

### One-time cluster setup

The Prometheus Operator CRDs must exist in the cluster before the chart can create
`ServiceMonitor`, `Prometheus`, `Alertmanager`, and `PrometheusRule` resources. This only needs
to be done once per cluster.

**1. Fetch chart dependencies** (if not already done):
```bash
helm dependency update ./helm/gas-killer
```

**2. Install Prometheus Operator CRDs:**
```bash
helm show crds helm/gas-killer/charts/kube-prometheus-stack-*.tgz | kubectl apply --server-side -f -
```

**3. Wait for CRDs to be registered** before running the helm upgrade:
```bash
kubectl wait --for=condition=Established \
  crd/prometheuses.monitoring.coreos.com \
  crd/servicemonitors.monitoring.coreos.com \
  crd/prometheusrules.monitoring.coreos.com \
  crd/alertmanagers.monitoring.coreos.com \
  --timeout=30s
```

**4. Create a DNS A-record** pointing `grafana-testnet.gaskiller.xyz` at the nginx-ingress
LoadBalancer IP (same IP used for the router ingress):
```bash
kubectl get svc ingress-nginx-controller \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

### Deploy with monitoring enabled

The testnet and mainnet override files (`testnet-overrides.yaml`, `mainnet-overrides.yaml`) already enable monitoring and the Grafana ingress.

```bash
helm upgrade --install gas-killer ./helm/gas-killer \
  -f helm/gas-killer/testnet-overrides.yaml \
  --set secrets.privateKey=0x... \
  --set secrets.fundedKey=0x... \
  --set secrets.httpRpc=https://... \
  --set secrets.l2HttpRpc=https://... \
  --set router.image.tag=router-<sha> \
  --set node.image.tag=node-<sha> \
  --set kube-prometheus-stack.grafana.adminPassword=<password>
```

### Accessing Grafana

Once deployed, Grafana is available at `https://grafana-testnet.gaskiller.xyz` (if the ingress
is enabled and DNS is configured), or via port-forward:

```bash
kubectl port-forward svc/gas-killer-grafana 3000:80
```

Then open `http://localhost:3000` and log in with username `admin` and the password you set.

The **Gas Killer** dashboard is pre-loaded automatically via the Grafana sidecar. It includes:
- Router and node up/down status, database and RPC health
- Pod restart counts, CPU and memory usage per pod
- Aggregation throughput, end-to-end and per-phase latency breakdowns
- Ingress request rates and per-API-key accept/reject series
- Window and height observability: concurrent heights, window base against the engine tip,
  per-height outcomes, directive delivery, and the configuration fingerprint across the fleet

### Verifying scrape targets

Port-forward the Prometheus UI and check that all targets show as `UP`:

```bash
kubectl port-forward svc/gas-killer-kube-prometheus-prometheus 9090:9090
```

Then open `http://localhost:9090/targets`.

## Troubleshooting

### Pods stuck in Init state

Check init container logs:
```bash
kubectl logs <pod-name> -c wait-for-setup
kubectl logs <pod-name> -c wait-for-ethereum
```

### Setup job failed

Check setup job logs:
```bash
kubectl logs job/gas-killer-setup
```

### Shared data issues

Verify PVC is bound:
```bash
kubectl get pvc gas-killer-shared-data
kubectl describe pvc gas-killer-shared-data
```
