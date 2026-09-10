//! Resolves the contract addresses that make up a deployment's settlement wiring.
//!
//! A target contract must be wired to two addresses: the AVS service manager and the
//! `IBLSSignatureChecker`. Get the checker wrong and the target still passes every router-side
//! check — it is handed a `ready` payload — and then reverts `InvalidQuorumApkHash` on chain,
//! because the router computes quorum APK indices against the live registry while the contract
//! checks them against a superseded one. Publishing the pair from the running deployment is what
//! makes that unwireable by accident.
//!
//! The set is shared because the wiring is: the router aggregates certificates against the
//! operators' registry coordinator and submits to a target that verifies through the checker, and a
//! node validates work for that same deployment. The router also publishes the resolved set as the
//! `contracts` block on `GET /avs-metadata`, which is what saves an integrator from hand-copying
//! any of it — hand-copied addresses are exactly what goes stale when the operator set is
//! redeployed.
//!
//! - `registryCoordinator` comes from the same `avs_deploy.json` loader the rest of the service
//!   reads, so there is one parser for that file.
//! - `avsAddress` and `blsSignatureChecker` are read from a live target's own getters, so they are
//!   authoritative by construction: whatever a settling target verifies against *is* the answer.
//!   In particular the checker is not `avs_deploy.json`'s `blsSigCheck` — upstream maps that key
//!   onto the operator-state retriever, and the target deploy script provisions a fresh checker
//!   from the registry coordinator, so that field names a different contract than any target uses.
//! - The two are then cross-checked: the checker's own `registryCoordinator()` must be the
//!   coordinator the operators are registered in. A mismatch means the reference target belongs to
//!   a superseded deployment, so nothing is published rather than publishing a pair that reverts.
//! - `schnorrStakeRegistry` comes from `SCHNORR_STAKE_REGISTRY_ADDRESS`, or failing that from
//!   `avs_deploy.json`, and publishes only once a registry answers at it. A target's registry is
//!   fixed by its constructor, so a wrong one settles BLS rounds today and can never settle a
//!   Schnorr round: a mistake that surfaces at a scheme cutover, long after it was made.
//! - `demoTarget` and `demoFactory` come from configuration or, failing that, from what the
//!   playground job recorded. The playground target doubles as the preferred reference target, so
//!   the checker published is the one `demoTarget.blsSignatureChecker()` itself returns.
//!
//! Resolution costs up to eight RPC round-trips, so the answer is cached in a
//! [`ResolvedContracts`] slot rather than recomputed per use: `/avs-metadata` is public and
//! unauthenticated, and serving it must not turn into an RPC amplifier.
//!
//! The slot holds the most complete set established so far, and a background resolver keeps
//! refining it. That matters because the records it reads are written by deploy jobs that can
//! finish — or fail outright — after the router is already serving: waiting for all of them before
//! publishing anything would let one broken job withhold addresses that have nothing to do with it,
//! while publishing once and stopping would miss a record that lands a minute later. So an attempt
//! publishes what it can establish, and repeats while any record a job is expected to write is
//! still unread.

use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use crate::bindings::IBLSSignatureCheckerRegistry;
use crate::bindings::gaskillersdk::GasKillerSDK;
use crate::bindings::schnorrstakeregistry::ISchnorrStakeRegistry;
use alloy_primitives::Address;
use alloy_provider::Provider;
use anyhow::Context;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::{error, info, warn};

/// Filename holding the address of the target the deploy job last provisioned. Used as the
/// reference target when no pin and no record path are configured, looked for beside
/// `avs_deploy.json`.
const DEMO_TARGET_FILENAME: &str = "demo_target.txt";

/// Names the file recording the target the deploy job provisioned.
///
/// Needed because that record and `avs_deploy.json` are not siblings on every deployment: under
/// Secret Manager the router reads the deployment file from a secrets volume holding nothing else,
/// while the deploy job writes its record to the shared-data volume. Where the chart sets this, the
/// job is what produces the file, so its absence means "not written yet" and resolution is retried.
const REFERENCE_TARGET_FILE_ENV: &str = "AVS_REFERENCE_TARGET_FILE";

/// Names the file recording the playground target — the shared contract the documentation points
/// readers at, deployed by its own job so public traffic cannot advance the transition counter the
/// smoke-test target depends on. Set for the same reason as [`REFERENCE_TARGET_FILE_ENV`]: the job
/// writes to the shared-data volume, which is not where `avs_deploy.json` is read from.
const DEMO_TARGET_FILE_ENV: &str = "DEMO_TARGET_FILE";

/// Names the file recording the playground `ArraySummationFactory`, from which a reader deploys a
/// target only they are advancing.
const DEMO_FACTORY_FILE_ENV: &str = "DEMO_FACTORY_FILE";

/// Key the AVS deployment JSON records the Schnorr stake registry under.
///
/// Public because the writer and the reader are different binaries: the operator-set job records it
/// after deploying the registry, and the router reads it back to publish. A rename on one side
/// alone would degrade into an absent field rather than an error, so both use this.
pub const SCHNORR_STAKE_REGISTRY_KEY: &str = "schnorrStakeRegistry";

/// How long one resolution attempt may run before it is abandoned and retried. Up to eight
/// sequential RPC round-trips sit behind it, and this runs during startup, so an unresponsive
/// provider must not stall the router indefinitely.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Delay before the first retry of a failed resolution, doubled on each further failure up to
/// [`RETRY_MAX_BACKOFF`].
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(5);

/// Ceiling on the retry backoff. Kept to a minute because one thing being waited on is the target
/// deploy job, which can finish minutes after the router is already serving.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// The addresses a target must be wired to in order to settle, plus the demo contracts the
/// documentation points at.
///
/// Serialized as the `contracts` object on `GET /avs-metadata`. Every field is a public on-chain
/// address, and the endpoint is already public and unauthenticated, so there is nothing here to
/// gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct AvsContracts {
    /// EVM chain ID every address below lives on, so an integrator can tell at a glance whether
    /// they are pointed at the right network. True of the AVS trio by construction, since they are
    /// read through a provider for this chain, and of the demo contracts because each is checked
    /// for code here before being published.
    #[serde(rename = "chainId")]
    #[schema(example = 11155111)]
    pub chain_id: u64,
    /// AVS service manager a target names as its `avsAddress`.
    #[serde(rename = "avsAddress", serialize_with = "checksummed")]
    #[schema(value_type = crate::openapi::Address)]
    pub avs_address: Address,
    /// Signature checker a target must verify against. The verification-blocking one of the pair.
    #[serde(rename = "blsSignatureChecker", serialize_with = "checksummed")]
    #[schema(value_type = crate::openapi::Address)]
    pub bls_signature_checker: Address,
    /// Registry coordinator the operators are registered in. Published so an integrator can
    /// confirm their own wiring independently: `blsSignatureChecker().registryCoordinator()` must
    /// equal this.
    #[serde(rename = "registryCoordinator", serialize_with = "checksummed")]
    #[schema(value_type = crate::openapi::Address)]
    pub registry_coordinator: Address,
    /// `SchnorrStakeRegistry` a target verifies aggregate Schnorr signatures against.
    ///
    /// Published so a target can be wired to both schemes at once. A fleet signs exactly one of
    /// them, fixed by its own `SIGNATURE_SCHEME`, and probes only that scheme's ERC-165 ID, so a
    /// target that accepts both settles under either without redeploying across a cutover. Absent
    /// while the deployment has provisioned no registry, which is every BLS-only one.
    #[serde(
        rename = "schnorrStakeRegistry",
        skip_serializing_if = "Option::is_none",
        serialize_with = "checksummed_opt"
    )]
    #[schema(value_type = Option<crate::openapi::Address>, nullable = false)]
    pub schnorr_stake_registry: Option<Address>,
    /// A deployed target anyone may submit against, for a first settlement with no Solidity
    /// written. Shared, so concurrent readers can invalidate each other's payloads on
    /// `transitionIndex`; `demoFactory` is the way to avoid that. Absent when none is configured.
    #[serde(
        rename = "demoTarget",
        skip_serializing_if = "Option::is_none",
        serialize_with = "checksummed_opt"
    )]
    #[schema(value_type = Option<crate::openapi::Address>, nullable = false)]
    pub demo_target: Option<Address>,
    /// Factory that deploys a caller-owned target, for a reader who wants an instance no one else
    /// is advancing. It takes the AVS and checker addresses as arguments rather than wiring them
    /// itself, so a caller should pass the two published above. Absent when none is configured.
    #[serde(
        rename = "demoFactory",
        skip_serializing_if = "Option::is_none",
        serialize_with = "checksummed_opt"
    )]
    #[schema(value_type = Option<crate::openapi::Address>, nullable = false)]
    pub demo_factory: Option<Address>,
}

/// Serializes an address in EIP-55 checksummed form.
///
/// Solidity rejects a lowercase address literal outright — "this looks like an address but has an
/// invalid checksum" — and wiring a target's constructor is exactly what these addresses are for,
/// so the published form has to be one an integrator can paste straight into source. Alloy's own
/// `Serialize` emits lowercase, hence the override. Parsing is unaffected: either form reads back.
fn checksummed<S: Serializer>(address: &Address, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&address.to_checksum(None))
}

/// [`checksummed`] for an optional address. The `None` arm is unreachable while the field is
/// `skip_serializing_if`, but serde requires it.
fn checksummed_opt<S: Serializer>(
    address: &Option<Address>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match address {
        Some(address) => checksummed(address, serializer),
        None => serializer.serialize_none(),
    }
}

/// The slot the resolved contract set is published into, shared between the background resolver and
/// every `/avs-metadata` response.
///
/// Rewritable rather than write-once, so an incomplete set can be served now and replaced when a
/// deploy job's record lands. Reads take the lock only long enough to clone; a poisoned lock is
/// recovered from rather than propagated, since the only writer replaces the value outright and so
/// cannot leave it half-updated.
#[derive(Debug, Clone, Default)]
pub struct ResolvedContracts(Arc<RwLock<Option<AvsContracts>>>);

impl ResolvedContracts {
    /// A copy of the set published so far, or `None` while nothing has been established.
    pub fn snapshot(&self) -> Option<AvsContracts> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether nothing has been resolved yet. Drives `skip_serializing_if` so an unresolved set is
    /// absent from the response rather than serialized as `null` — a client branches on presence.
    pub fn is_unresolved(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }

    /// Publishes `contracts`, returning whether it differs from what was already published.
    ///
    /// The caller uses that to log a change rather than every repeat, since the resolver may
    /// re-establish the same set many times while waiting on a record.
    pub fn publish(&self, contracts: AvsContracts) -> bool {
        let mut slot = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if slot.as_ref() == Some(&contracts) {
            return false;
        }
        *slot = Some(contracts);
        true
    }
}

impl Serialize for ResolvedContracts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &*self.0.read().unwrap_or_else(PoisonError::into_inner) {
            Some(contracts) => contracts.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for ResolvedContracts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let contracts = Option::<AvsContracts>::deserialize(deserializer)?;
        Ok(Self(Arc::new(RwLock::new(contracts))))
    }
}

/// Where the reference target and the demo contracts come from, read from the environment once so
/// that resolution itself is a pure function of its inputs.
#[derive(Debug, Clone, Default)]
pub struct ContractsConfig {
    /// `AVS_REFERENCE_TARGET`: pins the target whose getters establish the AVS/checker pair.
    pub reference_target: Option<Address>,
    /// [`REFERENCE_TARGET_FILE_ENV`]: file the deploy job records that target in.
    pub reference_target_file: Option<PathBuf>,
    /// `DEMO_TARGET_ADDRESS`.
    pub demo_target: Option<Address>,
    /// [`DEMO_TARGET_FILE_ENV`]: file the playground job records its target in.
    pub demo_target_file: Option<PathBuf>,
    /// `DEMO_FACTORY_ADDRESS`.
    pub demo_factory: Option<Address>,
    /// [`DEMO_FACTORY_FILE_ENV`]: file the playground job records its factory in.
    pub demo_factory_file: Option<PathBuf>,
    /// `SCHNORR_STAKE_REGISTRY_ADDRESS`: overrides the registry the AVS deployment JSON records,
    /// for a deployment whose registry was provisioned outside the chart's job.
    pub schnorr_stake_registry: Option<Address>,
}

impl ContractsConfig {
    /// Reads the configuration, failing on a malformed `AVS_REFERENCE_TARGET`.
    ///
    /// A set-but-unparseable pin is an error rather than a silent `None`: it would otherwise fall
    /// through to the deploy job's record and publish a *different* pair than the operator asked
    /// for, on an endpoint whose whole purpose is that nobody reads a wrong address. The demo
    /// contracts and the Schnorr registry are not, so a typo in one of those warns and omits its
    /// own field instead of withholding the settlement addresses beside it.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            reference_target: address_from_env("AVS_REFERENCE_TARGET")?,
            reference_target_file: non_empty_env(REFERENCE_TARGET_FILE_ENV).map(PathBuf::from),
            demo_target: lenient_address_from_env("DEMO_TARGET_ADDRESS"),
            demo_target_file: non_empty_env(DEMO_TARGET_FILE_ENV).map(PathBuf::from),
            demo_factory: lenient_address_from_env("DEMO_FACTORY_ADDRESS"),
            demo_factory_file: non_empty_env(DEMO_FACTORY_FILE_ENV).map(PathBuf::from),
            schnorr_stake_registry: lenient_address_from_env("SCHNORR_STAKE_REGISTRY_ADDRESS"),
        })
    }
}

/// An environment variable's value, treating unset and blank alike.
fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
}

/// Parses an address, naming the variable it came from so a typo is actionable from the log line.
fn parse_address(key: &str, raw: &str) -> anyhow::Result<Address> {
    raw.parse()
        .with_context(|| format!("{key} is not an EVM address: {raw:?}"))
}

/// Reads an optional address from an environment variable. Blank is unset; malformed is an error.
fn address_from_env(key: &str) -> anyhow::Result<Option<Address>> {
    non_empty_env(key)
        .map(|raw| parse_address(key, &raw))
        .transpose()
}

/// [`address_from_env`] for a field whose own typo must not suppress the settlement addresses,
/// degrading a malformed value to `None` with a warning.
///
/// Used for the demo pointers and the Schnorr registry. Not for the reference target: that one
/// establishes the pair, so a wrong value there would publish a *different* pair than the operator
/// asked for rather than leaving a field off.
fn lenient_address_from_env(key: &str) -> Option<Address> {
    match address_from_env(key) {
        Ok(address) => address,
        Err(e) => {
            warn!(error = %e, "ignoring a malformed address; /avs-metadata will omit that field");
            None
        }
    }
}

/// Reads an address a deploy job recorded in a file.
///
/// Trailing whitespace is tolerated: the jobs write with `printf`, but a shell redirect elsewhere
/// may leave a newline. Content that is not an address is an error rather than an omission — a job
/// that failed mid-write, or wrote a message where an address should be, must not resolve to
/// "nothing configured".
fn read_recorded_address(path: &Path) -> anyhow::Result<Address> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading the address recorded at {}", path.display()))?;
    parse_address(
        &format!("the address recorded at {}", path.display()),
        raw.trim(),
    )
}

/// Tracks whether anything the set could still grow by is outstanding: a record a deploy job is
/// expected to write and has not, or an address that did not verify on chain.
///
/// Neither fails an attempt, because one job that has not finished, or has failed for good, must
/// not withhold the addresses it has nothing to do with. Both mean the set is incomplete, so the
/// resolver keeps going until nothing named is outstanding.
#[derive(Debug, Default)]
struct Records {
    pending: bool,
}

impl Records {
    /// The address recorded at `path`, or `None` while that record cannot be read.
    fn read(&mut self, path: &Path) -> Option<Address> {
        match read_recorded_address(path) {
            Ok(address) => Some(address),
            Err(e) => {
                warn!(
                    error = %format!("{e:#}"),
                    "a deploy job's record is not readable; retrying while the set is incomplete"
                );
                self.pending = true;
                None
            }
        }
    }
}

/// An address configured outright, or failing that the one a deploy job recorded.
///
/// The configured value wins so a deployment can point readers at something other than what its own
/// job deployed.
fn configured_or_recorded(
    configured: Option<Address>,
    record: Option<&Path>,
    records: &mut Records,
) -> Option<Address> {
    if let Some(address) = configured {
        return Some(address);
    }
    record.and_then(|path| records.read(path))
}

/// The target whose getters the AVS and checker addresses are read from.
///
/// The `AVS_REFERENCE_TARGET` pin wins. Failing that a deploy job's own record is read, which tracks
/// redeployments without anyone editing configuration: the playground record, then the smoke-test
/// one, then — for a layout that keeps everything on one volume — a record beside `avs_deploy.json`.
///
/// The playground target comes first because it is the contract the documentation points readers at,
/// and it is published as `demoTarget`. Each target deploy provisions its own signature checker, so
/// reading the pair off anything else would publish a checker that verifies correctly yet is not the
/// one `demoTarget.blsSignatureChecker()` returns — a discrepancy an integrator would reasonably
/// read as a bug. Preferring it makes the published set describe the very contract a reader submits
/// against.
///
/// Each source is tried in turn, so a record that is not readable yet falls through to the next
/// rather than withholding everything. `None` means none of them answered; whether that is worth
/// retrying is [`Records::pending`], since a record absent because no job writes it is final while
/// one absent because a job has not finished is not.
fn reference_target(
    deployment_path: &Path,
    config: &ContractsConfig,
    records: &mut Records,
) -> Option<Address> {
    if let Some(address) = config.reference_target {
        return Some(address);
    }
    for named in [&config.demo_target_file, &config.reference_target_file] {
        if let Some(address) = named.as_deref().and_then(|path| records.read(path)) {
            return Some(address);
        }
    }
    let sibling = deployment_path.parent()?.join(DEMO_TARGET_FILENAME);
    if !sibling.exists() {
        return None;
    }
    records.read(&sibling)
}

/// Keeps a configured address only if it has code on the chain the block publishes.
///
/// The demo contracts and the Schnorr registry are free-form configuration, so unlike the AVS trio
/// nothing about them proves they are on this chain: an address could name a contract on another
/// network, or be checksum-valid and simply wrong. Checking for code is what makes `chainId` true of
/// the whole block. An RPC failure propagates so the attempt is retried rather than publishing a
/// partial set.
async fn deployed_on_chain<P: Provider>(
    provider: &P,
    field: &str,
    address: Option<Address>,
) -> anyhow::Result<Option<Address>> {
    let Some(address) = address else {
        return Ok(None);
    };
    let code = provider
        .get_code_at(address)
        .await
        .with_context(|| format!("reading code at the configured {field} {address}"))?;
    if code.is_empty() {
        warn!(
            %address,
            field,
            "no code at the configured address on this chain; /avs-metadata will omit it"
        );
        return Ok(None);
    }
    Ok(Some(address))
}

/// Keeps a Schnorr stake registry address only if a registry answers at it.
///
/// The probe is `nextPossibleMutationBlock()`, which `SchnorrStakeRegistry` declares and the
/// BLS-side contracts do not, so a checker, a coordinator or a target pasted here reverts on an
/// unknown selector and is left off. It is an answer rather than a proof of type: a contract with a
/// permissive fallback would pass. Worth the round trip anyway, because nothing downstream would
/// catch the mistake: a BLS fleet never reads this address, and a target's own registry is fixed by
/// its constructor, so a target wired to a wrong one settles BLS rounds today and can never settle a
/// Schnorr round.
///
/// An address that does not answer is omitted and left outstanding rather than treated as final, so
/// a registry provisioned after the router started is picked up without a restart and a provider
/// blip is retried. It is never fatal: an unverifiable registry must not withhold the addresses
/// beside it, which are the ones a BLS fleet is settling against.
async fn registry_on_chain<P: Provider>(
    provider: &P,
    configured: Option<Address>,
    records: &mut Records,
) -> anyhow::Result<Option<Address>> {
    let Some(address) = deployed_on_chain(provider, SCHNORR_STAKE_REGISTRY_KEY, configured).await?
    else {
        // No code where an address was configured is worth another attempt; nothing configured at
        // all is a final answer. Set rather than assigned: another field may already be waiting on
        // a record, and this must not report the set complete on its behalf.
        if configured.is_some() {
            records.pending = true;
        }
        return Ok(None);
    };
    match ISchnorrStakeRegistry::new(address, provider)
        .nextPossibleMutationBlock()
        .call()
        .await
    {
        Ok(_) => Ok(Some(address)),
        Err(error) => {
            warn!(
                %address,
                %error,
                "the configured schnorrStakeRegistry does not answer nextPossibleMutationBlock(); \
                 /avs-metadata will omit it and try again"
            );
            records.pending = true;
            Ok(None)
        }
    }
}

/// What one resolution attempt established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// The set to publish, or `None` when no reference target answered and there is therefore no
    /// pair to read.
    pub contracts: Option<AvsContracts>,
    /// Whether anything the set could still grow by is outstanding, so it is worth re-establishing:
    /// a record a deploy job is expected to write and has not, or an address that did not verify on
    /// chain. The resolver keeps attempting while this holds; when it is false the answer is as
    /// complete as this configuration allows and resolution is done.
    pub incomplete: bool,
}

/// Resolves as much of the published contract set as the running deployment can answer for.
///
/// An error is a failed attempt — something that should have answered did not, or the pair that came
/// back is not the operators' own. A record that is merely unread is not an error: it leaves
/// [`Resolution::incomplete`] set so the caller tries again, having published whatever else stands.
///
/// `deployment_schnorr_registry` is what the AVS deployment JSON records under
/// [`SCHNORR_STAKE_REGISTRY_KEY`], read by the caller because it owns the parser for that file.
/// `SCHNORR_STAKE_REGISTRY_ADDRESS` wins over it, so a registry provisioned outside the chart's job
/// can be published without editing the deployment file the operators' jobs own.
pub async fn resolve<P: Provider>(
    provider: &P,
    registry_coordinator: Address,
    deployment_schnorr_registry: Option<Address>,
    deployment_path: &Path,
    config: &ContractsConfig,
) -> anyhow::Result<Resolution> {
    let mut records = Records::default();
    let Some(target) = reference_target(deployment_path, config, &mut records) else {
        return Ok(Resolution {
            contracts: None,
            incomplete: records.pending,
        });
    };

    let chain_id = provider
        .get_chain_id()
        .await
        .context("reading chain id for the published contract set")?;

    let sdk = GasKillerSDK::new(target, provider);
    let avs_address = sdk
        .avsAddress()
        .call()
        .await
        .with_context(|| format!("reading avsAddress() from reference target {target}"))?;
    let bls_signature_checker =
        sdk.blsSignatureChecker().call().await.with_context(|| {
            format!("reading blsSignatureChecker() from reference target {target}")
        })?;

    // The pairing check. A reference target left over from a superseded deployment reads back a
    // checker bound to that deployment's coordinator, and publishing it would hand every
    // integrator the wiring that reverts.
    let checker_coordinator = IBLSSignatureCheckerRegistry::new(bls_signature_checker, provider)
        .registryCoordinator()
        .call()
        .await
        .with_context(|| {
            format!("reading registryCoordinator() from checker {bls_signature_checker}")
        })?;
    if checker_coordinator != registry_coordinator {
        anyhow::bail!(
            "reference target {target} verifies against checker {bls_signature_checker}, whose \
             registry coordinator {checker_coordinator} is not the operators' coordinator \
             {registry_coordinator}: the target belongs to a superseded deployment"
        );
    }

    let demo_target = configured_or_recorded(
        config.demo_target,
        config.demo_target_file.as_deref(),
        &mut records,
    );
    let demo_factory = configured_or_recorded(
        config.demo_factory,
        config.demo_factory_file.as_deref(),
        &mut records,
    );
    let schnorr_stake_registry = registry_on_chain(
        provider,
        config
            .schnorr_stake_registry
            .or(deployment_schnorr_registry),
        &mut records,
    )
    .await?;

    Ok(Resolution {
        contracts: Some(AvsContracts {
            chain_id,
            avs_address,
            bls_signature_checker,
            registry_coordinator,
            schnorr_stake_registry,
            demo_target: deployed_on_chain(provider, "demoTarget", demo_target).await?,
            demo_factory: deployed_on_chain(provider, "demoFactory", demo_factory).await?,
        }),
        incomplete: records.pending,
    })
}

/// Keeps `slot` as complete as the deployment allows, publishing each improvement as it lands.
///
/// A failure is not fatal — the rest of `/avs-metadata` is identity information worth serving, and
/// an absent block reads as "no authoritative answer" rather than handing out a wrong one — but it
/// must not be permanent either. The providers this reads through are known to flap, which is why
/// the router carries an RPC circuit breaker, and the deploy jobs that record addresses can finish
/// after the router is already serving. Without retrying, either would leave the block absent for
/// the process lifetime, self-healing only on another restart.
///
/// The loop ends once an attempt reports a complete answer — including the complete answer "nothing
/// is configured to resolve from". Until then it publishes what each attempt established and tries
/// again with backoff, so a misconfiguration stays visible in the logs instead of scrolling past
/// once at startup, and a job that lands late upgrades what is already being served.
pub fn spawn_resolver<P: Provider + 'static>(
    provider: P,
    registry_coordinator: Address,
    deployment_schnorr_registry: Option<Address>,
    deployment_path: PathBuf,
    config: ContractsConfig,
    slot: ResolvedContracts,
) {
    tokio::spawn(async move {
        let mut backoff = RETRY_INITIAL_BACKOFF;
        loop {
            let attempt = resolve(
                &provider,
                registry_coordinator,
                deployment_schnorr_registry,
                &deployment_path,
                &config,
            );
            match tokio::time::timeout(RESOLVE_TIMEOUT, attempt).await {
                Ok(Ok(resolution)) => {
                    match resolution.contracts {
                        // Logged on a change only: while a record is outstanding the same set is
                        // re-established every attempt, and repeating it would drown the change.
                        Some(contracts) => {
                            let demo_target = contracts.demo_target;
                            let demo_factory = contracts.demo_factory;
                            let schnorr_stake_registry = contracts.schnorr_stake_registry;
                            let published = slot.publish(contracts.clone());
                            if published {
                                info!(
                                    avs_address = %contracts.avs_address,
                                    bls_signature_checker = %contracts.bls_signature_checker,
                                    registry_coordinator = %contracts.registry_coordinator,
                                    chain_id = contracts.chain_id,
                                    schnorr_stake_registry = ?schnorr_stake_registry,
                                    demo_target = ?demo_target,
                                    demo_factory = ?demo_factory,
                                    incomplete = resolution.incomplete,
                                    "publishing settlement contract addresses on /avs-metadata"
                                );
                            }
                        }
                        None if !resolution.incomplete => {
                            info!(
                                "no reference target configured ({REFERENCE_TARGET_FILE_ENV}, \
                                 {DEMO_TARGET_FILE_ENV} or AVS_REFERENCE_TARGET); /avs-metadata \
                                 will omit the contracts block"
                            );
                        }
                        None => {}
                    }
                    if !resolution.incomplete {
                        return;
                    }
                }
                Ok(Err(e)) => error!(
                    error = %e,
                    retry_in_secs = backoff.as_secs(),
                    "could not resolve settlement contract addresses; /avs-metadata omits them \
                     until this succeeds"
                ),
                Err(_) => error!(
                    timeout_secs = RESOLVE_TIMEOUT.as_secs(),
                    retry_in_secs = backoff.as_secs(),
                    "timed out resolving settlement contract addresses; /avs-metadata omits them \
                     until this succeeds"
                ),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
        }
    });
}

/// Path to `avs_deploy.json`, from `AVS_DEPLOYMENT_PATH`. Used only to locate the deploy job's
/// record beside it; the file's own contents are read through the shared loader.
pub fn deployment_path() -> Option<PathBuf> {
    non_empty_env("AVS_DEPLOYMENT_PATH").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::SolValue;
    use alloy_primitives::{Bytes, U64, U256, address};
    use alloy_provider::{ProviderBuilder, mock::Asserter};

    const TARGET: Address = address!("F143a9D93045474C2B573d21AC1CCe8dB2b06dbD");
    const AVS: Address = address!("dCec8ce0a03848B55989Bcc711e424Ca31d9eeD9");
    const CHECKER: Address = address!("6953fc47FC8b7568801f3fdc327bc0d9aD12E5b9");
    const COORDINATOR: Address = address!("0a032D62dde46670Ae40Ce532C97f6CE9Af72Dc4");
    const SUPERSEDED_COORDINATOR: Address = address!("00000000000000000000000000000000000000bb");
    const DEMO: Address = address!("00000000000000000000000000000000000000aa");
    const PLAYGROUND: Address = address!("00000000000000000000000000000000000000cc");
    const FACTORY: Address = address!("00000000000000000000000000000000000000dd");
    const REGISTRY: Address = address!("00000000000000000000000000000000000000ee");
    const OTHER_REGISTRY: Address = address!("00000000000000000000000000000000000000ff");
    fn write(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).expect("writing fixture");
        path
    }

    /// [`reference_target`] plus whether anything it tried is still outstanding.
    fn reference(deployment_path: &str, config: &ContractsConfig) -> (Option<Address>, bool) {
        let mut records = Records::default();
        let target = reference_target(Path::new(deployment_path), config, &mut records);
        (target, records.pending)
    }

    const NO_DEPLOYMENT: &str = "/nonexistent/avs_deploy.json";

    /// A path to a record nothing has written, for asserting how an unwritten one is treated.
    ///
    /// Deliberately not beside [`NO_DEPLOYMENT`]: a test naming one of these must not be satisfiable
    /// by the sibling lookup instead of the named path it is about.
    fn unwritten(name: &str) -> PathBuf {
        Path::new("/nonexistent/records").join(name)
    }

    // -- reference target selection --

    #[test]
    fn a_pinned_target_needs_no_recorded_file() {
        let config = ContractsConfig {
            reference_target: Some(TARGET),
            ..Default::default()
        };

        assert_eq!(reference(NO_DEPLOYMENT, &config), (Some(TARGET), false));
    }

    #[test]
    fn reads_the_target_from_the_named_record() {
        let dir = tempfile::tempdir().unwrap();
        // Written with a trailing newline by a shell redirect in some revisions of the job.
        let config = ContractsConfig {
            reference_target_file: Some(write(
                dir.path(),
                DEMO_TARGET_FILENAME,
                &format!("{TARGET}\n"),
            )),
            ..Default::default()
        };

        assert_eq!(
            reference(NO_DEPLOYMENT, &config),
            (Some(TARGET), false),
            "the job's own record tracks redeployments without anyone editing configuration"
        );
    }

    #[test]
    fn a_named_record_that_is_absent_leaves_the_answer_incomplete() {
        let config = ContractsConfig {
            reference_target_file: Some(unwritten(DEMO_TARGET_FILENAME)),
            ..Default::default()
        };

        assert_eq!(
            reference(NO_DEPLOYMENT, &config),
            (None, true),
            "the deploy job writes that file and can finish after the router is serving, so this \
             is worth another attempt rather than a final answer"
        );
    }

    #[test]
    fn the_playground_record_wins_over_the_smoke_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = ContractsConfig {
            demo_target_file: Some(write(
                dir.path(),
                "playground_target.txt",
                &format!("{PLAYGROUND}\n"),
            )),
            reference_target_file: Some(write(
                dir.path(),
                DEMO_TARGET_FILENAME,
                &TARGET.to_string(),
            )),
            ..Default::default()
        };

        // Reading the pair off the contract the docs point at is what keeps the published
        // blsSignatureChecker equal to demoTarget.blsSignatureChecker().
        assert_eq!(reference(NO_DEPLOYMENT, &config), (Some(PLAYGROUND), false));

        let pinned = ContractsConfig {
            reference_target: Some(TARGET),
            ..config
        };
        assert_eq!(
            reference(NO_DEPLOYMENT, &pinned),
            (Some(TARGET), false),
            "an operator's pin outranks both records"
        );
    }

    #[test]
    fn an_unwritten_playground_record_falls_through_to_the_smoke_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = ContractsConfig {
            demo_target_file: Some(dir.path().join("playground_target.txt")),
            reference_target_file: Some(write(
                dir.path(),
                DEMO_TARGET_FILENAME,
                &TARGET.to_string(),
            )),
            ..Default::default()
        };

        // The whole point of falling through: a playground job that has not finished — or has
        // failed for good — must not withhold the settlement pair, which has nothing to do with it.
        assert_eq!(
            reference(NO_DEPLOYMENT, &config),
            (Some(TARGET), true),
            "serve what is available, and keep trying for the record that is not"
        );
    }

    #[test]
    fn the_smoke_record_is_the_reference_when_no_playground_runs() {
        let dir = tempfile::tempdir().unwrap();
        let config = ContractsConfig {
            reference_target_file: Some(write(
                dir.path(),
                DEMO_TARGET_FILENAME,
                &TARGET.to_string(),
            )),
            ..Default::default()
        };

        assert_eq!(reference(NO_DEPLOYMENT, &config), (Some(TARGET), false));
    }

    #[test]
    fn falls_back_to_a_record_beside_the_deployment_file() {
        let dir = tempfile::tempdir().unwrap();
        let deployment = write(dir.path(), "avs_deploy.json", "{}");
        write(dir.path(), DEMO_TARGET_FILENAME, &TARGET.to_string());

        assert_eq!(
            reference(deployment.to_str().unwrap(), &ContractsConfig::default()),
            (Some(TARGET), false),
            "a deployment keeping both files on one volume needs no extra configuration"
        );
    }

    #[test]
    fn nothing_configured_is_a_complete_answer() {
        let dir = tempfile::tempdir().unwrap();
        let deployment = write(dir.path(), "avs_deploy.json", "{}");

        assert_eq!(
            reference(deployment.to_str().unwrap(), &ContractsConfig::default()),
            (None, false),
            "with no record named and none beside the deployment file there is nothing to wait for"
        );
    }

    #[test]
    fn a_malformed_address_is_an_error_not_a_silent_none() {
        assert!(
            parse_address("AVS_REFERENCE_TARGET", "0xnot-an-address").is_err(),
            "a typo must not fall through to a different source and publish another pair"
        );
        assert_eq!(
            parse_address("AVS_REFERENCE_TARGET", &TARGET.to_string()).unwrap(),
            TARGET
        );
    }

    // -- demo contract records --

    #[test]
    fn demo_contracts_fall_back_to_the_playground_records() {
        let dir = tempfile::tempdir().unwrap();
        let target = write(dir.path(), "playground_target.txt", &PLAYGROUND.to_string());
        let factory = write(dir.path(), "playground_factory.txt", &FACTORY.to_string());
        let mut records = Records::default();

        assert_eq!(
            configured_or_recorded(None, Some(&target), &mut records),
            Some(PLAYGROUND)
        );
        assert_eq!(
            configured_or_recorded(None, Some(&factory), &mut records),
            Some(FACTORY)
        );
        assert_eq!(
            configured_or_recorded(Some(DEMO), Some(&target), &mut records),
            Some(DEMO),
            "a configured address points readers somewhere other than what the job deployed"
        );
        assert_eq!(
            configured_or_recorded(None, None, &mut records),
            None,
            "with no playground deployed the demo fields are omitted, not defaulted"
        );
        assert!(
            !records.pending,
            "nothing above is outstanding, so the answer is final"
        );
    }

    #[test]
    fn a_record_holding_something_other_than_an_address_is_outstanding() {
        let dir = tempfile::tempdir().unwrap();
        // A job that failed mid-write, or wrote an error message where an address should be.
        let record = write(dir.path(), "playground_target.txt", "ERROR: forge failed\n");
        let mut records = Records::default();

        assert_eq!(
            configured_or_recorded(None, Some(&record), &mut records),
            None,
            "a malformed record must not become a published address"
        );
        assert!(
            records.pending,
            "and it must be retried rather than read as nothing configured"
        );
    }

    // -- resolution against a mocked provider --
    //
    // Responses are queued FIFO and consumed by each RPC call in the order `resolve` makes them:
    //   1. eth_chainId
    //   2. eth_call   avsAddress()
    //   3. eth_call   blsSignatureChecker()
    //   4. eth_call   registryCoordinator() on the checker
    //   5. eth_getCode then eth_call nextPossibleMutationBlock() on the schnorr registry, both
    //      only when one is configured
    //   6. eth_getCode demoTarget, then demoFactory, each only when one was established

    fn mock_provider() -> (impl Provider + Clone, Asserter) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        (provider, asserter)
    }

    fn push_address(asserter: &Asserter, address: Address) {
        asserter.push_success(&Bytes::from(address.abi_encode()));
    }

    /// Queues the four reads of a healthy deployment, with `checker_coordinator` as what the
    /// checker reports.
    fn push_wiring(asserter: &Asserter, checker_coordinator: Address) {
        asserter.push_success(&U64::from(11155111u64));
        push_address(asserter, AVS);
        push_address(asserter, CHECKER);
        push_address(asserter, checker_coordinator);
    }

    fn pinned(demo_target: Option<Address>) -> ContractsConfig {
        ContractsConfig {
            reference_target: Some(TARGET),
            demo_target,
            ..Default::default()
        }
    }

    async fn resolve_pinned<P: Provider>(provider: &P, config: &ContractsConfig) -> Resolution {
        resolve(
            provider,
            COORDINATOR,
            None,
            Path::new(NO_DEPLOYMENT),
            config,
        )
        .await
        .expect("a pinned reference target resolves without error")
    }

    /// Queues the two reads that accept a registry: code at the address, then the probe answering.
    fn push_registry(asserter: &Asserter) {
        asserter.push_success(&Bytes::from(vec![0x60u8]));
        asserter.push_success(&U256::from(u64::MAX).abi_encode());
    }

    #[tokio::test]
    async fn publishes_the_pair_a_live_target_verifies_against() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);

        let resolution = resolve_pinned(&provider, &pinned(None)).await;
        let contracts = resolution
            .contracts
            .expect("a pinned reference target has a set to publish");

        assert_eq!(contracts.chain_id, 11155111);
        assert_eq!(contracts.avs_address, AVS);
        assert_eq!(contracts.bls_signature_checker, CHECKER);
        assert_eq!(contracts.registry_coordinator, COORDINATOR);
        assert!(
            !resolution.incomplete,
            "nothing was left outstanding, so the resolver is done"
        );
    }

    #[tokio::test]
    async fn rejects_a_checker_bound_to_another_coordinator() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, SUPERSEDED_COORDINATOR);

        let err = resolve(
            &provider,
            COORDINATOR,
            None,
            Path::new(NO_DEPLOYMENT),
            &pinned(None),
        )
        .await
        .expect_err("a superseded pair must not be published");

        // This is the load-bearing check: the pair reads back cleanly and only the cross-check
        // tells it apart from a working one.
        let message = format!("{err}");
        assert!(
            message.contains("superseded deployment"),
            "expected the pairing check to reject it, got {message}"
        );
    }

    #[tokio::test]
    async fn resolves_to_nothing_without_a_reference_target() {
        let dir = tempfile::tempdir().unwrap();
        let deployment = write(dir.path(), "avs_deploy.json", "{}");
        let (provider, _asserter) = mock_provider();

        // No responses are queued: nothing to read the pair from means no RPC is made at all.
        let resolution = resolve(
            &provider,
            COORDINATOR,
            None,
            &deployment,
            &ContractsConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            resolution,
            Resolution {
                contracts: None,
                incomplete: false
            }
        );
    }

    #[tokio::test]
    async fn an_outstanding_demo_record_publishes_the_pair_and_asks_to_be_retried() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);

        let config = ContractsConfig {
            reference_target: Some(TARGET),
            demo_factory_file: Some(unwritten("playground_factory.txt")),
            ..Default::default()
        };
        let resolution = resolve_pinned(&provider, &config).await;

        let contracts = resolution
            .contracts
            .expect("the settlement pair does not depend on the playground job");
        assert_eq!(contracts.avs_address, AVS);
        assert!(contracts.demo_factory.is_none());
        assert!(
            resolution.incomplete,
            "the factory record is still coming, so the set is worth re-establishing"
        );
    }

    #[tokio::test]
    async fn publishes_a_demo_contract_that_has_code() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        asserter.push_success(&Bytes::from(vec![0x60u8]));

        let contracts = resolve_pinned(&provider, &pinned(Some(DEMO)))
            .await
            .contracts
            .expect("a pinned reference target has a set to publish");

        assert_eq!(contracts.demo_target, Some(DEMO));
    }

    #[tokio::test]
    async fn omits_a_demo_contract_with_no_code_on_this_chain() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        asserter.push_success(&Bytes::new());

        let contracts = resolve_pinned(&provider, &pinned(Some(DEMO)))
            .await
            .contracts
            .expect("a demo address on the wrong chain must not withhold the settlement pair");

        assert!(
            contracts.demo_target.is_none(),
            "chainId is published for every address in the block, so an address that is not \
             there cannot be one of them"
        );
    }

    // -- the schnorr stake registry --

    /// [`resolve`] with a registry recorded in the deployment JSON and nothing else configured.
    async fn resolve_with_recorded_registry<P: Provider>(
        provider: &P,
        recorded: Option<Address>,
        config: &ContractsConfig,
    ) -> Resolution {
        resolve(
            provider,
            COORDINATOR,
            recorded,
            Path::new(NO_DEPLOYMENT),
            config,
        )
        .await
        .expect("a pinned reference target resolves without error")
    }

    #[tokio::test]
    async fn publishes_the_registry_the_deployment_recorded() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        push_registry(&asserter);

        let resolution =
            resolve_with_recorded_registry(&provider, Some(REGISTRY), &pinned(None)).await;

        let contracts = resolution.contracts.expect("the pair still publishes");
        assert_eq!(contracts.schnorr_stake_registry, Some(REGISTRY));
        assert!(
            !resolution.incomplete,
            "a registry that answers leaves nothing outstanding"
        );
    }

    #[tokio::test]
    async fn the_env_override_wins_over_the_recorded_registry() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        push_registry(&asserter);

        let config = ContractsConfig {
            schnorr_stake_registry: Some(OTHER_REGISTRY),
            ..pinned(None)
        };
        let contracts = resolve_with_recorded_registry(&provider, Some(REGISTRY), &config)
            .await
            .contracts
            .expect("the pair still publishes");

        assert_eq!(
            contracts.schnorr_stake_registry,
            Some(OTHER_REGISTRY),
            "a registry provisioned outside the chart's job must be publishable without editing \
             the deployment file"
        );
    }

    #[tokio::test]
    async fn omits_a_registry_that_does_not_answer_the_probe() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        asserter.push_success(&Bytes::from(vec![0x60u8]));
        asserter.push_failure_msg("execution reverted");

        let resolution =
            resolve_with_recorded_registry(&provider, Some(REGISTRY), &pinned(None)).await;

        let contracts = resolution
            .contracts
            .expect("a contract that is not a registry must not withhold the settlement pair");
        assert!(
            contracts.schnorr_stake_registry.is_none(),
            "only nextPossibleMutationBlock() tells a registry apart from a checker pasted in \
             its place"
        );
        assert!(
            resolution.incomplete,
            "and it is worth another attempt: the address may be a registry deployed since"
        );
    }

    #[tokio::test]
    async fn omits_a_registry_with_no_code_and_asks_to_be_retried() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);
        asserter.push_success(&Bytes::new());

        let resolution =
            resolve_with_recorded_registry(&provider, Some(REGISTRY), &pinned(None)).await;

        let contracts = resolution.contracts.expect("the pair still publishes");
        assert!(contracts.schnorr_stake_registry.is_none());
        assert!(
            resolution.incomplete,
            "a registry recorded before it was deployed shows up on a later attempt"
        );
    }

    #[tokio::test]
    async fn no_registry_configured_is_a_complete_answer() {
        let (provider, asserter) = mock_provider();
        push_wiring(&asserter, COORDINATOR);

        // No registry response is queued: with none configured, neither read is made.
        let resolution = resolve_pinned(&provider, &pinned(None)).await;

        let contracts = resolution.contracts.expect("the pair still publishes");
        assert!(contracts.schnorr_stake_registry.is_none());
        assert!(
            !resolution.incomplete,
            "a BLS-only deployment has provisioned no registry and never will; retrying forever \
             would log a misconfiguration that is not one"
        );
    }

    /// The router reads the registry back through the same deployment loader the rest of the
    /// service uses, and that loader names only three keys explicitly: anything else reaches it
    /// through the `extra` map behind `custom_address`. Pinned against a file in the shape the
    /// operator-set job writes, because a rename on either side degrades into an absent field
    /// rather than an error.
    #[test]
    fn the_deployment_loader_finds_the_recorded_registry() {
        let recorded = format!(
            r#"{{"addresses":{{"registryCoordinator":"{COORDINATOR:?}","blsapkRegistry":"{AVS:?}",
                "blsSigCheck":"{CHECKER:?}","{SCHNORR_STAKE_REGISTRY_KEY}":"{REGISTRY:?}"}}}}"#
        );
        let deployment: commonware_avs_eigenlayer::AvsDeployment =
            serde_json::from_str(&recorded).expect("a recorded deployment parses");

        assert_eq!(
            deployment
                .custom_address(SCHNORR_STAKE_REGISTRY_KEY)
                .expect("the recorded registry is readable"),
            REGISTRY
        );

        // A deployment that has provisioned no registry, which is every BLS-only one. The loader
        // reports it as an error, which the router degrades to "none configured".
        let bare = format!(
            r#"{{"addresses":{{"registryCoordinator":"{COORDINATOR:?}","blsapkRegistry":"{AVS:?}",
                "blsSigCheck":"{CHECKER:?}"}}}}"#
        );
        let deployment: commonware_avs_eigenlayer::AvsDeployment =
            serde_json::from_str(&bare).expect("a deployment with no registry still parses");
        assert!(
            deployment
                .custom_address(SCHNORR_STAKE_REGISTRY_KEY)
                .is_err(),
            "an absent key must be distinguishable from a recorded one"
        );
    }

    // -- serialization and the published slot --

    fn contracts_with(demo_target: Option<Address>) -> AvsContracts {
        AvsContracts {
            chain_id: 11155111,
            avs_address: AVS,
            bls_signature_checker: CHECKER,
            registry_coordinator: COORDINATOR,
            schnorr_stake_registry: None,
            demo_target,
            demo_factory: None,
        }
    }

    #[test]
    fn addresses_publish_in_checksummed_form() {
        let contracts = contracts_with(None);

        let json = serde_json::to_value(&contracts).unwrap();
        // Solidity rejects a lowercase address literal, so a mixed-case rendering is the contract
        // with an integrator, not cosmetic.
        assert_eq!(
            json["avsAddress"],
            "0xdCec8ce0a03848B55989Bcc711e424Ca31d9eeD9"
        );
        assert_eq!(
            json["blsSignatureChecker"],
            "0x6953fc47FC8b7568801f3fdc327bc0d9aD12E5b9"
        );
        assert_eq!(
            json["registryCoordinator"],
            "0x0a032D62dde46670Ae40Ce532C97f6CE9Af72Dc4"
        );

        // Round-trips: a client that parses what it was served gets the same addresses back.
        let parsed: AvsContracts = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, contracts);

        // The optional addresses take the same treatment, which is a separate serializer.
        let with_optionals = AvsContracts {
            schnorr_stake_registry: Some(address!("b4d3f7c1c8a2e5901f6d0b7a3e9c2d4f5a6b7c8d")),
            ..contracts_with(Some(address!("dcec8ce0a03848b55989bcc711e424ca31d9eed9")))
        };
        let json = serde_json::to_value(&with_optionals).unwrap();
        assert_eq!(
            json["schnorrStakeRegistry"],
            "0xB4d3f7C1c8A2E5901f6d0B7A3e9c2D4F5a6b7c8D"
        );
        assert_eq!(
            json["demoTarget"],
            "0xdCec8ce0a03848B55989Bcc711e424Ca31d9eeD9"
        );
        assert_eq!(
            serde_json::from_value::<AvsContracts>(json).unwrap(),
            with_optionals
        );
    }

    #[test]
    fn an_unresolved_slot_serializes_as_null_and_reports_itself_unresolved() {
        let slot = ResolvedContracts::default();
        assert!(slot.is_unresolved());
        assert!(slot.snapshot().is_none());
        assert_eq!(
            serde_json::to_value(&slot).unwrap(),
            serde_json::Value::Null
        );

        assert!(slot.publish(contracts_with(None)));
        assert!(!slot.is_unresolved());
        assert_eq!(
            serde_json::to_value(&slot).unwrap()["avsAddress"],
            "0xdCec8ce0a03848B55989Bcc711e424Ca31d9eeD9"
        );
    }

    #[test]
    fn republishing_the_same_set_is_not_a_change_but_a_better_one_is() {
        let slot = ResolvedContracts::default();
        assert!(slot.publish(contracts_with(None)));

        // The resolver re-establishes the same set on every attempt while a record is outstanding;
        // reporting that as a change would log it once a minute forever.
        assert!(
            !slot.publish(contracts_with(None)),
            "an identical set is not news"
        );

        // And when the record finally lands, the fuller set replaces what is being served.
        assert!(slot.publish(contracts_with(Some(DEMO))));
        assert_eq!(
            slot.snapshot().and_then(|c| c.demo_target),
            Some(DEMO),
            "a late record upgrades the published set instead of being lost to a write-once slot"
        );
    }
}
