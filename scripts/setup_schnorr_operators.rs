//! Deploys the `SchnorrStakeRegistry` and registers the operator set against it.
//!
//! This is AVS operator-set construction, not contract deployment, which is why it is its own
//! binary rather than a mode of `deploy_example`. The arguments it submits are derived from
//! operator secret key material at runtime — a proof of possession per operator, generated with
//! a live RNG — so they cannot be expressed as manifest values the way a constructor's can.
//!
//! **It is a phase, and ordering is load-bearing.** Every registration advances the registry's
//! `effectiveBlock` watermark, and verification fail-closes for reference blocks behind it, so
//! the whole operator set must be registered *before* any target contract is deployed. Run this
//! first, then `deploy_example`, which reads the registry address back out of the deployment
//! JSON via `$deploy:schnorrStakeRegistry`.
//!
//! `SCHNORR_REGISTRY_ONLY` deploys and records the registry without registering anyone, for a
//! deployment publishing the address before it has an operator set to put in it: a target's
//! constructor takes that address, so an integrator can wire one and settle under whichever
//! scheme the fleet runs. The registry verifies nothing until operators are registered, which its
//! owner can do later into the same registry.
//!
//! Only meaningful under `SIGNATURE_SCHEME=schnorr`; the BLS stack verifies against a
//! `BLSSignatureChecker` from the EigenLayer deployment and needs none of this.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use gas_killer_common::avs_contracts::SCHNORR_STAKE_REGISTRY_KEY;
use gas_killer_common::schnorr::{PrivateKey, private_key_from_hex};
use gas_killer_common::{
    SignatureScheme, quorum_threshold_fraction, schnorr_notice_window, signature_scheme,
};
use rand::RngCore;
use scripts::bindings::schnorrstakeregistry::SchnorrStakeRegistry;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Operator key files the eigenlayer setup container writes next to the deployment JSON. The
/// Schnorr signing key IS the operator's secp256k1 key.
const OPERATOR_KEY_FILE_SUFFIX: &str = ".private.ecdsa.key.json";

#[derive(Debug, Deserialize)]
struct OperatorKeyFile {
    #[serde(rename = "privateKey")]
    private_key: String,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    dotenv::dotenv().ok();

    if signature_scheme() != SignatureScheme::Schnorr {
        println!(
            "⏭️  SIGNATURE_SCHEME is not 'schnorr'; nothing to do (the BLS stack verifies \
             against a BLSSignatureChecker from the EigenLayer deployment)"
        );
        return Ok(());
    }

    let http_rpc = env::var("HTTP_RPC").map_err(|_| "HTTP_RPC environment variable is required")?;
    let private_key =
        env::var("PRIVATE_KEY").map_err(|_| "PRIVATE_KEY environment variable is required")?;
    let avs_deployment_path = env::var("AVS_DEPLOYMENT_PATH")
        .map_err(|_| "AVS_DEPLOYMENT_PATH environment variable is required")?;

    // The on-chain stake threshold, fixed at registry deployment. Must match the router
    // coordinator's local participation floor (same env vars, see `quorum_threshold_fraction`).
    let (threshold_num, threshold_den) = quorum_threshold_fraction();

    // Blocks an operator-set change must be announced ahead of applying, fixed at registry
    // deployment. Zero for the e2e stack: the whole operator set is registered before any target
    // deploys, so no round is ever in flight for a mutation to invalidate.
    let notice_window = schnorr_notice_window();

    // The AVS reference comes from the eigenlayer deployment JSON — operators register with
    // EigenLayer through the same service manager regardless of the quorum-signature scheme the
    // target contract verifies.
    let avs_address = read_avs_address(&avs_deployment_path)?;

    // Deploy the registry and stop. Read before anything is loaded or deployed so the choice is
    // visible in the log above the addresses it produces.
    let registry_only = env_flag("SCHNORR_REGISTRY_ONLY");

    // The operators' Schnorr keys are their existing secp256k1 keys, read from the key files the
    // eigenlayer setup container produced. Loaded before the registry exists, so a volume missing
    // them fails with the reason rather than after deploying a registry nobody is in.
    let operator_keys = if registry_only {
        println!(
            "📋 SCHNORR_REGISTRY_ONLY: deploying the registry without registering an operator set"
        );
        Vec::new()
    } else {
        let keys = load_operator_keys(&avs_deployment_path)?;
        println!("🔑 Loaded {} operator key(s)", keys.len());
        keys
    };

    // The deployer owns the registry (stand-in for the EigenLayer registration lifecycle).
    let signer: PrivateKeySigner = private_key
        .parse()
        .map_err(|_| "Invalid private key format")?;
    let deployer = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(http_rpc.parse().map_err(|_| "Invalid RPC URL")?);

    let code_avs = provider
        .get_code_at(avs_address)
        .await
        .map_err(|e| format!("Failed to get code for AVS address {avs_address}: {e}"))?;
    if code_avs.is_empty() {
        return Err(format!(
            "AVS service manager {avs_address} has no code deployed. Check AVS_DEPLOYMENT_PATH."
        )
        .into());
    }

    // Reuse SCHNORR_STAKE_REGISTRY_ADDRESS when it points at deployed code (its operator set is
    // assumed already registered), otherwise deploy a fresh registry and register everyone.
    let (registry_address, register_operators) = match env_address(
        "SCHNORR_STAKE_REGISTRY_ADDRESS",
    )? {
        Some(addr) => {
            let code = provider
                .get_code_at(addr)
                .await
                .map_err(|e| format!("Failed to get code for SchnorrStakeRegistry {addr}: {e}"))?;
            if code.is_empty() {
                return Err(format!(
                    "SCHNORR_STAKE_REGISTRY_ADDRESS {addr} has no code deployed; unset it to \
                     deploy a fresh registry"
                )
                .into());
            }
            println!(
                "🏦 Using existing SchnorrStakeRegistry at: {addr} (skipping operator registrations)"
            );
            (addr, false)
        }
        None => {
            println!(
                "🏦 Deploying SchnorrStakeRegistry (threshold {threshold_num}/{threshold_den}, \
                 owner {deployer}, notice window {notice_window} blocks)..."
            );
            let registry = SchnorrStakeRegistry::deploy(
                provider.clone(),
                U256::from(threshold_num),
                U256::from(threshold_den),
                deployer,
                U256::from(notice_window),
            )
            .await
            .map_err(|e| format!("Failed to deploy SchnorrStakeRegistry: {e}"))?;
            let address = *registry.address();
            println!("✅ SchnorrStakeRegistry deployed at: {address}");
            (address, true)
        }
    };

    if registry_only {
        println!(
            "⏭️  SCHNORR_REGISTRY_ONLY: no operator registered. The registry verifies nothing \
             until one is, and its owner {deployer} is who can register them."
        );
    } else if register_operators {
        register_operator_set(&provider, registry_address, &operator_keys).await?;
    }

    record_registry_address(&avs_deployment_path, registry_address)?;

    println!("\n🎉 Schnorr operator set ready");
    println!("  SchnorrStakeRegistry: {registry_address}");
    println!("  AVS service manager:  {avs_address}");
    println!(
        "\nNext: deploy a target, which reads the registry via \
         $deploy:{SCHNORR_STAKE_REGISTRY_KEY}\n  \
         cargo run -p scripts --bin deploy_example -- --example schnorrArraySummation"
    );
    Ok(())
}

/// Registers every operator's Schnorr key with a fresh proof of possession.
///
/// MUST complete before any target deploys: the registry's `effectiveBlock` watermark advances
/// on every registration, and verification fail-closes for reference blocks behind it.
async fn register_operator_set<P: Provider + Clone>(
    provider: &P,
    registry_address: Address,
    operator_keys: &[PrivateKey],
) -> Result<(), DynError> {
    let registry = SchnorrStakeRegistry::new(registry_address, provider.clone());
    let mut rng = rand::rng();
    let mut fill = |b: &mut [u8]| rng.fill_bytes(b);

    for key in operator_keys {
        let pubkey = key.public_key();
        let operator = pubkey.eth_address();
        let pop = key.prove_possession(&mut fill);
        // Cheap local check before spending gas: the registry verifies the same PoP.
        if !pubkey.verify_possession(&pop) {
            return Err(format!(
                "locally generated proof of possession failed to verify for operator {operator}"
            )
            .into());
        }
        let pop_bytes = pop.0.to_bytes();
        let receipt = registry
            .registerOperator(
                U256::from_be_bytes(pubkey.x_bytes()),
                U256::from_be_bytes(pubkey.y_bytes()),
                // Uniform weight: the e2e stack has no stake differentiation.
                U256::from(1),
                U256::from_be_slice(&pop_bytes[..32]),
                Address::from_slice(&pop_bytes[32..]),
            )
            .send()
            .await
            .map_err(|e| format!("Failed to send registerOperator for {operator}: {e}"))?
            .get_receipt()
            .await
            .map_err(|e| {
                format!("registerOperator transaction for {operator} failed or was not mined: {e}")
            })?;
        if !receipt.status() {
            return Err(format!("registerOperator reverted for operator {operator}").into());
        }
        println!("✅ Registered operator {operator} (weight 1)");
    }
    Ok(())
}

/// Loads every operator's secp256k1 key from the `*.private.ecdsa.key.json` files the eigenlayer
/// setup container writes next to the deployment JSON (override the directory with
/// `OPERATOR_KEYS_DIR`). Sorted by filename for a deterministic registration order.
fn load_operator_keys(avs_deployment_path: &str) -> Result<Vec<PrivateKey>, DynError> {
    let keys_dir: PathBuf = match env::var("OPERATOR_KEYS_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        Some(dir) => PathBuf::from(dir),
        None => Path::new(avs_deployment_path)
            .parent()
            .map(|p| p.join("operator_keys"))
            .unwrap_or_else(|| PathBuf::from("operator_keys")),
    };
    println!("🔑 Loading operator keys from: {}", keys_dir.display());

    let mut key_files: Vec<PathBuf> = fs::read_dir(&keys_dir)
        .map_err(|e| {
            format!(
                "Failed to read operator keys directory {}: {e}",
                keys_dir.display()
            )
        })?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(OPERATOR_KEY_FILE_SUFFIX))
        })
        .collect();
    key_files.sort();

    if key_files.is_empty() {
        return Err(format!(
            "no *{OPERATOR_KEY_FILE_SUFFIX} files found in {} — the eigenlayer setup container \
             writes them next to the deployment JSON (or set OPERATOR_KEYS_DIR)",
            keys_dir.display()
        )
        .into());
    }

    let mut keys = Vec::with_capacity(key_files.len());
    for file in &key_files {
        let content = fs::read_to_string(file)
            .map_err(|e| format!("Failed to read operator key file {}: {e}", file.display()))?;
        let parsed: OperatorKeyFile = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse operator key file {}: {e}", file.display()))?;
        let key = private_key_from_hex(&parsed.private_key)
            .ok_or_else(|| format!("Invalid privateKey in operator key file {}", file.display()))?;
        keys.push(key);
    }
    Ok(keys)
}

/// Reads `addresses.avsServiceManagerWrapper` from the eigenlayer deployment JSON.
fn read_avs_address(avs_deployment_path: &str) -> Result<Address, DynError> {
    println!("📖 Reading AVS deployment from: {avs_deployment_path}");
    let content = fs::read_to_string(avs_deployment_path)
        .map_err(|e| format!("Failed to read AVS deployment file: {e}"))?;
    let parsed: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse AVS deployment JSON: {e}"))?;
    let raw = parsed
        .get("addresses")
        .and_then(|a| a.get("avsServiceManagerWrapper"))
        .and_then(|v| v.as_str())
        .ok_or("addresses.avsServiceManagerWrapper missing from the deployment JSON")?;
    raw.parse()
        .map_err(|_| format!("Invalid avsServiceManagerWrapper address: {raw}").into())
}

/// Records the registry under `addresses.schnorrStakeRegistry`, preserving every other key.
/// This is the handoff to `deploy_example`, which resolves it as `$deploy:schnorrStakeRegistry`.
fn record_registry_address(avs_deployment_path: &str, registry: Address) -> Result<(), DynError> {
    let content = fs::read_to_string(avs_deployment_path)
        .map_err(|e| format!("Failed to read deployment JSON for updating: {e}"))?;
    let mut deployment: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse deployment JSON for updating: {e}"))?;

    if !deployment["addresses"].is_object() {
        deployment["addresses"] = serde_json::json!({});
    }
    deployment["addresses"][SCHNORR_STAKE_REGISTRY_KEY] =
        serde_json::json!(format!("{registry:?}"));

    let serialized = serde_json::to_string_pretty(&deployment)
        .map_err(|e| format!("Failed to serialize deployment JSON: {e}"))?;
    fs::write(avs_deployment_path, serialized)
        .map_err(|e| format!("Failed to write deployment JSON: {e}"))?;
    println!("📝 recorded addresses.{SCHNORR_STAKE_REGISTRY_KEY} = {registry:?}");
    Ok(())
}

/// Reads a boolean environment variable, accepting `true` and `1` case-insensitively. Anything
/// else, including unset, is false: a mistyped flag must not silently skip the registrations.
fn env_flag(name: &str) -> bool {
    parse_flag(env::var(name).ok().as_deref())
}

fn parse_flag(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true" | "1")
    )
}

fn env_address(name: &str) -> Result<Option<Address>, DynError> {
    match env::var(name).ok().filter(|s| !s.trim().is_empty()) {
        Some(raw) => {
            Ok(Some(raw.trim().parse().map_err(|_| {
                format!("Invalid address in {name}: {raw}")
            })?))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_flag;

    #[test]
    fn only_true_and_one_enable_a_flag() {
        for raw in ["true", "TRUE", " True ", "1"] {
            assert!(parse_flag(Some(raw)), "{raw} should enable the flag");
        }
        // A mistyped value reads as off, which runs the registrations rather than skipping them.
        for raw in ["", "yes", "on", "0", "false", "ture"] {
            assert!(!parse_flag(Some(raw)), "{raw} should not enable the flag");
        }
        assert!(!parse_flag(None));
    }
}
