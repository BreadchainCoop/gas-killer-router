//! Construction of read-only RPC providers shared across components.
//!
//! Reads the per-chain RPC endpoints from the environment (`HTTP_RPC`, `L2_HTTP_RPC`)
//! and builds one provider per chain.
//!
//! Simulation is addressed separately (`SIM_HTTP_RPC`, `L2_SIM_HTTP_RPC`): extraction may need a
//! node with a lifted `debug_traceCall` cap while settlement still has to reach the real chain.

use std::collections::HashMap;
use std::env;

use url::Url;

use crate::ReadOnlyProvider;
use crate::config::ChainRole;

/// Reads the per-chain RPC URLs from the environment.
///
/// - `HTTP_RPC` → [`ChainRole::L1`] (required)
/// - `L2_HTTP_RPC` → [`ChainRole::L2`] (optional)
///
/// Returns an error if `HTTP_RPC` is not set.
pub fn chain_rpc_urls_from_env() -> anyhow::Result<HashMap<ChainRole, String>> {
    let mut urls = HashMap::new();

    let l1_rpc = env::var("HTTP_RPC")
        .map_err(|_| anyhow::anyhow!("HTTP_RPC environment variable is not set"))?;
    Url::parse(&l1_rpc).map_err(|e| anyhow::anyhow!("HTTP_RPC is not a valid URL: {e}"))?;
    urls.insert(ChainRole::L1, l1_rpc);

    if let Ok(l2_rpc) = env::var("L2_HTTP_RPC") {
        Url::parse(&l2_rpc).map_err(|e| anyhow::anyhow!("L2_HTTP_RPC is not a valid URL: {e}"))?;
        urls.insert(ChainRole::L2, l2_rpc);
    }

    Ok(urls)
}

/// Reads the per-chain simulation RPC URLs, defaulting each to that chain's `HTTP_RPC`.
///
/// - `SIM_HTTP_RPC` -> [`ChainRole::L1`]
/// - `L2_SIM_HTTP_RPC` -> [`ChainRole::L2`]
///
/// Only extraction and gas estimation run against these. Settlement, chain detection and
/// `stateTransitionCount` reads stay on the chain RPC, so the simulation endpoint may be a fork
/// that never sees a transaction.
///
/// The split exists because `GK_SIM_PROFILE=unbounded` needs a node whose `debug_traceCall`
/// execution cap is lifted, and hosted endpoints clamp it silently — returning a truncated trace
/// rather than an error. An empty value counts as unset, so a chart that always renders the
/// variable still gets the default.
///
/// A role absent from `chain_rpc_urls` is rejected rather than inserted: a simulation endpoint for
/// a chain the deployment does not serve would never be read, so accepting it would hide the typo.
pub fn sim_rpc_urls_from_env(
    chain_rpc_urls: &HashMap<ChainRole, String>,
) -> anyhow::Result<HashMap<ChainRole, String>> {
    sim_rpc_urls_with(chain_rpc_urls, |var| env::var(var).ok())
}

/// [`sim_rpc_urls_from_env`] with the environment injected, so the precedence rules are testable
/// without mutating process-global state.
fn sim_rpc_urls_with<F>(
    chain_rpc_urls: &HashMap<ChainRole, String>,
    lookup: F,
) -> anyhow::Result<HashMap<ChainRole, String>>
where
    F: Fn(&str) -> Option<String>,
{
    let mut urls = chain_rpc_urls.clone();

    for (role, var) in [
        (ChainRole::L1, "SIM_HTTP_RPC"),
        (ChainRole::L2, "L2_SIM_HTTP_RPC"),
    ] {
        let Some(raw) = lookup(var) else { continue };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        if !chain_rpc_urls.contains_key(&role) {
            anyhow::bail!("{var} is set but {role} has no chain RPC configured");
        }
        Url::parse(raw).map_err(|e| anyhow::anyhow!("{var} is not a valid URL: {e}"))?;
        urls.insert(role, raw.to_string());
    }

    Ok(urls)
}

/// Builds a read-only HTTP provider for each chain URL.
///
/// URLs that fail to parse are skipped with a warning; a chain with no provider
/// has its lookups error at call time.
pub fn build_read_providers(
    chain_rpc_urls: &HashMap<ChainRole, String>,
) -> HashMap<ChainRole, ReadOnlyProvider> {
    use alloy_provider::ProviderBuilder;

    let mut providers = HashMap::with_capacity(chain_rpc_urls.len());
    for (&chain_id, rpc_url) in chain_rpc_urls {
        match Url::parse(rpc_url) {
            Ok(url) => {
                providers.insert(chain_id, ProviderBuilder::new().connect_http(url));
            }
            Err(e) => {
                tracing::warn!(
                    chain = %chain_id,
                    error = %e,
                    "Skipping read provider for chain with unparseable RPC URL"
                );
            }
        }
    }
    providers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_a_provider_per_valid_url() {
        let mut urls = HashMap::new();
        urls.insert(ChainRole::L1, "https://example.com".to_string());
        urls.insert(ChainRole::L2, "https://l2.example.com".to_string());

        let providers = build_read_providers(&urls);

        assert_eq!(providers.len(), 2);
        assert!(providers.contains_key(&ChainRole::L1));
        assert!(providers.contains_key(&ChainRole::L2));
    }

    #[test]
    fn skips_unparseable_urls() {
        let mut urls = HashMap::new();
        urls.insert(ChainRole::L1, "https://example.com".to_string());
        urls.insert(ChainRole::L2, "not a url".to_string());

        let providers = build_read_providers(&urls);

        assert!(providers.contains_key(&ChainRole::L1));
        assert!(!providers.contains_key(&ChainRole::L2));
    }

    fn chain_urls() -> HashMap<ChainRole, String> {
        let mut urls = HashMap::new();
        urls.insert(ChainRole::L1, "https://chain.example".to_string());
        urls
    }

    #[test]
    fn sim_urls_default_to_the_chain_urls() {
        let chain = chain_urls();
        let sim = sim_rpc_urls_with(&chain, |_| None).unwrap();
        assert_eq!(sim, chain);
    }

    #[test]
    fn sim_url_overrides_only_its_own_role() {
        let mut chain = chain_urls();
        chain.insert(ChainRole::L2, "https://l2.example".to_string());

        let sim = sim_rpc_urls_with(&chain, |var| {
            (var == "SIM_HTTP_RPC").then(|| "http://anvil:8545".to_string())
        })
        .unwrap();

        assert_eq!(sim[&ChainRole::L1], "http://anvil:8545");
        assert_eq!(sim[&ChainRole::L2], "https://l2.example");
    }

    /// A chart that always renders the variable emits `value: ""` when it is unset.
    #[test]
    fn blank_sim_url_falls_back_rather_than_failing() {
        let chain = chain_urls();
        let sim = sim_rpc_urls_with(&chain, |_| Some("   ".to_string())).unwrap();
        assert_eq!(sim, chain);
    }

    #[test]
    fn unparseable_sim_url_is_an_error() {
        let chain = chain_urls();
        let err = sim_rpc_urls_with(&chain, |var| {
            (var == "SIM_HTTP_RPC").then(|| "not a url".to_string())
        })
        .unwrap_err();
        assert!(err.to_string().contains("SIM_HTTP_RPC"), "{err}");
    }

    /// Silently inserting it would leave a typo'd variable looking configured but never read.
    #[test]
    fn sim_url_for_an_unserved_chain_is_an_error() {
        let chain = chain_urls();
        let err = sim_rpc_urls_with(&chain, |var| {
            (var == "L2_SIM_HTTP_RPC").then(|| "http://anvil-l2:8545".to_string())
        })
        .unwrap_err();
        assert!(err.to_string().contains("L2_SIM_HTTP_RPC"), "{err}");
    }
}
