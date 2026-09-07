// Gas killer router: verifier-only certificate collector, task sequencer, and
// on-chain submitter around the commonware aggregation engine.
pub mod directive_metrics;
pub mod error;
pub mod executor;
pub mod expiry;
pub mod factories;
pub mod height_metrics;
pub mod ingress;
#[cfg(test)]
mod log_capture;
pub mod metrics;
pub mod openapi;
pub mod operator_http;
pub mod payload_revert;
pub mod rate_limit;
pub mod rpc_health;
pub mod schnorr_coordinator;
pub mod schnorr_submitter;
pub mod sequencer;
pub mod store;

// Re-export task_data from common crate
pub mod task_data {
    pub use gas_killer_common::task_data::GasKillerTaskData;
}

// Re-export validator from common crate
pub mod validator {
    pub use gas_killer_common::validator::*;
}

// Re-export main types for easy access
pub use commonware_avs_router::executor::ExecutionResult;
pub use error::{ApiError, ApiErrorBody, ApiErrorEnvelope, ApiJson, ErrorCode};
pub use executor::GasKillerHandler;
pub use gas_killer_common::GasKillerTaskData;
pub use gas_killer_common::GasKillerValidator;
pub use ingress::{GasKillerTaskRequest, GasKillerTaskRequestBody, ValidationError};
pub use store::{ApiKeyMetadata, CreatedApiKey, SqliteStore, Task, TaskStatus};
