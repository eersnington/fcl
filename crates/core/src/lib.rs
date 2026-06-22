#![deny(unsafe_code)]

mod checkout;
mod clone;
mod compression;
mod error;
mod git_object;
mod local;
mod metrics;
mod pack;
mod protocol;
mod repo;

pub use clone::{
    CloneProgressEvent, CloneProgressPhase, CloneRequest, clone_repo, clone_repo_with_progress,
};
pub use compression::compression_backend;
pub use error::CloneError;
pub use local::{
    LocalCloneProgressEvent, LocalCloneProgressPhase, LocalCloneReport, LocalCloneRequest,
    local_clone, local_clone_with_progress,
};
pub use metrics::CloneReport;
