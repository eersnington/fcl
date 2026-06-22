#![deny(unsafe_code)]

mod checkout;
mod clone;
mod error;
mod local;
mod metrics;
mod pack;
mod protocol;
mod repo;

pub use clone::{CloneRequest, clone_repo};
pub use error::CloneError;
pub use local::{LocalCloneReport, LocalCloneRequest, local_clone};
pub use metrics::CloneReport;
