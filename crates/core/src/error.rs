use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CloneError {
    #[error(
        "unsupported URL scheme for `{url}`; supported schemes: {supported}. Use an HTTPS Git URL for the current fcl build."
    )]
    UnsupportedUrlScheme { url: String, supported: String },

    #[error(
        "could not derive a target directory from `{url}`. Pass an explicit target path, for example `fcl {url} repo`."
    )]
    TargetNameMissing { url: String },

    #[error(
        "target directory `{path}` already exists. fcl leaves existing data untouched; choose another target or remove it yourself."
    )]
    TargetAlreadyExists { path: PathBuf },

    #[error("failed to create repository layout at `{path}` while {operation}: {source}")]
    RepoLayoutFailed {
        path: PathBuf,
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("remote discovery failed for `{url}` while {operation}: {detail}")]
    RemoteDiscoveryFailed {
        url: String,
        operation: &'static str,
        detail: String,
    },

    #[error("remote `{url}` does not support required capability `{capability}`. {remediation}")]
    UnsupportedRemoteCapability {
        url: String,
        capability: &'static str,
        remediation: &'static str,
    },

    #[error("remote response from `{url}` was malformed while {operation}: {detail}")]
    MalformedRemoteResponse {
        url: String,
        operation: &'static str,
        detail: String,
    },

    #[error("failed to write pack data to `{path}`: {source}")]
    PackWriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "pack checksum mismatch for `{path}`: expected trailer {expected}, computed {actual}. The target contains only fcl temporary clone data and should be removed before retrying."
    )]
    PackChecksumMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },

    #[error("pack index failed for `{path}` while {operation}: {detail}")]
    PackIndexFailed {
        path: PathBuf,
        operation: &'static str,
        detail: String,
    },

    #[error("object lookup failed for {oid}: expected {expected_type}; {detail}")]
    ObjectLookupFailed {
        oid: String,
        expected_type: &'static str,
        detail: String,
    },

    #[error("failed to parse {object_type} object {oid} while {operation}: {detail}")]
    ObjectParseFailed {
        oid: String,
        object_type: &'static str,
        operation: &'static str,
        detail: String,
    },

    #[error(
        "checkout failed for `{path}` while {operation}: {detail}. The repository object database and refs were written, but the working tree checkout may be incomplete. Remove the target directory and retry."
    )]
    CheckoutFailed {
        path: PathBuf,
        operation: &'static str,
        detail: String,
    },

    #[error("benchmark failed while {operation}: {detail}")]
    BenchmarkFailed {
        operation: &'static str,
        detail: String,
    },

    #[error(
        "clone safety limit exceeded while {operation}: {detail}. The target contains only fcl temporary clone data and should be removed before retrying if you do not need it."
    )]
    CloneLimitExceeded {
        operation: &'static str,
        detail: String,
    },

    #[error("local clone failed for `{path}` while {operation}: {detail}")]
    LocalCloneFailed {
        path: PathBuf,
        operation: &'static str,
        detail: String,
    },
}

impl CloneError {
    pub const fn repo_layout(
        path: PathBuf,
        operation: &'static str,
        source: std::io::Error,
    ) -> Self {
        Self::RepoLayoutFailed {
            path,
            operation,
            source,
        }
    }
}
