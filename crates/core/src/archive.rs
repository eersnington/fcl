use std::fs;
use std::path::{Component, Path, PathBuf};

use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use tar::Archive;
use url::Url;

use crate::error::CloneError;

pub fn archive_checkout_enabled() -> bool {
    matches!(
        std::env::var("FCL_ARCHIVE_CHECKOUT").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "on")
    )
}

pub fn checkout_github_archive(url: &str, commit: &str, target: &Path) -> Result<(), CloneError> {
    let archive_url = github_archive_url(url, commit)?;
    let response = Client::builder()
        .build()
        .map_err(|error| CloneError::ArchiveCheckoutFailed {
            operation: "building archive HTTP client",
            detail: error.to_string(),
        })?
        .get(archive_url)
        .send()
        .map_err(|error| CloneError::ArchiveCheckoutFailed {
            operation: "downloading GitHub archive",
            detail: error.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(CloneError::ArchiveCheckoutFailed {
            operation: "downloading GitHub archive",
            detail: format!("server returned HTTP {}", response.status()),
        });
    }

    let decoder = GzDecoder::new(response);
    let mut archive = Archive::new(decoder);
    for entry in archive
        .entries()
        .map_err(|error| CloneError::ArchiveCheckoutFailed {
            operation: "reading archive entries",
            detail: error.to_string(),
        })?
    {
        let mut entry = entry.map_err(|error| CloneError::ArchiveCheckoutFailed {
            operation: "reading archive entry",
            detail: error.to_string(),
        })?;
        let path = entry
            .path()
            .map_err(|error| CloneError::ArchiveCheckoutFailed {
                operation: "reading archive entry path",
                detail: error.to_string(),
            })?;
        let relative = strip_archive_prefix(&path)?;
        if relative.components().next().is_none() {
            continue;
        }
        let destination = target.join(relative);
        if destination.starts_with(target.join(".git")) {
            continue;
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| CloneError::ArchiveCheckoutFailed {
                operation: "creating archive checkout directory",
                detail: error.to_string(),
            })?;
        }
        entry
            .unpack(&destination)
            .map_err(|error| CloneError::ArchiveCheckoutFailed {
                operation: "unpacking archive entry",
                detail: format!("{}: {error}", destination.display()),
            })?;
    }

    Ok(())
}

fn github_archive_url(raw_url: &str, commit: &str) -> Result<String, CloneError> {
    let url = Url::parse(raw_url).map_err(|error| CloneError::ArchiveCheckoutFailed {
        operation: "parsing GitHub URL",
        detail: error.to_string(),
    })?;
    if url.host_str() != Some("github.com") {
        return Err(CloneError::ArchiveCheckoutFailed {
            operation: "building GitHub archive URL",
            detail: "archive checkout currently supports github.com HTTPS URLs only".to_owned(),
        });
    }
    let mut segments = url
        .path_segments()
        .ok_or_else(|| CloneError::ArchiveCheckoutFailed {
            operation: "building GitHub archive URL",
            detail: "URL path is not valid".to_owned(),
        })?;
    let owner = segments
        .next()
        .ok_or_else(|| CloneError::ArchiveCheckoutFailed {
            operation: "building GitHub archive URL",
            detail: "URL is missing owner".to_owned(),
        })?;
    let repo = segments
        .next()
        .ok_or_else(|| CloneError::ArchiveCheckoutFailed {
            operation: "building GitHub archive URL",
            detail: "URL is missing repository".to_owned(),
        })?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    Ok(format!(
        "https://codeload.github.com/{owner}/{repo}/tar.gz/{commit}"
    ))
}

fn strip_archive_prefix(path: &Path) -> Result<PathBuf, CloneError> {
    let mut components = path.components();
    let _prefix = components.next();
    let mut out = PathBuf::new();
    for component in components {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CloneError::ArchiveCheckoutFailed {
                    operation: "validating archive path",
                    detail: format!("unsafe archive path `{}`", path.display()),
                });
            }
        }
    }
    Ok(out)
}
