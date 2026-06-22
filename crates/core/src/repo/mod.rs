use std::fs;
use std::path::{Path, PathBuf};

use crate::error::CloneError;
use crate::protocol::{Remote, RemoteRef};

#[derive(Debug)]
pub struct RepoLayout {
    root: PathBuf,
}

impl RepoLayout {
    pub fn create(target: &Path) -> Result<Self, CloneError> {
        if target.exists() {
            return Err(CloneError::TargetAlreadyExists {
                path: target.to_owned(),
            });
        }

        fs::create_dir(target).map_err(|source| {
            CloneError::repo_layout(target.to_owned(), "creating target directory", source)
        })?;

        let git_dir = target.join(".git");
        fs::create_dir(&git_dir).map_err(|source| {
            CloneError::repo_layout(git_dir.clone(), "creating .git directory", source)
        })?;

        for path in [
            git_dir.join("objects"),
            git_dir.join("objects/pack"),
            git_dir.join("refs"),
            git_dir.join("refs/heads"),
            git_dir.join("refs/remotes"),
            git_dir.join("refs/remotes/origin"),
            git_dir.join("refs/tags"),
        ] {
            fs::create_dir(&path).map_err(|source| {
                CloneError::repo_layout(path, "creating repository subdirectory", source)
            })?;
        }

        Ok(Self {
            root: target.to_owned(),
        })
    }

    pub fn pack_temp_path(&self) -> PathBuf {
        self.root.join(".git/objects/pack/fcl.pack")
    }

    pub fn pack_index_temp_path(&self) -> PathBuf {
        self.root.join(".git/objects/pack/fcl.idx")
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn git_index_path(&self) -> PathBuf {
        self.root.join(".git/index")
    }

    pub fn write_initial_metadata(
        &self,
        remote: &Remote,
        refs: &[RemoteRef],
    ) -> Result<(), CloneError> {
        self.write_config(&remote.url)?;
        self.write_head(remote.refs.default_branch.as_deref())?;
        self.write_refs(refs, remote.refs.default_branch.as_deref())
    }

    fn write_config(&self, url: &str) -> Result<(), CloneError> {
        let path = self.root.join(".git/config");
        let content = format!(
            "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n\tlogallrefupdates = true\n[remote \"origin\"]\n\turl = {url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n"
        );
        fs::write(&path, content)
            .map_err(|source| CloneError::repo_layout(path, "writing .git/config", source))
    }

    fn write_head(&self, default_branch: Option<&str>) -> Result<(), CloneError> {
        let path = self.root.join(".git/HEAD");
        let head = default_branch.unwrap_or("refs/heads/main");
        let content = format!("ref: {head}\n");
        fs::write(&path, content)
            .map_err(|source| CloneError::repo_layout(path, "writing HEAD", source))
    }

    fn write_refs(
        &self,
        refs: &[RemoteRef],
        default_branch: Option<&str>,
    ) -> Result<(), CloneError> {
        for remote_ref in refs {
            if let Some(branch) = remote_ref.name.strip_prefix("refs/heads/") {
                let path = self.root.join(".git/refs/remotes/origin").join(branch);
                write_ref(&path, &remote_ref.oid)?;
                if default_branch == Some(remote_ref.name.as_str()) {
                    let path = self.root.join(".git/refs/heads").join(branch);
                    write_ref(&path, &remote_ref.oid)?;
                }
            } else if let Some(tag) = remote_ref.name.strip_prefix("refs/tags/") {
                let path = self.root.join(".git/refs/tags").join(tag);
                write_ref(&path, &remote_ref.oid)?;
            }
        }
        Ok(())
    }
}

fn write_ref(path: &Path, oid: &str) -> Result<(), CloneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            CloneError::repo_layout(parent.to_owned(), "creating ref parent directory", source)
        })?;
    }
    fs::write(path, format!("{oid}\n"))
        .map_err(|source| CloneError::repo_layout(path.to_owned(), "writing ref", source))
}
