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
        default_branch: &str,
    ) -> Result<(), CloneError> {
        self.write_config(&remote.url)?;
        self.write_head(default_branch)?;
        self.write_refs(refs, default_branch)
    }

    fn write_config(&self, url: &str) -> Result<(), CloneError> {
        let path = self.root.join(".git/config");
        let content = format!(
            "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n\tlogallrefupdates = true\n[remote \"origin\"]\n\turl = {url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n"
        );
        fs::write(&path, content)
            .map_err(|source| CloneError::repo_layout(path, "writing .git/config", source))
    }

    fn write_head(&self, default_branch: &str) -> Result<(), CloneError> {
        let path = self.root.join(".git/HEAD");
        let content = format!("ref: {default_branch}\n");
        fs::write(&path, content)
            .map_err(|source| CloneError::repo_layout(path, "writing HEAD", source))
    }

    fn write_refs(&self, refs: &[RemoteRef], default_branch: &str) -> Result<(), CloneError> {
        let mut packed_refs = Vec::new();
        for remote_ref in refs {
            if let Some(branch) = remote_ref.name.strip_prefix("refs/heads/") {
                packed_refs.push((
                    format!("refs/remotes/origin/{branch}"),
                    remote_ref.oid.as_str(),
                ));
                if default_branch == remote_ref.name {
                    let path = self.root.join(".git/refs/heads").join(branch);
                    write_ref(&path, &remote_ref.oid)?;
                }
            } else if let Some(tag) = remote_ref.name.strip_prefix("refs/tags/") {
                packed_refs.push((format!("refs/tags/{tag}"), remote_ref.oid.as_str()));
            }
        }
        write_packed_refs(&self.root.join(".git/packed-refs"), packed_refs)?;
        Ok(())
    }
}

fn write_packed_refs(path: &Path, mut refs: Vec<(String, &str)>) -> Result<(), CloneError> {
    refs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut content = String::from("# pack-refs with: sorted\n");
    for (name, oid) in refs {
        content.push_str(oid);
        content.push(' ');
        content.push_str(&name);
        content.push('\n');
    }
    fs::write(path, content)
        .map_err(|source| CloneError::repo_layout(path.to_owned(), "writing packed-refs", source))
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
