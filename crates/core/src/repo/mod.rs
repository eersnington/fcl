use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::CloneError;
use crate::protocol::{Remote, RemoteRef};

#[derive(Debug)]
pub struct RepoLayout {
    root: PathBuf,
}

#[derive(Debug)]
pub struct FinalizingRepo {
    layout: Option<RepoLayout>,
    final_target: PathBuf,
    committed: bool,
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

impl FinalizingRepo {
    pub fn create(final_target: &Path) -> Result<Self, CloneError> {
        if final_target.exists() {
            return Err(CloneError::TargetAlreadyExists {
                path: final_target.to_owned(),
            });
        }
        let staging = staging_path(final_target);
        let layout = RepoLayout::create(&staging)?;
        Ok(Self {
            layout: Some(layout),
            final_target: final_target.to_owned(),
            committed: false,
        })
    }

    pub fn layout(&self) -> Result<&RepoLayout, CloneError> {
        self.layout.as_ref().ok_or_else(|| {
            CloneError::repo_layout(
                self.final_target.clone(),
                "accessing staged repository layout",
                std::io::Error::other("staged repository was already committed"),
            )
        })
    }

    pub fn commit(mut self) -> Result<RepoLayout, CloneError> {
        let layout = self.layout.take().ok_or_else(|| {
            CloneError::repo_layout(
                self.final_target.clone(),
                "publishing staged repository",
                std::io::Error::other("staged repository was already committed"),
            )
        })?;
        fs::rename(layout.root(), &self.final_target).map_err(|source| {
            CloneError::repo_layout(
                self.final_target.clone(),
                "publishing staged repository",
                source,
            )
        })?;
        self.committed = true;
        Ok(RepoLayout {
            root: self.final_target.clone(),
        })
    }
}

impl Drop for FinalizingRepo {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(layout) = &self.layout {
            let _ = fs::remove_dir_all(layout.root());
        }
    }
}

fn staging_path(final_target: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let name = final_target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("fcl-target");
    let staging_name = format!(".{name}.fcl-staging-{}-{stamp}", std::process::id());
    final_target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(staging_name)
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::FinalizingRepo;

    #[test]
    fn staged_repo_should_publish_only_on_commit() {
        let temp = test_temp_dir("staged-publish");
        let target = temp.join("repo");
        let staged = FinalizingRepo::create(&target).expect("staged repo should be created");
        let staging_root = staged
            .layout()
            .expect("layout should exist")
            .root()
            .to_owned();

        assert!(!target.exists());
        assert!(staging_root.exists());

        let published = staged.commit().expect("staged repo should publish");

        assert_eq!(published.root(), target.as_path());
        assert!(target.join(".git").exists());
        assert!(!staging_root.exists());
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn staged_repo_should_cleanup_after_failure() {
        let temp = test_temp_dir("staged-cleanup");
        let target = temp.join("repo");
        let staging_root = {
            let staged = FinalizingRepo::create(&target).expect("staged repo should be created");
            staged
                .layout()
                .expect("layout should exist")
                .root()
                .to_owned()
        };

        assert!(!target.exists());
        assert!(!staging_root.exists());
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    fn test_temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("fcl-{name}-{}-{stamp}", std::process::id()));
        fs::create_dir(&path).expect("test temp directory should be created");
        path
    }
}
