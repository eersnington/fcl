use crate::error::CloneError;
use crate::pack::ObjectId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeEntryMode {
    File,
    Executable,
    Symlink,
    Directory,
    Gitlink,
}

impl TreeEntryMode {
    pub const fn index_mode(self) -> u32 {
        match self {
            Self::File => 0o100_644,
            Self::Executable => 0o100_755,
            Self::Symlink => 0o120_000,
            Self::Directory => 0o040_000,
            Self::Gitlink => 0o160_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedTreeEntry {
    pub mode: TreeEntryMode,
    pub name: String,
    pub oid: ObjectId,
}

pub fn parse_commit_tree_oid(commit_oid: ObjectId, data: &[u8]) -> Result<ObjectId, CloneError> {
    let text = std::str::from_utf8(data).map_err(|error| CloneError::ObjectParseFailed {
        oid: commit_oid.to_hex(),
        object_type: "commit",
        operation: "reading commit as UTF-8",
        detail: error.to_string(),
    })?;
    let tree = text
        .lines()
        .find_map(|line| line.strip_prefix("tree "))
        .ok_or_else(|| CloneError::ObjectParseFailed {
            oid: commit_oid.to_hex(),
            object_type: "commit",
            operation: "finding tree header",
            detail: "commit did not contain a tree header".to_owned(),
        })?;
    ObjectId::parse_hex(tree)
}

pub fn parse_tree_entries(
    tree_oid: ObjectId,
    data: &[u8],
) -> Result<Vec<ParsedTreeEntry>, CloneError> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        let mode_start = cursor;
        while cursor < data.len() && data[cursor] != b' ' {
            cursor += 1;
        }
        if cursor == data.len() {
            return tree_parse_error(tree_oid, "tree entry mode was not terminated by a space");
        }
        let mode = std::str::from_utf8(&data[mode_start..cursor]).map_err(|error| {
            CloneError::ObjectParseFailed {
                oid: tree_oid.to_hex(),
                object_type: "tree",
                operation: "parsing entry mode",
                detail: error.to_string(),
            }
        })?;
        cursor += 1;

        let name_start = cursor;
        while cursor < data.len() && data[cursor] != 0 {
            cursor += 1;
        }
        if cursor == data.len() {
            return tree_parse_error(tree_oid, "tree entry name was not NUL terminated");
        }
        let name = std::str::from_utf8(&data[name_start..cursor]).map_err(|error| {
            CloneError::ObjectParseFailed {
                oid: tree_oid.to_hex(),
                object_type: "tree",
                operation: "parsing entry name",
                detail: error.to_string(),
            }
        })?;
        cursor += 1;
        if data.len() - cursor < 20 {
            return tree_parse_error(tree_oid, "tree entry object id was truncated");
        }
        let mut oid = [0u8; 20];
        oid.copy_from_slice(&data[cursor..cursor + 20]);
        cursor += 20;

        entries.push(ParsedTreeEntry {
            mode: parse_tree_mode(tree_oid, mode)?,
            name: name.to_owned(),
            oid: ObjectId::from_bytes(oid),
        });
    }
    Ok(entries)
}

fn parse_tree_mode(tree_oid: ObjectId, mode: &str) -> Result<TreeEntryMode, CloneError> {
    match mode {
        "100644" => Ok(TreeEntryMode::File),
        "100755" => Ok(TreeEntryMode::Executable),
        "120000" => Ok(TreeEntryMode::Symlink),
        "040000" | "40000" => Ok(TreeEntryMode::Directory),
        "160000" => Ok(TreeEntryMode::Gitlink),
        other => Err(CloneError::ObjectParseFailed {
            oid: tree_oid.to_hex(),
            object_type: "tree",
            operation: "parsing entry mode",
            detail: format!("unsupported tree mode `{other}`"),
        }),
    }
}

fn tree_parse_error<T>(tree_oid: ObjectId, detail: &str) -> Result<T, CloneError> {
    Err(CloneError::ObjectParseFailed {
        oid: tree_oid.to_hex(),
        object_type: "tree",
        operation: "parsing tree entry",
        detail: detail.to_owned(),
    })
}
