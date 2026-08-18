use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    #[error("output path already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("failed to publish output: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize output: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("output path has no parent directory: {0}")]
    MissingParent(PathBuf),
}

pub struct OutputOptions {
    pub pretty: bool,
    pub path: Option<PathBuf>,
    pub force: bool,
}

impl OutputOptions {
    pub fn validate(&self) -> Result<(), OutputError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let parent = output_parent(path)?;
        if !parent.is_dir() {
            return Err(OutputError::MissingParent(path.clone()));
        }
        if !self.force && path.exists() {
            return Err(OutputError::AlreadyExists(path.clone()));
        }
        Ok(())
    }

    pub fn write(&self, value: &impl Serialize) -> Result<(), OutputError> {
        let mut bytes = if self.pretty {
            serde_json::to_vec_pretty(value)?
        } else {
            serde_json::to_vec(value)?
        };
        bytes.push(b'\n');
        match &self.path {
            Some(path) => publish(path, &bytes, self.force),
            None => {
                std::io::stdout().lock().write_all(&bytes)?;
                Ok(())
            }
        }
    }
}

fn publish(path: &Path, bytes: &[u8], force: bool) -> Result<(), OutputError> {
    let parent = output_parent(path)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    if force {
        temporary.persist(path).map_err(|error| error.error)?;
    } else {
        temporary.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                OutputError::AlreadyExists(path.to_path_buf())
            } else {
                OutputError::Io(error.error)
            }
        })?;
    }
    sync_parent(parent)?;
    Ok(())
}

fn output_parent(path: &Path) -> Result<&Path, OutputError> {
    match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(parent) => Ok(parent),
        None => Err(OutputError::MissingParent(path.to_path_buf())),
    }
}

fn sync_parent(parent: &Path) -> Result<(), std::io::Error> {
    std::fs::File::open(parent)?.sync_all()
}
