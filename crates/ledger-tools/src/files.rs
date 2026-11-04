//! A file writer confined to a root directory.

use std::io;
use std::path::{Component, Path, PathBuf};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WriteFileRequest {
    /// Path relative to the server's root directory.
    pub path: String,
    /// Full new contents of the file.
    pub content: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadFileRequest {
    /// Path relative to the server's root directory.
    pub path: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WriteFileResponse {
    pub path: String,
    pub bytes_written: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ReadFileResponse {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct FileServer {
    root: PathBuf,
    tool_router: ToolRouter<Self>,
}

impl FileServer {
    /// `root` must already exist.
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            root: root.as_ref().canonicalize()?,
            tool_router: Self::tool_router(),
        })
    }

    /// Maps a caller-supplied path into the root, refusing anything that would leave
    /// it, whether by `..`, an absolute path, or a symlink already inside the root.
    async fn resolve(&self, relative: &str) -> Result<PathBuf, String> {
        let path = Path::new(relative);
        let plain = path
            .components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir));
        if relative.is_empty() || !plain {
            return Err(format!("path must stay inside the root: {relative:?}"));
        }

        let target = self.root.join(path);
        let mut existing = target.as_path();
        while !tokio::fs::try_exists(existing).await.unwrap_or(false) {
            existing = existing
                .parent()
                .ok_or_else(|| format!("no existing parent for {relative:?}"))?;
        }
        let real = tokio::fs::canonicalize(existing)
            .await
            .map_err(|e| format!("cannot resolve {relative:?}: {e}"))?;
        if !real.starts_with(&self.root) {
            return Err(format!("path must stay inside the root: {relative:?}"));
        }
        Ok(target)
    }
}

/// Replaces `target` in one step, so a crash leaves the old file or the new one but
/// never a partial write. A symlink at `target` is replaced, not followed.
async fn write_atomically(target: &Path, content: &[u8]) -> io::Result<()> {
    let parent = target.parent().expect("resolved paths have a parent");
    tokio::fs::create_dir_all(parent).await?;

    let temp = parent.join(format!(".{}.tmp", Uuid::new_v4()));
    let write = async {
        let mut file = tokio::fs::File::create(&temp).await?;
        file.write_all(content).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp, target).await
    };
    let result = write.await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp).await;
    }
    result
}

#[tool_router]
impl FileServer {
    /// Create or overwrite a file. Writing the same content again changes nothing.
    #[tool(annotations(
        title = "Write file",
        read_only_hint = false,
        destructive_hint = true,
        idempotent_hint = true,
        open_world_hint = false
    ))]
    async fn write_file(
        &self,
        Parameters(request): Parameters<WriteFileRequest>,
    ) -> Result<Json<WriteFileResponse>, String> {
        let target = self.resolve(&request.path).await?;
        write_atomically(&target, request.content.as_bytes())
            .await
            .map_err(|e| format!("could not write {:?}: {e}", request.path))?;
        Ok(Json(WriteFileResponse {
            bytes_written: request.content.len() as u64,
            path: request.path,
        }))
    }

    /// Read a UTF-8 text file.
    #[tool(annotations(title = "Read file", read_only_hint = true, open_world_hint = false))]
    async fn read_file(
        &self,
        Parameters(request): Parameters<ReadFileRequest>,
    ) -> Result<Json<ReadFileResponse>, String> {
        let target = self.resolve(&request.path).await?;
        let content = tokio::fs::read_to_string(&target)
            .await
            .map_err(|e| format!("could not read {:?}: {e}", request.path))?;
        Ok(Json(ReadFileResponse {
            path: request.path,
            content,
        }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FileServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("ledger-files", env!("CARGO_PKG_VERSION")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &str, content: &str) -> Parameters<WriteFileRequest> {
        Parameters(WriteFileRequest {
            path: path.to_owned(),
            content: content.to_owned(),
        })
    }

    fn read(path: &str) -> Parameters<ReadFileRequest> {
        Parameters(ReadFileRequest {
            path: path.to_owned(),
        })
    }

    #[tokio::test]
    async fn writes_then_reads_back_creating_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let server = FileServer::new(dir.path()).unwrap();

        let Json(written) = server
            .write_file(write("a/b/c.txt", "héllo"))
            .await
            .unwrap();
        assert_eq!(written.bytes_written, "héllo".len() as u64);

        let Json(contents) = server.read_file(read("a/b/c.txt")).await.unwrap();
        assert_eq!(contents.content, "héllo");
    }

    #[tokio::test]
    async fn writing_twice_keeps_the_latest_and_leaves_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let server = FileServer::new(dir.path()).unwrap();

        server.write_file(write("f.txt", "one")).await.unwrap();
        server.write_file(write("f.txt", "two")).await.unwrap();

        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec!["f.txt"]);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("f.txt")).unwrap(),
            "two"
        );
    }

    #[tokio::test]
    async fn paths_cannot_leave_the_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let server = FileServer::new(&root).unwrap();

        let outside = parent.path().join("outside.txt");
        let absolute = outside.to_str().unwrap().to_owned();
        for path in [
            "../outside.txt",
            "a/../../outside.txt",
            absolute.as_str(),
            "",
        ] {
            assert!(server.write_file(write(path, "x")).await.is_err(), "{path}");
            assert!(server.read_file(read(path)).await.is_err(), "{path}");
        }
        assert!(!outside.exists());
    }

    #[tokio::test]
    async fn symlinks_cannot_leave_the_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("root");
        let elsewhere = parent.path().join("elsewhere");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&elsewhere).unwrap();
        let secret = elsewhere.join("secret.txt");
        std::fs::write(&secret, "secret").unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("dir")).unwrap();
        std::os::unix::fs::symlink(&secret, root.join("link.txt")).unwrap();
        let server = FileServer::new(&root).unwrap();

        assert!(server.read_file(read("dir/secret.txt")).await.is_err());
        assert!(server.read_file(read("link.txt")).await.is_err());
        assert!(server.write_file(write("dir/new.txt", "x")).await.is_err());
        assert!(server.write_file(write("link.txt", "x")).await.is_err());

        assert_eq!(std::fs::read_to_string(&secret).unwrap(), "secret");
        assert!(!elsewhere.join("new.txt").exists());
    }

    #[test]
    fn annotations_describe_the_effects() {
        let write = FileServer::write_file_tool_attr().annotations.unwrap();
        assert_eq!(write.read_only_hint, Some(false));
        assert_eq!(write.idempotent_hint, Some(true));

        let read = FileServer::read_file_tool_attr().annotations.unwrap();
        assert_eq!(read.read_only_hint, Some(true));
    }
}
