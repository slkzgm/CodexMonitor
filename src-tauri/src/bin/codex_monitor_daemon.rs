#[path = "../backend/mod.rs"]
mod backend;
#[path = "../storage.rs"]
mod storage;
#[path = "../types.rs"]
mod types;

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use git2::{BranchType, DiffOptions, Repository, Sort, Status, StatusOptions, Tree};
use ignore::WalkBuilder;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{broadcast, mpsc, Mutex};
use uuid::Uuid;

use backend::app_server::{spawn_workspace_session, WorkspaceSession};
use backend::events::{AppServerEvent, EventSink, TerminalOutput};
use storage::{read_settings, read_workspaces, write_settings, write_workspaces};
use types::{
    AppSettings, BranchInfo, GitFileDiff, GitFileStatus, GitHubIssue, GitHubIssuesResponse,
    GitLogEntry, GitLogResponse, WorkspaceEntry, WorkspaceInfo, WorkspaceKind, WorkspaceSettings,
    WorktreeInfo,
};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:4732";

#[derive(Clone)]
struct DaemonEventSink {
    tx: broadcast::Sender<DaemonEvent>,
}

#[derive(Clone)]
enum DaemonEvent {
    AppServer(AppServerEvent),
    TerminalOutput(TerminalOutput),
}

impl EventSink for DaemonEventSink {
    fn emit_app_server_event(&self, event: AppServerEvent) {
        let _ = self.tx.send(DaemonEvent::AppServer(event));
    }

    fn emit_terminal_output(&self, event: TerminalOutput) {
        let _ = self.tx.send(DaemonEvent::TerminalOutput(event));
    }
}

struct DaemonConfig {
    listen: SocketAddr,
    token: Option<String>,
    data_dir: PathBuf,
}

struct DaemonState {
    workspaces: Mutex<HashMap<String, WorkspaceEntry>>,
    sessions: Mutex<HashMap<String, Arc<WorkspaceSession>>>,
    storage_path: PathBuf,
    settings_path: PathBuf,
    app_settings: Mutex<AppSettings>,
    event_sink: DaemonEventSink,
}

impl DaemonState {
    fn load(config: &DaemonConfig, event_sink: DaemonEventSink) -> Self {
        let storage_path = config.data_dir.join("workspaces.json");
        let settings_path = config.data_dir.join("settings.json");
        let workspaces = read_workspaces(&storage_path).unwrap_or_default();
        let app_settings = read_settings(&settings_path).unwrap_or_default();
        Self {
            workspaces: Mutex::new(workspaces),
            sessions: Mutex::new(HashMap::new()),
            storage_path,
            settings_path,
            app_settings: Mutex::new(app_settings),
            event_sink,
        }
    }

    async fn list_workspaces(&self) -> Vec<WorkspaceInfo> {
        let workspaces = self.workspaces.lock().await;
        let sessions = self.sessions.lock().await;
        let mut result = Vec::new();
        for entry in workspaces.values() {
            result.push(WorkspaceInfo {
                id: entry.id.clone(),
                name: entry.name.clone(),
                path: entry.path.clone(),
                connected: sessions.contains_key(&entry.id),
                codex_bin: entry.codex_bin.clone(),
                kind: entry.kind.clone(),
                parent_id: entry.parent_id.clone(),
                worktree: entry.worktree.clone(),
                settings: entry.settings.clone(),
            });
        }
        sort_workspaces(&mut result);
        result
    }

    async fn add_workspace(
        &self,
        path: String,
        codex_bin: Option<String>,
        client_version: String,
    ) -> Result<WorkspaceInfo, String> {
        let name = PathBuf::from(&path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("Workspace")
            .to_string();

        let entry = WorkspaceEntry {
            id: Uuid::new_v4().to_string(),
            name: name.clone(),
            path: path.clone(),
            codex_bin,
            kind: WorkspaceKind::Main,
            parent_id: None,
            worktree: None,
            settings: WorkspaceSettings::default(),
        };

        let default_bin = {
            let settings = self.app_settings.lock().await;
            settings.codex_bin.clone()
        };

        let session = spawn_workspace_session(
            entry.clone(),
            default_bin,
            client_version,
            self.event_sink.clone(),
        )
        .await?;

        let list = {
            let mut workspaces = self.workspaces.lock().await;
            workspaces.insert(entry.id.clone(), entry.clone());
            workspaces.values().cloned().collect::<Vec<_>>()
        };
        write_workspaces(&self.storage_path, &list)?;

        self.sessions.lock().await.insert(entry.id.clone(), session);

        Ok(WorkspaceInfo {
            id: entry.id,
            name: entry.name,
            path: entry.path,
            connected: true,
            codex_bin: entry.codex_bin,
            kind: entry.kind,
            parent_id: entry.parent_id,
            worktree: entry.worktree,
            settings: entry.settings,
        })
    }

    async fn add_worktree(
        &self,
        parent_id: String,
        branch: String,
        client_version: String,
    ) -> Result<WorkspaceInfo, String> {
        let branch = branch.trim().to_string();
        if branch.trim().is_empty() {
            return Err("Branch name is required.".to_string());
        }

        let parent_entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&parent_id)
                .cloned()
                .ok_or("parent workspace not found")?
        };

        if parent_entry.kind.is_worktree() {
            return Err("Cannot create a worktree from another worktree.".to_string());
        }

        let worktree_root = PathBuf::from(&parent_entry.path).join(".codex-worktrees");
        std::fs::create_dir_all(&worktree_root)
            .map_err(|e| format!("Failed to create worktree directory: {e}"))?;
        ensure_worktree_ignored(&PathBuf::from(&parent_entry.path))?;

        let safe_name = sanitize_worktree_name(&branch);
        let worktree_path = unique_worktree_path(&worktree_root, &safe_name);
        let worktree_path_string = worktree_path.to_string_lossy().to_string();

        let branch_exists = git_branch_exists(&PathBuf::from(&parent_entry.path), &branch).await?;
        if branch_exists {
            run_git_command(
                &PathBuf::from(&parent_entry.path),
                &["worktree", "add", &worktree_path_string, &branch],
            )
            .await?;
        } else {
            run_git_command(
                &PathBuf::from(&parent_entry.path),
                &["worktree", "add", "-b", &branch, &worktree_path_string],
            )
            .await?;
        }

        let entry = WorkspaceEntry {
            id: Uuid::new_v4().to_string(),
            name: branch.to_string(),
            path: worktree_path_string,
            codex_bin: parent_entry.codex_bin.clone(),
            kind: WorkspaceKind::Worktree,
            parent_id: Some(parent_entry.id.clone()),
            worktree: Some(WorktreeInfo {
                branch: branch.to_string(),
            }),
            settings: WorkspaceSettings::default(),
        };

        let default_bin = {
            let settings = self.app_settings.lock().await;
            settings.codex_bin.clone()
        };

        let session = spawn_workspace_session(
            entry.clone(),
            default_bin,
            client_version,
            self.event_sink.clone(),
        )
        .await?;

        let list = {
            let mut workspaces = self.workspaces.lock().await;
            workspaces.insert(entry.id.clone(), entry.clone());
            workspaces.values().cloned().collect::<Vec<_>>()
        };
        write_workspaces(&self.storage_path, &list)?;

        self.sessions.lock().await.insert(entry.id.clone(), session);

        Ok(WorkspaceInfo {
            id: entry.id,
            name: entry.name,
            path: entry.path,
            connected: true,
            codex_bin: entry.codex_bin,
            kind: entry.kind,
            parent_id: entry.parent_id,
            worktree: entry.worktree,
            settings: entry.settings,
        })
    }

    async fn remove_workspace(&self, id: String) -> Result<(), String> {
        let (entry, child_worktrees) = {
            let workspaces = self.workspaces.lock().await;
            let entry = workspaces.get(&id).cloned().ok_or("workspace not found")?;
            if entry.kind.is_worktree() {
                return Err("Use remove_worktree for worktree agents.".to_string());
            }
            let children = workspaces
                .values()
                .filter(|workspace| workspace.parent_id.as_deref() == Some(&id))
                .cloned()
                .collect::<Vec<_>>();
            (entry, children)
        };

        let parent_path = PathBuf::from(&entry.path);
        for child in &child_worktrees {
            if let Some(session) = self.sessions.lock().await.remove(&child.id) {
                let mut child_process = session.child.lock().await;
                let _ = child_process.kill().await;
            }
            let child_path = PathBuf::from(&child.path);
            if child_path.exists() {
                run_git_command(
                    &parent_path,
                    &["worktree", "remove", "--force", &child.path],
                )
                .await?;
            }
        }
        let _ = run_git_command(&parent_path, &["worktree", "prune", "--expire", "now"]).await;

        if let Some(session) = self.sessions.lock().await.remove(&id) {
            let mut child = session.child.lock().await;
            let _ = child.kill().await;
        }

        {
            let mut workspaces = self.workspaces.lock().await;
            workspaces.remove(&id);
            for child in child_worktrees {
                workspaces.remove(&child.id);
            }
            let list: Vec<_> = workspaces.values().cloned().collect();
            write_workspaces(&self.storage_path, &list)?;
        }

        Ok(())
    }

    async fn remove_worktree(&self, id: String) -> Result<(), String> {
        let (entry, parent) = {
            let workspaces = self.workspaces.lock().await;
            let entry = workspaces.get(&id).cloned().ok_or("workspace not found")?;
            if !entry.kind.is_worktree() {
                return Err("Not a worktree workspace.".to_string());
            }
            let parent_id = entry.parent_id.clone().ok_or("worktree parent not found")?;
            let parent = workspaces
                .get(&parent_id)
                .cloned()
                .ok_or("worktree parent not found")?;
            (entry, parent)
        };

        if let Some(session) = self.sessions.lock().await.remove(&entry.id) {
            let mut child = session.child.lock().await;
            let _ = child.kill().await;
        }

        let parent_path = PathBuf::from(&parent.path);
        let entry_path = PathBuf::from(&entry.path);
        if entry_path.exists() {
            run_git_command(
                &parent_path,
                &["worktree", "remove", "--force", &entry.path],
            )
            .await?;
        }
        let _ = run_git_command(&parent_path, &["worktree", "prune", "--expire", "now"]).await;

        {
            let mut workspaces = self.workspaces.lock().await;
            workspaces.remove(&entry.id);
            let list: Vec<_> = workspaces.values().cloned().collect();
            write_workspaces(&self.storage_path, &list)?;
        }

        Ok(())
    }

    async fn update_workspace_settings(
        &self,
        id: String,
        settings: WorkspaceSettings,
    ) -> Result<WorkspaceInfo, String> {
        let (entry_snapshot, list) = {
            let mut workspaces = self.workspaces.lock().await;
            let entry_snapshot = match workspaces.get_mut(&id) {
                Some(entry) => {
                    entry.settings = settings.clone();
                    entry.clone()
                }
                None => return Err("workspace not found".to_string()),
            };
            let list: Vec<_> = workspaces.values().cloned().collect();
            (entry_snapshot, list)
        };
        write_workspaces(&self.storage_path, &list)?;

        let connected = self.sessions.lock().await.contains_key(&id);
        Ok(WorkspaceInfo {
            id: entry_snapshot.id,
            name: entry_snapshot.name,
            path: entry_snapshot.path,
            connected,
            codex_bin: entry_snapshot.codex_bin,
            kind: entry_snapshot.kind,
            parent_id: entry_snapshot.parent_id,
            worktree: entry_snapshot.worktree,
            settings: entry_snapshot.settings,
        })
    }

    async fn update_workspace_codex_bin(
        &self,
        id: String,
        codex_bin: Option<String>,
    ) -> Result<WorkspaceInfo, String> {
        let (entry_snapshot, list) = {
            let mut workspaces = self.workspaces.lock().await;
            let entry_snapshot = match workspaces.get_mut(&id) {
                Some(entry) => {
                    entry.codex_bin = codex_bin.clone();
                    entry.clone()
                }
                None => return Err("workspace not found".to_string()),
            };
            let list: Vec<_> = workspaces.values().cloned().collect();
            (entry_snapshot, list)
        };
        write_workspaces(&self.storage_path, &list)?;

        let connected = self.sessions.lock().await.contains_key(&id);
        Ok(WorkspaceInfo {
            id: entry_snapshot.id,
            name: entry_snapshot.name,
            path: entry_snapshot.path,
            connected,
            codex_bin: entry_snapshot.codex_bin,
            kind: entry_snapshot.kind,
            parent_id: entry_snapshot.parent_id,
            worktree: entry_snapshot.worktree,
            settings: entry_snapshot.settings,
        })
    }

    async fn connect_workspace(&self, id: String, client_version: String) -> Result<(), String> {
        {
            let sessions = self.sessions.lock().await;
            if sessions.contains_key(&id) {
                return Ok(());
            }
        }

        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&id)
                .cloned()
                .ok_or("workspace not found")?
        };

        let default_bin = {
            let settings = self.app_settings.lock().await;
            settings.codex_bin.clone()
        };

        let session = spawn_workspace_session(
            entry,
            default_bin,
            client_version,
            self.event_sink.clone(),
        )
        .await?;

        self.sessions.lock().await.insert(id, session);
        Ok(())
    }

    async fn update_app_settings(&self, settings: AppSettings) -> Result<AppSettings, String> {
        write_settings(&self.settings_path, &settings)?;
        let mut current = self.app_settings.lock().await;
        *current = settings.clone();
        Ok(settings)
    }

    async fn get_session(&self, workspace_id: &str) -> Result<Arc<WorkspaceSession>, String> {
        let sessions = self.sessions.lock().await;
        sessions
            .get(workspace_id)
            .cloned()
            .ok_or("workspace not connected".to_string())
    }

    async fn list_workspace_files(&self, workspace_id: String) -> Result<Vec<String>, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };

        let root = PathBuf::from(entry.path);
        Ok(list_workspace_files_inner(&root, 20000))
    }

    async fn start_thread(&self, workspace_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({
            "cwd": session.entry.path,
            "approvalPolicy": "on-request"
        });
        session.send_request("thread/start", params).await
    }

    async fn resume_thread(&self, workspace_id: String, thread_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({
            "threadId": thread_id
        });
        session.send_request("thread/resume", params).await
    }

    async fn list_threads(
        &self,
        workspace_id: String,
        cursor: Option<String>,
        limit: Option<u32>,
    ) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({
            "cursor": cursor,
            "limit": limit
        });
        session.send_request("thread/list", params).await
    }

    async fn archive_thread(&self, workspace_id: String, thread_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({ "threadId": thread_id });
        session.send_request("thread/archive", params).await
    }

    async fn send_user_message(
        &self,
        workspace_id: String,
        thread_id: String,
        text: String,
        model: Option<String>,
        effort: Option<String>,
        access_mode: Option<String>,
        images: Option<Vec<String>>,
    ) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let access_mode = access_mode.unwrap_or_else(|| "current".to_string());
        let sandbox_policy = match access_mode.as_str() {
            "full-access" => json!({
                "type": "dangerFullAccess"
            }),
            "read-only" => json!({
                "type": "readOnly"
            }),
            _ => json!({
                "type": "workspaceWrite",
                "writableRoots": [session.entry.path],
                "networkAccess": true
            }),
        };

        let approval_policy = if access_mode == "full-access" {
            "never"
        } else {
            "on-request"
        };

        let trimmed_text = text.trim();
        let mut input: Vec<Value> = Vec::new();
        if !trimmed_text.is_empty() {
            input.push(json!({ "type": "text", "text": trimmed_text }));
        }
        if let Some(paths) = images {
            for path in paths {
                let trimmed = path.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.starts_with("data:")
                    || trimmed.starts_with("http://")
                    || trimmed.starts_with("https://")
                {
                    input.push(json!({ "type": "image", "url": trimmed }));
                } else {
                    input.push(json!({ "type": "localImage", "path": trimmed }));
                }
            }
        }
        if input.is_empty() {
            return Err("empty user message".to_string());
        }

        let params = json!({
            "threadId": thread_id,
            "input": input,
            "cwd": session.entry.path,
            "approvalPolicy": approval_policy,
            "sandboxPolicy": sandbox_policy,
            "model": model,
            "effort": effort,
        });
        session.send_request("turn/start", params).await
    }

    async fn turn_interrupt(
        &self,
        workspace_id: String,
        thread_id: String,
        turn_id: String,
    ) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({
            "threadId": thread_id,
            "turnId": turn_id
        });
        session.send_request("turn/interrupt", params).await
    }

    async fn start_review(
        &self,
        workspace_id: String,
        thread_id: String,
        target: Value,
        delivery: Option<String>,
    ) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let mut params = Map::new();
        params.insert("threadId".to_string(), json!(thread_id));
        params.insert("target".to_string(), target);
        if let Some(delivery) = delivery {
            params.insert("delivery".to_string(), json!(delivery));
        }
        session
            .send_request("review/start", Value::Object(params))
            .await
    }

    async fn model_list(&self, workspace_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        session.send_request("model/list", json!({})).await
    }

    async fn account_rate_limits(&self, workspace_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        session
            .send_request("account/rateLimits/read", Value::Null)
            .await
    }

    async fn skills_list(&self, workspace_id: String) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        let params = json!({
            "cwd": session.entry.path
        });
        session.send_request("skills/list", params).await
    }

    async fn respond_to_server_request(
        &self,
        workspace_id: String,
        request_id: u64,
        result: Value,
    ) -> Result<Value, String> {
        let session = self.get_session(&workspace_id).await?;
        session.send_response(request_id, result).await?;
        Ok(json!({ "ok": true }))
    }

    async fn get_git_status(&self, workspace_id: String) -> Result<Value, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || git_status_for_path(&entry.path))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn get_git_diffs(&self, workspace_id: String) -> Result<Vec<GitFileDiff>, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || git_diffs_for_path(&entry.path))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn get_git_log(
        &self,
        workspace_id: String,
        limit: Option<usize>,
    ) -> Result<GitLogResponse, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || git_log_for_path(&entry.path, limit))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn get_git_remote(&self, workspace_id: String) -> Result<Option<String>, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || git_remote_for_path(&entry.path))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn get_github_issues(&self, workspace_id: String) -> Result<GitHubIssuesResponse, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        let repo_name = tokio::task::spawn_blocking({
            let path = entry.path.clone();
            move || github_repo_name_for_path(&path)
        })
        .await
        .map_err(|err| err.to_string())??;

        let output = Command::new("gh")
            .args([
                "issue",
                "list",
                "--repo",
                &repo_name,
                "--limit",
                "50",
                "--json",
                "number,title,url,updatedAt",
            ])
            .current_dir(&entry.path)
            .output()
            .await
            .map_err(|e| format!("Failed to run gh: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let detail = if stderr.trim().is_empty() {
                stdout.trim()
            } else {
                stderr.trim()
            };
            if detail.is_empty() {
                return Err("GitHub CLI command failed.".to_string());
            }
            return Err(detail.to_string());
        }

        let issues: Vec<GitHubIssue> =
            serde_json::from_slice(&output.stdout).map_err(|e| e.to_string())?;

        let search_query = format!("repo:{repo_name} is:issue is:open").replace(' ', "+");
        let total = match Command::new("gh")
            .args([
                "api",
                &format!("/search/issues?q={search_query}"),
                "--jq",
                ".total_count",
            ])
            .current_dir(&entry.path)
            .output()
            .await
        {
            Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<usize>()
                .unwrap_or(issues.len()),
            _ => issues.len(),
        };

        Ok(GitHubIssuesResponse { total, issues })
    }

    async fn list_git_branches(&self, workspace_id: String) -> Result<Value, String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || list_branches_for_path(&entry.path))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn checkout_git_branch(&self, workspace_id: String, name: String) -> Result<(), String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || checkout_branch_for_path(&entry.path, &name))
            .await
            .map_err(|err| err.to_string())?
    }

    async fn create_git_branch(&self, workspace_id: String, name: String) -> Result<(), String> {
        let entry = {
            let workspaces = self.workspaces.lock().await;
            workspaces
                .get(&workspace_id)
                .cloned()
                .ok_or("workspace not found")?
        };
        tokio::task::spawn_blocking(move || create_branch_for_path(&entry.path, &name))
            .await
            .map_err(|err| err.to_string())?
    }
}

fn sort_workspaces(workspaces: &mut [WorkspaceInfo]) {
    workspaces.sort_by(|a, b| {
        let a_order = a.settings.sort_order.unwrap_or(u32::MAX);
        let b_order = b.settings.sort_order.unwrap_or(u32::MAX);
        if a_order != b_order {
            return a_order.cmp(&b_order);
        }
        a.name.cmp(&b.name)
    });
}

fn should_skip_dir(name: &str) -> bool {
    matches!(
        name,
        ".git" | "node_modules" | "dist" | "target" | "release-artifacts"
    )
}

fn normalize_git_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn list_workspace_files_inner(root: &PathBuf, max_files: usize) -> Vec<String> {
    let mut results = Vec::new();
    let walker = WalkBuilder::new(root)
        .hidden(false)
        .follow_links(false)
        .require_git(false)
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            if entry.file_type().is_some_and(|ft| ft.is_dir()) {
                let name = entry.file_name().to_string_lossy();
                return !should_skip_dir(&name);
            }
            true
        })
        .build();

    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !entry.file_type().is_some_and(|ft| ft.is_file()) {
            continue;
        }
        if let Ok(rel_path) = entry.path().strip_prefix(root) {
            let normalized = normalize_git_path(&rel_path.to_string_lossy());
            if !normalized.is_empty() {
                results.push(normalized);
            }
        }
        if results.len() >= max_files {
            break;
        }
    }

    results.sort();
    results
}

async fn run_git_command(repo_path: &PathBuf, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_path)
        .output()
        .await
        .map_err(|e| format!("Failed to run git: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let detail = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        if detail.is_empty() {
            Err("Git command failed.".to_string())
        } else {
            Err(detail.to_string())
        }
    }
}

async fn git_branch_exists(repo_path: &PathBuf, branch: &str) -> Result<bool, String> {
    let status = Command::new("git")
        .args(["show-ref", "--verify", &format!("refs/heads/{branch}")])
        .current_dir(repo_path)
        .status()
        .await
        .map_err(|e| format!("Failed to run git: {e}"))?;
    Ok(status.success())
}

fn sanitize_worktree_name(branch: &str) -> String {
    let mut result = String::new();
    for ch in branch.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            result.push(ch);
        } else {
            result.push('-');
        }
    }
    let trimmed = result.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "worktree".to_string()
    } else {
        trimmed
    }
}

fn commit_to_entry(commit: git2::Commit) -> GitLogEntry {
    let summary = commit.summary().unwrap_or("").to_string();
    let author = commit.author().name().unwrap_or("").to_string();
    let timestamp = commit.time().seconds();
    GitLogEntry {
        sha: commit.id().to_string(),
        summary,
        author,
        timestamp,
    }
}

fn checkout_branch(repo: &Repository, name: &str) -> Result<(), git2::Error> {
    let refname = format!("refs/heads/{name}");
    repo.set_head(&refname)?;
    let mut options = git2::build::CheckoutBuilder::new();
    options.safe();
    repo.checkout_head(Some(&mut options))?;
    Ok(())
}

fn diff_stats_for_path(
    repo: &Repository,
    head_tree: Option<&Tree>,
    path: &str,
    include_index: bool,
    include_workdir: bool,
) -> Result<(i64, i64), git2::Error> {
    let mut additions = 0i64;
    let mut deletions = 0i64;

    if include_index {
        let mut options = DiffOptions::new();
        options.pathspec(path).include_untracked(true);
        let diff = repo.diff_tree_to_index(head_tree, None, Some(&mut options))?;
        let stats = diff.stats()?;
        additions += stats.insertions() as i64;
        deletions += stats.deletions() as i64;
    }

    if include_workdir {
        let mut options = DiffOptions::new();
        options
            .pathspec(path)
            .include_untracked(true)
            .recurse_untracked_dirs(true)
            .show_untracked_content(true);
        let diff = repo.diff_index_to_workdir(None, Some(&mut options))?;
        let stats = diff.stats()?;
        additions += stats.insertions() as i64;
        deletions += stats.deletions() as i64;
    }

    Ok((additions, deletions))
}

fn diff_patch_to_string(patch: &mut git2::Patch) -> Result<String, git2::Error> {
    let buf = patch.to_buf()?;
    Ok(buf
        .as_str()
        .map(|value| value.to_string())
        .unwrap_or_else(|| String::from_utf8_lossy(&buf).to_string()))
}

fn parse_github_repo(remote_url: &str) -> Option<String> {
    let trimmed = remote_url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut path = if trimmed.starts_with("git@github.com:") {
        trimmed.trim_start_matches("git@github.com:").to_string()
    } else if trimmed.starts_with("ssh://git@github.com/") {
        trimmed.trim_start_matches("ssh://git@github.com/").to_string()
    } else if let Some(index) = trimmed.find("github.com/") {
        trimmed[index + "github.com/".len()..].to_string()
    } else {
        return None;
    };
    path = path.trim_end_matches(".git").trim_end_matches('/').to_string();
    if path.is_empty() { None } else { Some(path) }
}

fn git_status_for_path(path: &str) -> Result<Value, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;

    let branch_name = repo
        .head()
        .ok()
        .and_then(|head| head.shorthand().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let mut status_options = StatusOptions::new();
    status_options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .renames_head_to_index(true)
        .renames_index_to_workdir(true)
        .include_ignored(false);

    let statuses = repo
        .statuses(Some(&mut status_options))
        .map_err(|e| e.to_string())?;

    let head_tree = repo.head().ok().and_then(|head| head.peel_to_tree().ok());

    let mut files = Vec::new();
    let mut total_additions = 0i64;
    let mut total_deletions = 0i64;
    for entry in statuses.iter() {
        let file_path = entry.path().unwrap_or("");
        if file_path.is_empty() {
            continue;
        }
        let status = entry.status();
        let status_str = if status.contains(Status::WT_NEW) || status.contains(Status::INDEX_NEW) {
            "A"
        } else if status.contains(Status::WT_MODIFIED) || status.contains(Status::INDEX_MODIFIED) {
            "M"
        } else if status.contains(Status::WT_DELETED) || status.contains(Status::INDEX_DELETED) {
            "D"
        } else if status.contains(Status::WT_RENAMED) || status.contains(Status::INDEX_RENAMED) {
            "R"
        } else if status.contains(Status::WT_TYPECHANGE) || status.contains(Status::INDEX_TYPECHANGE)
        {
            "T"
        } else {
            "--"
        };
        let normalized_path = normalize_git_path(file_path);
        let include_index = status.intersects(
            Status::INDEX_NEW
                | Status::INDEX_MODIFIED
                | Status::INDEX_DELETED
                | Status::INDEX_RENAMED
                | Status::INDEX_TYPECHANGE,
        );
        let include_workdir = status.intersects(
            Status::WT_NEW
                | Status::WT_MODIFIED
                | Status::WT_DELETED
                | Status::WT_RENAMED
                | Status::WT_TYPECHANGE,
        );
        let (additions, deletions) = diff_stats_for_path(
            &repo,
            head_tree.as_ref(),
            file_path,
            include_index,
            include_workdir,
        )
        .map_err(|e| e.to_string())?;
        total_additions += additions;
        total_deletions += deletions;
        files.push(GitFileStatus {
            path: normalized_path,
            status: status_str.to_string(),
            additions,
            deletions,
        });
    }

    Ok(json!({
        "branchName": branch_name,
        "files": files,
        "totalAdditions": total_additions,
        "totalDeletions": total_deletions,
    }))
}

fn git_diffs_for_path(path: &str) -> Result<Vec<GitFileDiff>, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let head_tree = repo
        .head()
        .ok()
        .and_then(|head| head.peel_to_tree().ok());

    let mut options = DiffOptions::new();
    options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true);

    let diff = match head_tree.as_ref() {
        Some(tree) => repo
            .diff_tree_to_workdir_with_index(Some(tree), Some(&mut options))
            .map_err(|e| e.to_string())?,
        None => repo
            .diff_tree_to_workdir_with_index(None, Some(&mut options))
            .map_err(|e| e.to_string())?,
    };

    let mut results = Vec::new();
    for (index, delta) in diff.deltas().enumerate() {
        let file_path = delta.new_file().path().or_else(|| delta.old_file().path());
        let Some(file_path) = file_path else {
            continue;
        };
        let patch = match git2::Patch::from_diff(&diff, index) {
            Ok(patch) => patch,
            Err(_) => continue,
        };
        let Some(mut patch) = patch else {
            continue;
        };
        let content = match diff_patch_to_string(&mut patch) {
            Ok(content) => content,
            Err(_) => continue,
        };
        if content.trim().is_empty() {
            continue;
        }
        results.push(GitFileDiff {
            path: normalize_git_path(file_path.to_string_lossy().as_ref()),
            diff: content,
        });
    }

    Ok(results)
}

fn git_log_for_path(path: &str, limit: Option<usize>) -> Result<GitLogResponse, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let max_items = limit.unwrap_or(40);
    let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
    revwalk.push_head().map_err(|e| e.to_string())?;
    revwalk.set_sorting(Sort::TIME).map_err(|e| e.to_string())?;

    let mut total = 0usize;
    for oid_result in revwalk {
        oid_result.map_err(|e| e.to_string())?;
        total += 1;
    }

    let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
    revwalk.push_head().map_err(|e| e.to_string())?;
    revwalk.set_sorting(Sort::TIME).map_err(|e| e.to_string())?;

    let mut entries = Vec::new();
    for oid_result in revwalk.take(max_items) {
        let oid = oid_result.map_err(|e| e.to_string())?;
        let commit = repo.find_commit(oid).map_err(|e| e.to_string())?;
        entries.push(commit_to_entry(commit));
    }

    let mut ahead = 0usize;
    let mut behind = 0usize;
    let mut ahead_entries = Vec::new();
    let mut behind_entries = Vec::new();
    let mut upstream = None;

    if let Ok(head) = repo.head() {
        if head.is_branch() {
            if let Some(branch_name) = head.shorthand() {
                if let Ok(branch) = repo.find_branch(branch_name, BranchType::Local) {
                    if let Ok(upstream_branch) = branch.upstream() {
                        let upstream_ref = upstream_branch.get();
                        upstream = upstream_ref
                            .shorthand()
                            .map(|name| name.to_string())
                            .or_else(|| upstream_ref.name().map(|name| name.to_string()));
                        if let (Some(head_oid), Some(upstream_oid)) =
                            (head.target(), upstream_ref.target())
                        {
                            let (ahead_count, behind_count) = repo
                                .graph_ahead_behind(head_oid, upstream_oid)
                                .map_err(|e| e.to_string())?;
                            ahead = ahead_count;
                            behind = behind_count;

                            let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
                            revwalk.push(head_oid).map_err(|e| e.to_string())?;
                            revwalk.hide(upstream_oid).map_err(|e| e.to_string())?;
                            revwalk
                                .set_sorting(Sort::TIME)
                                .map_err(|e| e.to_string())?;
                            for oid_result in revwalk.take(max_items) {
                                let oid = oid_result.map_err(|e| e.to_string())?;
                                let commit = repo.find_commit(oid).map_err(|e| e.to_string())?;
                                ahead_entries.push(commit_to_entry(commit));
                            }

                            let mut revwalk = repo.revwalk().map_err(|e| e.to_string())?;
                            revwalk.push(upstream_oid).map_err(|e| e.to_string())?;
                            revwalk.hide(head_oid).map_err(|e| e.to_string())?;
                            revwalk
                                .set_sorting(Sort::TIME)
                                .map_err(|e| e.to_string())?;
                            for oid_result in revwalk.take(max_items) {
                                let oid = oid_result.map_err(|e| e.to_string())?;
                                let commit = repo.find_commit(oid).map_err(|e| e.to_string())?;
                                behind_entries.push(commit_to_entry(commit));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(GitLogResponse {
        total,
        entries,
        ahead,
        behind,
        ahead_entries,
        behind_entries,
        upstream,
    })
}

fn git_remote_for_path(path: &str) -> Result<Option<String>, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let remotes = repo.remotes().map_err(|e| e.to_string())?;
    let name = if remotes.iter().any(|remote| remote == Some("origin")) {
        "origin".to_string()
    } else {
        remotes
            .iter()
            .flatten()
            .next()
            .unwrap_or("")
            .to_string()
    };
    if name.is_empty() {
        return Ok(None);
    }
    let remote = repo.find_remote(&name).map_err(|e| e.to_string())?;
    Ok(remote.url().map(|url| url.to_string()))
}

fn github_repo_name_for_path(path: &str) -> Result<String, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let remotes = repo.remotes().map_err(|e| e.to_string())?;
    let name = if remotes.iter().any(|remote| remote == Some("origin")) {
        "origin".to_string()
    } else {
        remotes
            .iter()
            .flatten()
            .next()
            .unwrap_or("")
            .to_string()
    };
    if name.is_empty() {
        return Err("No git remote configured.".to_string());
    }
    let remote = repo.find_remote(&name).map_err(|e| e.to_string())?;
    let remote_url = remote.url().ok_or("Remote has no URL configured.")?;
    parse_github_repo(remote_url).ok_or("Remote is not a GitHub repository.".to_string())
}

fn list_branches_for_path(path: &str) -> Result<Value, String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let mut branches = Vec::new();
    let refs = repo
        .branches(Some(BranchType::Local))
        .map_err(|e| e.to_string())?;
    for branch_result in refs {
        let (branch, _) = branch_result.map_err(|e| e.to_string())?;
        let name = branch.name().ok().flatten().unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        let last_commit = branch
            .get()
            .target()
            .and_then(|oid| repo.find_commit(oid).ok())
            .map(|commit| commit.time().seconds())
            .unwrap_or(0);
        branches.push(BranchInfo { name, last_commit });
    }
    branches.sort_by(|a, b| b.last_commit.cmp(&a.last_commit));
    Ok(json!({ "branches": branches }))
}

fn checkout_branch_for_path(path: &str, name: &str) -> Result<(), String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    checkout_branch(&repo, name).map_err(|e| e.to_string())
}

fn create_branch_for_path(path: &str, name: &str) -> Result<(), String> {
    let repo = Repository::open(path).map_err(|e| e.to_string())?;
    let head = repo.head().map_err(|e| e.to_string())?;
    let target = head.peel_to_commit().map_err(|e| e.to_string())?;
    repo.branch(name, &target, false)
        .map_err(|e| e.to_string())?;
    checkout_branch(&repo, name).map_err(|e| e.to_string())
}

fn unique_worktree_path(base_dir: &PathBuf, name: &str) -> PathBuf {
    let mut candidate = base_dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    for index in 2..1000 {
        let next = base_dir.join(format!("{name}-{index}"));
        if !next.exists() {
            candidate = next;
            break;
        }
    }
    candidate
}

fn ensure_worktree_ignored(repo_path: &PathBuf) -> Result<(), String> {
    let ignore_path = repo_path.join(".gitignore");
    let entry = ".codex-worktrees/";
    let existing = std::fs::read_to_string(&ignore_path).unwrap_or_default();
    if existing.lines().any(|line| line.trim() == entry) {
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ignore_path)
        .map_err(|e| format!("Failed to update .gitignore: {e}"))?;
    if !existing.ends_with('\n') && !existing.is_empty() {
        file.write_all(b"\n")
            .map_err(|e| format!("Failed to update .gitignore: {e}"))?;
    }
    file.write_all(format!("{entry}\n").as_bytes())
        .map_err(|e| format!("Failed to update .gitignore: {e}"))?;
    Ok(())
}

fn default_data_dir() -> PathBuf {
    if let Ok(xdg) = env::var("XDG_DATA_HOME") {
        let trimmed = xdg.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed).join("codex-monitor-daemon");
        }
    }
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".local")
        .join("share")
        .join("codex-monitor-daemon")
}

fn usage() -> String {
    format!(
        "\
USAGE:\n  codex-monitor-daemon [--listen <addr>] [--data-dir <path>] [--token <token> | --insecure-no-auth]\n\n\
OPTIONS:\n  --listen <addr>        Bind address (default: {DEFAULT_LISTEN_ADDR})\n  --data-dir <path>      Data dir holding workspaces.json/settings.json\n  --token <token>        Shared token required by clients\n  --insecure-no-auth      Disable auth (dev only)\n  -h, --help             Show this help\n"
    )
}

fn parse_args() -> Result<DaemonConfig, String> {
    let mut listen = DEFAULT_LISTEN_ADDR
        .parse::<SocketAddr>()
        .map_err(|err| err.to_string())?;
    let mut token = env::var("CODEX_MONITOR_DAEMON_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let mut insecure_no_auth = false;
    let mut data_dir: Option<PathBuf> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--listen" => {
                let value = args.next().ok_or("--listen requires a value")?;
                listen = value.parse::<SocketAddr>().map_err(|err| err.to_string())?;
            }
            "--token" => {
                let value = args.next().ok_or("--token requires a value")?;
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err("--token requires a non-empty value".to_string());
                }
                token = Some(trimmed.to_string());
            }
            "--data-dir" => {
                let value = args.next().ok_or("--data-dir requires a value")?;
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err("--data-dir requires a non-empty value".to_string());
                }
                data_dir = Some(PathBuf::from(trimmed));
            }
            "--insecure-no-auth" => {
                insecure_no_auth = true;
                token = None;
            }
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }

    if token.is_none() && !insecure_no_auth {
        return Err(
            "Missing --token (or set CODEX_MONITOR_DAEMON_TOKEN). Use --insecure-no-auth for local dev only."
                .to_string(),
        );
    }

    Ok(DaemonConfig {
        listen,
        token,
        data_dir: data_dir.unwrap_or_else(default_data_dir),
    })
}

fn build_error_response(id: Option<u64>, message: &str) -> Option<String> {
    let id = id?;
    Some(
        serde_json::to_string(&json!({
            "id": id,
            "error": { "message": message }
        }))
        .unwrap_or_else(|_| "{\"id\":0,\"error\":{\"message\":\"serialization failed\"}}".to_string()),
    )
}

fn build_result_response(id: Option<u64>, result: Value) -> Option<String> {
    let id = id?;
    Some(serde_json::to_string(&json!({ "id": id, "result": result })).unwrap_or_else(|_| {
        "{\"id\":0,\"error\":{\"message\":\"serialization failed\"}}".to_string()
    }))
}

fn build_event_notification(event: DaemonEvent) -> Option<String> {
    let payload = match event {
        DaemonEvent::AppServer(payload) => json!({
            "method": "app-server-event",
            "params": payload,
        }),
        DaemonEvent::TerminalOutput(payload) => json!({
            "method": "terminal-output",
            "params": payload,
        }),
    };
    serde_json::to_string(&payload).ok()
}

fn parse_auth_token(params: &Value) -> Option<String> {
    match params {
        Value::String(value) => Some(value.clone()),
        Value::Object(map) => map
            .get("token")
            .and_then(|value| value.as_str())
            .map(|v| v.to_string()),
        _ => None,
    }
}

fn parse_string(value: &Value, key: &str) -> Result<String, String> {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| format!("missing or invalid `{key}`")),
        _ => Err(format!("missing `{key}`")),
    }
}

fn parse_optional_string(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(|value| value.as_str())
            .map(|v| v.to_string()),
        _ => None,
    }
}

fn parse_optional_u32(value: &Value, key: &str) -> Option<u32> {
    match value {
        Value::Object(map) => map.get(key).and_then(|value| value.as_u64()).and_then(|v| {
            if v > u32::MAX as u64 {
                None
            } else {
                Some(v as u32)
            }
        }),
        _ => None,
    }
}

fn parse_optional_bool(value: &Value, key: &str) -> Option<bool> {
    match value {
        Value::Object(map) => map.get(key).and_then(|value| value.as_bool()),
        _ => None,
    }
}

fn parse_optional_string_array(value: &Value, key: &str) -> Option<Vec<String>> {
    match value {
        Value::Object(map) => map.get(key).and_then(|value| value.as_array()).map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|value| value.to_string()))
                .collect::<Vec<_>>()
        }),
        _ => None,
    }
}

async fn handle_rpc_request(
    state: &DaemonState,
    method: &str,
    params: Value,
    client_version: String,
) -> Result<Value, String> {
    match method {
        "ping" => Ok(json!({ "ok": true })),
        "list_workspaces" => {
            let workspaces = state.list_workspaces().await;
            serde_json::to_value(workspaces).map_err(|err| err.to_string())
        }
        "add_workspace" => {
            let path = parse_string(&params, "path")?;
            let codex_bin = parse_optional_string(&params, "codex_bin");
            let workspace = state.add_workspace(path, codex_bin, client_version).await?;
            serde_json::to_value(workspace).map_err(|err| err.to_string())
        }
        "add_worktree" => {
            let parent_id = parse_string(&params, "parentId")?;
            let branch = parse_string(&params, "branch")?;
            let workspace = state
                .add_worktree(parent_id, branch, client_version)
                .await?;
            serde_json::to_value(workspace).map_err(|err| err.to_string())
        }
        "connect_workspace" => {
            let id = parse_string(&params, "id")?;
            state.connect_workspace(id, client_version).await?;
            Ok(json!({ "ok": true }))
        }
        "remove_workspace" => {
            let id = parse_string(&params, "id")?;
            state.remove_workspace(id).await?;
            Ok(json!({ "ok": true }))
        }
        "remove_worktree" => {
            let id = parse_string(&params, "id")?;
            state.remove_worktree(id).await?;
            Ok(json!({ "ok": true }))
        }
        "update_workspace_settings" => {
            let id = parse_string(&params, "id")?;
            let settings_value = match params {
                Value::Object(map) => map.get("settings").cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            let settings: WorkspaceSettings =
                serde_json::from_value(settings_value).map_err(|err| err.to_string())?;
            let workspace = state.update_workspace_settings(id, settings).await?;
            serde_json::to_value(workspace).map_err(|err| err.to_string())
        }
        "update_workspace_codex_bin" => {
            let id = parse_string(&params, "id")?;
            let codex_bin = parse_optional_string(&params, "codex_bin");
            let workspace = state.update_workspace_codex_bin(id, codex_bin).await?;
            serde_json::to_value(workspace).map_err(|err| err.to_string())
        }
        "list_workspace_files" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let files = state.list_workspace_files(workspace_id).await?;
            serde_json::to_value(files).map_err(|err| err.to_string())
        }
        "get_app_settings" => {
            let settings = state.app_settings.lock().await;
            serde_json::to_value(settings.clone()).map_err(|err| err.to_string())
        }
        "update_app_settings" => {
            let settings_value = match params {
                Value::Object(map) => map.get("settings").cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            let settings: AppSettings =
                serde_json::from_value(settings_value).map_err(|err| err.to_string())?;
            let updated = state.update_app_settings(settings).await?;
            serde_json::to_value(updated).map_err(|err| err.to_string())
        }
        "start_thread" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.start_thread(workspace_id).await
        }
        "resume_thread" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let thread_id = parse_string(&params, "threadId")?;
            state.resume_thread(workspace_id, thread_id).await
        }
        "list_threads" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let cursor = parse_optional_string(&params, "cursor");
            let limit = parse_optional_u32(&params, "limit");
            state.list_threads(workspace_id, cursor, limit).await
        }
        "archive_thread" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let thread_id = parse_string(&params, "threadId")?;
            state.archive_thread(workspace_id, thread_id).await
        }
        "send_user_message" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let thread_id = parse_string(&params, "threadId")?;
            let text = parse_string(&params, "text")?;
            let model = parse_optional_string(&params, "model");
            let effort = parse_optional_string(&params, "effort");
            let access_mode = parse_optional_string(&params, "accessMode");
            let images = parse_optional_string_array(&params, "images");
            state
                .send_user_message(workspace_id, thread_id, text, model, effort, access_mode, images)
                .await
        }
        "turn_interrupt" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let thread_id = parse_string(&params, "threadId")?;
            let turn_id = parse_string(&params, "turnId")?;
            state.turn_interrupt(workspace_id, thread_id, turn_id).await
        }
        "start_review" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let thread_id = parse_string(&params, "threadId")?;
            let target = params
                .as_object()
                .and_then(|map| map.get("target"))
                .cloned()
                .ok_or("missing `target`")?;
            let delivery = parse_optional_string(&params, "delivery");
            state.start_review(workspace_id, thread_id, target, delivery).await
        }
        "model_list" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.model_list(workspace_id).await
        }
        "account_rate_limits" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.account_rate_limits(workspace_id).await
        }
        "skills_list" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.skills_list(workspace_id).await
        }
        "respond_to_server_request" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let map = params.as_object().ok_or("missing requestId")?;
            let request_id = map
                .get("requestId")
                .and_then(|value| value.as_u64())
                .ok_or("missing requestId")?;
            let result = map.get("result").cloned().ok_or("missing `result`")?;
            state
                .respond_to_server_request(workspace_id, request_id, result)
                .await
        }
        "get_git_status" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.get_git_status(workspace_id).await
        }
        "get_git_diffs" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let diffs = state.get_git_diffs(workspace_id).await?;
            serde_json::to_value(diffs).map_err(|err| err.to_string())
        }
        "get_git_log" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let limit = match &params {
                Value::Object(map) => map
                    .get("limit")
                    .and_then(|value| value.as_u64())
                    .and_then(|value| usize::try_from(value).ok()),
                _ => None,
            };
            let log = state.get_git_log(workspace_id, limit).await?;
            serde_json::to_value(log).map_err(|err| err.to_string())
        }
        "get_git_remote" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let remote = state.get_git_remote(workspace_id).await?;
            serde_json::to_value(remote).map_err(|err| err.to_string())
        }
        "get_github_issues" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let issues = state.get_github_issues(workspace_id).await?;
            serde_json::to_value(issues).map_err(|err| err.to_string())
        }
        "list_git_branches" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            state.list_git_branches(workspace_id).await
        }
        "checkout_git_branch" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let name = parse_string(&params, "name")?;
            state.checkout_git_branch(workspace_id, name).await?;
            Ok(json!({ "ok": true }))
        }
        "create_git_branch" => {
            let workspace_id = parse_string(&params, "workspaceId")?;
            let name = parse_string(&params, "name")?;
            state.create_git_branch(workspace_id, name).await?;
            Ok(json!({ "ok": true }))
        }
        _ => Err(format!("unknown method: {method}")),
    }
}

async fn forward_events(
    mut rx: broadcast::Receiver<DaemonEvent>,
    out_tx_events: mpsc::UnboundedSender<String>,
) {
    loop {
        let event = match rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        };

        let Some(payload) = build_event_notification(event) else {
            continue;
        };

        if out_tx_events.send(payload).is_err() {
            break;
        }
    }
}

async fn handle_client(
    socket: TcpStream,
    config: Arc<DaemonConfig>,
    state: Arc<DaemonState>,
    events: broadcast::Sender<DaemonEvent>,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let write_task = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            if writer.write_all(message.as_bytes()).await.is_err() {
                break;
            }
            if writer.write_all(b"\n").await.is_err() {
                break;
            }
        }
    });

    let mut authenticated = config.token.is_none();
    let mut events_task: Option<tokio::task::JoinHandle<()>> = None;

    if authenticated {
        let rx = events.subscribe();
        let out_tx_events = out_tx.clone();
        events_task = Some(tokio::spawn(forward_events(rx, out_tx_events)));
    }

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let message: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let id = message.get("id").and_then(|value| value.as_u64());
        let method = message
            .get("method")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();
        let params = message.get("params").cloned().unwrap_or(Value::Null);

        if !authenticated {
            if method != "auth" {
                if let Some(response) = build_error_response(id, "unauthorized") {
                    let _ = out_tx.send(response);
                }
                continue;
            }

            let expected = config.token.clone().unwrap_or_default();
            let provided = parse_auth_token(&params).unwrap_or_default();
            if expected != provided {
                if let Some(response) = build_error_response(id, "invalid token") {
                    let _ = out_tx.send(response);
                }
                continue;
            }

            authenticated = true;
            if let Some(response) = build_result_response(id, json!({ "ok": true })) {
                let _ = out_tx.send(response);
            }

            let rx = events.subscribe();
            let out_tx_events = out_tx.clone();
            events_task = Some(tokio::spawn(forward_events(rx, out_tx_events)));

            continue;
        }

        let client_version = format!("daemon-{}", env!("CARGO_PKG_VERSION"));
        let result = handle_rpc_request(&state, &method, params, client_version).await;
        let response = match result {
            Ok(result) => build_result_response(id, result),
            Err(message) => build_error_response(id, &message),
        };
        if let Some(response) = response {
            let _ = out_tx.send(response);
        }
    }

    drop(out_tx);
    if let Some(task) = events_task {
        task.abort();
    }
    write_task.abort();
}

fn main() {
    let config = match parse_args() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}\n\n{}", usage());
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async move {
        let (events_tx, _events_rx) = broadcast::channel::<DaemonEvent>(2048);
        let event_sink = DaemonEventSink {
            tx: events_tx.clone(),
        };
        let state = Arc::new(DaemonState::load(&config, event_sink));
        let config = Arc::new(config);

        let listener = TcpListener::bind(config.listen)
            .await
            .unwrap_or_else(|err| panic!("failed to bind {}: {err}", config.listen));
        eprintln!(
            "codex-monitor-daemon listening on {} (data dir: {})",
            config.listen,
            state
                .storage_path
                .parent()
                .unwrap_or(&state.storage_path)
                .display()
        );

        loop {
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    let config = Arc::clone(&config);
                    let state = Arc::clone(&state);
                    let events = events_tx.clone();
                    tokio::spawn(async move {
                        handle_client(socket, config, state, events).await;
                    });
                }
                Err(_) => continue,
            }
        }
    });
}
