//! Concrete ntn 0.22.10 API transport. Only compiled by the default `ntn` feature.
use super::*;
use crate::process::{Tool, ToolError, ToolFailure};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    time::Duration,
};
use tokio::time::Instant;

struct Operation<'a> {
    adapter: &'a Adapter,
    cancel: &'a CancellationToken,
    work: tempfile::TempDir,
    token: String,
    deadline: Instant,
    requests: usize,
    write_started: bool,
    page_id: Option<String>,
    observer: &'a ProgressObserver<'a>,
    written_blocks: usize,
}

impl<'a> Operation<'a> {
    async fn new(
        adapter: &'a Adapter,
        cancel: &'a CancellationToken,
        observer: &'a ProgressObserver<'a>,
    ) -> Result<Self, PublicationError> {
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled.into());
        }
        let token = adapter
            .config
            .token
            .as_ref()
            .ok_or(Failure::PendingConfiguration)?
            .read()
            .map_err(|_| Failure::PendingConfiguration)?;
        if token.is_empty() || token.len() > 16384 {
            return Err(Failure::PendingConfiguration.into());
        }
        let work = tempfile::Builder::new()
            .prefix("notion-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(&adapter.work_root)
            .map_err(|_| Failure::Unavailable)?;
        let mut operation = Self {
            adapter,
            cancel,
            work,
            token,
            deadline: Instant::now() + Duration::from_secs(adapter.config.operation_seconds),
            requests: 0,
            write_started: false,
            page_id: None,
            observer,
            written_blocks: 0,
        };
        let version = operation.tool(vec!["--version".into()], false).await?;
        if version.trim() != NTN_VERSION {
            return Err(operation.error(Failure::UnsupportedTool));
        }
        Ok(operation)
    }

    fn error(&self, state: Failure) -> PublicationError {
        PublicationError {
            state,
            page_id: self.page_id.clone(),
            retry_after_seconds: None,
        }
    }

    fn progress(&self, verified_blocks: usize) -> Result<(), PublicationError> {
        if let Some(page_id) = &self.page_id {
            (self.observer)(&PublicationProgress {
                page_id: page_id.clone(),
                written_blocks: self.written_blocks,
                verified_blocks,
            })
            .map_err(|_| self.uncertain(Failure::Unavailable))?;
        }
        Ok(())
    }

    fn uncertain(&self, otherwise: Failure) -> PublicationError {
        self.error(if self.write_started {
            Failure::Reconcile
        } else {
            otherwise
        })
    }

    fn remote_error(&self, value: &Value) -> Option<PublicationError> {
        if value.get("object").and_then(Value::as_str) != Some("error") {
            return None;
        }
        let status = value.get("status").and_then(Value::as_u64)?;
        let state = match status {
            401 | 403 => Failure::BlockedAuth,
            429 | 529 => Failure::RetryWait,
            409 => Failure::Conflict,
            400 | 404 | 422 => Failure::InvalidPayload,
            _ => {
                if self.write_started {
                    Failure::Reconcile
                } else {
                    Failure::Unavailable
                }
            }
        };
        let mut error = self.error(state);
        if state == Failure::RetryWait {
            // ntn may not expose response headers; never infer Retry-After from prose.
            error.retry_after_seconds = value.get("retry_after").and_then(Value::as_u64);
        }
        Some(error)
    }

    fn tool_error(&self, error: ToolError) -> PublicationError {
        if let Some(report) = error.report {
            for text in [&report.stdout, &report.stderr] {
                if let Ok(value) = serde_json::from_str(text)
                    && let Some(classified) = self.remote_error(&value)
                {
                    return classified;
                }
            }
        }
        match error.kind {
            ToolFailure::Missing | ToolFailure::InvalidConfiguration => {
                self.error(Failure::PendingConfiguration)
            }
            ToolFailure::Cancelled => self.uncertain(Failure::Cancelled),
            ToolFailure::LogLimit | ToolFailure::WorkLimit => self.uncertain(Failure::Limit),
            _ => self.uncertain(Failure::Unavailable),
        }
    }

    async fn tool(
        &mut self,
        args: Vec<OsString>,
        writing: bool,
    ) -> Result<String, PublicationError> {
        if self.cancel.is_cancelled() {
            return Err(self.uncertain(Failure::Cancelled));
        }
        if self.requests >= self.adapter.config.max_requests {
            return Err(self.uncertain(Failure::Limit));
        }
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.uncertain(Failure::Limit));
        }
        self.requests += 1;
        self.write_started |= writing;
        let environment: BTreeMap<OsString, OsString> = [
            ("NOTION_API_TOKEN".into(), self.token.clone().into()),
            ("NOTION_API_VERSION".into(), API_VERSION.into()),
            (
                "NOTION_API_BASE_URL".into(),
                "https://api.notion.com".into(),
            ),
            ("NOTION_KEYRING".into(), "0".into()),
            (
                "NOTION_HOME".into(),
                self.work.path().join("notion-home").into(),
            ),
            (
                "XDG_CONFIG_HOME".into(),
                self.work.path().join("config").into(),
            ),
        ]
        .into_iter()
        .collect();
        Tool {
            executable: self
                .adapter
                .config
                .executable
                .clone()
                .ok_or(Failure::PendingConfiguration)?,
            args,
            environment,
            directory: self.work.path().to_owned(),
            timeout: remaining.min(Duration::from_secs(self.adapter.config.request_seconds)),
            log_bytes: 16384,
            stdout_bytes: Some(self.adapter.config.response_bytes),
            work_bytes: 16_000_000,
            secrets: vec![self.token.clone()],
        }
        .run(self.cancel)
        .await
        .map(|result| result.stdout)
        .map_err(|error| self.tool_error(error))
    }

    async fn request(
        &mut self,
        path: &str,
        method: &str,
        body: Option<&Value>,
        cursor: Option<&str>,
    ) -> Result<Value, PublicationError> {
        let mut args: Vec<OsString> = vec!["api".into(), path.into(), "-X".into(), method.into()];
        let body_path = self.work.path().join("body.json");
        if let Some(body) = body {
            let bytes =
                serde_json::to_vec(body).map_err(|_| self.error(Failure::InvalidPayload))?;
            if bytes.len() > self.adapter.config.payload_bytes {
                return Err(self.error(Failure::Limit));
            }
            if String::from_utf8_lossy(&bytes).contains(&self.token) {
                return Err(self.error(Failure::InvalidPayload));
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&body_path)
                .map_err(|_| self.error(Failure::Unavailable))?;
            file.write_all(&bytes)
                .map_err(|_| self.error(Failure::Unavailable))?;
            file.flush().map_err(|_| self.error(Failure::Unavailable))?;
            args.extend(["-d".into(), format!("@{}", body_path.display()).into()]);
        }
        if path.ends_with("/children") && method == "GET" {
            args.push("page_size==100".into());
            if let Some(cursor) = cursor {
                args.push(format!("start_cursor=={cursor}").into());
            }
        }
        args.extend(["--notion-version".into(), API_VERSION.into()]);
        let result = self
            .tool(args, method != "GET" && path != "v1/search")
            .await;
        if body.is_some() {
            std::fs::remove_file(&body_path).map_err(|_| self.uncertain(Failure::Unavailable))?;
        }
        let raw = result?;
        let value: Value =
            serde_json::from_str(&raw).map_err(|_| self.uncertain(Failure::InvalidOutput))?;
        if let Some(error) = self.remote_error(&value) {
            return Err(error);
        }
        if !value.is_object() {
            return Err(self.uncertain(Failure::InvalidOutput));
        }
        Ok(value)
    }

    fn check_page(
        &self,
        page: &Value,
        page_id: &str,
        plan: &PublicationPlan,
    ) -> Result<(), PublicationError> {
        let destination = self
            .adapter
            .config
            .destination
            .as_ref()
            .ok_or(Failure::PendingConfiguration)?;
        let (parent_field, expected_parent, property) = destination_fields(destination);
        let actual_parent = page
            .get("parent")
            .and_then(|p| p.get(parent_field))
            .and_then(Value::as_str)
            .and_then(notion_id);
        let title = page
            .get("properties")
            .and_then(|p| p.get(property))
            .and_then(|p| p.get("title"))
            .and_then(rich_text);
        if page.get("object").and_then(Value::as_str) != Some("page")
            || page
                .get("id")
                .and_then(Value::as_str)
                .and_then(notion_id)
                .as_deref()
                != Some(page_id)
            || page.get("archived").and_then(Value::as_bool) == Some(true)
            || page.get("in_trash").and_then(Value::as_bool) == Some(true)
            || actual_parent != notion_id(expected_parent)
            || title.as_deref() != Some(plan.page_title.as_str())
        {
            return Err(self.error(Failure::Conflict));
        }
        Ok(())
    }

    async fn readback(
        &mut self,
        page_id: &str,
        plan: &PublicationPlan,
    ) -> Result<VerifiedPage, PublicationError> {
        let page = self
            .request(&format!("v1/pages/{page_id}"), "GET", None, None)
            .await?;
        self.check_page(&page, page_id, plan)?;
        self.progress(0)?;
        let mut returned = Vec::new();
        let mut block_ids = Vec::new();
        let mut unique_blocks = BTreeSet::new();
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        while returned.len() < plan.managed_blocks.len() {
            let response = self
                .request(
                    &format!("v1/blocks/{page_id}/children"),
                    "GET",
                    None,
                    cursor.as_deref(),
                )
                .await?;
            if response.get("object").and_then(Value::as_str) != Some("list") {
                return Err(self.error(Failure::Conflict));
            }
            let results = response
                .get("results")
                .and_then(Value::as_array)
                .ok_or_else(|| self.error(Failure::Conflict))?;
            if results.len() > 100 {
                return Err(self.error(Failure::Conflict));
            }
            for value in results {
                if returned.len() == plan.managed_blocks.len() {
                    break;
                } // human section is outside the managed canonical projection
                let kind = value
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| self.error(Failure::Conflict))?;
                let text = value
                    .get(kind)
                    .and_then(|v| v.get("rich_text"))
                    .and_then(rich_text)
                    .ok_or_else(|| self.error(Failure::Conflict))?;
                if value.get("object").and_then(Value::as_str) != Some("block")
                    || value.get("has_children").and_then(Value::as_bool) != Some(false)
                {
                    return Err(self.error(Failure::Conflict));
                }
                let block = ManagedBlock {
                    kind: kind.into(),
                    text,
                };
                if plan.managed_blocks.get(returned.len()) != Some(&block) {
                    return Err(self.error(Failure::Conflict));
                }
                let id = value
                    .get("id")
                    .and_then(Value::as_str)
                    .and_then(notion_id)
                    .ok_or_else(|| self.error(Failure::Conflict))?;
                if !unique_blocks.insert(id.clone()) {
                    return Err(self.error(Failure::Conflict));
                }
                block_ids.push(id);
                returned.push(block);
            }
            self.progress(returned.len())?;
            if returned.len() == plan.managed_blocks.len() {
                break;
            }
            if response.get("has_more").and_then(Value::as_bool) != Some(true) || results.is_empty()
            {
                return Err(self.error(Failure::Conflict));
            }
            let next = response
                .get("next_cursor")
                .and_then(Value::as_str)
                .filter(|c| !c.is_empty() && c.len() <= 512 && !c.chars().any(char::is_control))
                .ok_or_else(|| self.error(Failure::Conflict))?;
            if !seen.insert(next.to_owned()) {
                return Err(self.error(Failure::Conflict));
            }
            cursor = Some(next.into());
        }
        let digest = crate::digest(
            serde_json::to_vec(&returned).map_err(|_| self.error(Failure::InvalidOutput))?,
        );
        if digest != plan.canonical_digest {
            return Err(self.error(Failure::Conflict));
        }
        Ok(VerifiedPage {
            page_id: page_id.into(),
            publication_key: plan.publication_key.clone(),
            canonical_digest: digest,
            content_sha256: plan.content_sha256.clone(),
            schema_version: SCHEMA_VERSION,
            block_ids,
            destination_key: self
                .adapter
                .config
                .destination
                .as_ref()
                .and_then(NotionDestination::identity)
                .ok_or(Failure::PendingConfiguration)?,
        })
    }
}

fn rich_text(value: &Value) -> Option<String> {
    let parts = value.as_array()?;
    if parts.len() > 100 {
        return None;
    }
    let mut text = String::new();
    for part in parts {
        if part.get("type").and_then(Value::as_str) != Some("text") {
            return None;
        }
        let content = part.get("text")?.get("content")?.as_str()?;
        if part
            .get("plain_text")
            .is_some_and(|plain| plain.as_str() != Some(content))
        {
            return None;
        }
        text.push_str(content);
    }
    Some(text)
}

fn destination_fields(destination: &NotionDestination) -> (&'static str, &str, &str) {
    match destination {
        NotionDestination::Page { id } => ("page_id", id, "title"),
        NotionDestination::DataSource { id, title_property } => {
            ("data_source_id", id, title_property)
        }
    }
}

fn requests(plan: &PublicationPlan, config: &Notion) -> Result<Vec<Value>, PublicationError> {
    let destination = config
        .destination
        .as_ref()
        .ok_or(Failure::PendingConfiguration)?;
    let (field, id, property) = destination_fields(destination);
    let mut first = json!({"parent":{"type":field},"properties":{},"children":[]});
    first["parent"][field] = json!(notion_id(id).ok_or(Failure::InvalidPayload)?);
    first["properties"][property] =
        json!({"title":[{"type":"text","text":{"content":plan.page_title}}]});
    let mut result = vec![];
    let mut next = 0;
    while next < plan.managed_blocks.len() {
        let mut request = if result.is_empty() {
            first.clone()
        } else {
            json!({"children":[]})
        };
        let start = next;
        while next < plan.managed_blocks.len() && next - start < 100 {
            let b = &plan.managed_blocks[next];
            let block = json!({"object":"block","type":b.kind,(b.kind.clone()):{"rich_text":[{"type":"text","text":{"content":b.text}}]}});
            request["children"]
                .as_array_mut()
                .ok_or(Failure::InvalidPayload)?
                .push(block);
            if serde_json::to_vec(&request)
                .map_err(|_| Failure::InvalidPayload)?
                .len()
                > config.payload_bytes
            {
                request["children"]
                    .as_array_mut()
                    .ok_or(Failure::InvalidPayload)?
                    .pop();
                break;
            }
            next += 1;
        }
        if next == start {
            return Err(Failure::Limit.into());
        }
        result.push(request);
    }
    if result.len() + 3 > config.max_requests {
        return Err(Failure::Limit.into());
    }
    Ok(result)
}

pub(super) async fn create(
    adapter: &Adapter,
    record: &Record,
    cancel: &CancellationToken,
    observer: &ProgressObserver<'_>,
) -> Result<VerifiedPage, PublicationError> {
    let plan = record.plan(&adapter.config)?;
    let bodies = requests(&plan, &adapter.config)?; // validate every request size before creating anything
    let mut operation = Operation::new(adapter, cancel, observer).await?;
    // Token checks apply before any publication, including a later append containing the secret.
    let original =
        serde_json::to_value(record).map_err(|_| operation.error(Failure::InvalidPayload))?;
    if contains_secret(&original, &operation.token)
        || bodies
            .iter()
            .any(|body| contains_secret(body, &operation.token))
    {
        return Err(operation.error(Failure::InvalidPayload));
    }
    let page = operation
        .request("v1/pages", "POST", bodies.first(), None)
        .await?;
    let page_id = page
        .get("id")
        .and_then(Value::as_str)
        .and_then(notion_id)
        .ok_or_else(|| operation.error(Failure::Reconcile))?;
    operation.page_id = Some(page_id.clone());
    operation.check_page(&page, &page_id, &plan)?;
    operation.written_blocks = bodies
        .first()
        .and_then(|b| b["children"].as_array())
        .map_or(0, Vec::len);
    operation.progress(0)?;
    for body in bodies.iter().skip(1) {
        operation
            .request(
                &format!("v1/blocks/{page_id}/children"),
                "PATCH",
                Some(body),
                None,
            )
            .await?;
        operation.written_blocks += body["children"].as_array().map_or(0, Vec::len);
        operation.progress(0)?;
    }
    operation.readback(&page_id, &plan).await
}

fn contains_secret(value: &Value, secret: &str) -> bool {
    match value {
        Value::String(text) => text.contains(secret),
        Value::Array(items) => items.iter().any(|item| contains_secret(item, secret)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| key.contains(secret) || contains_secret(value, secret)),
        _ => false,
    }
}

pub(super) async fn verify(
    adapter: &Adapter,
    page_id: &str,
    record: &Record,
    cancel: &CancellationToken,
    observer: &ProgressObserver<'_>,
) -> Result<VerifiedPage, PublicationError> {
    let page_id = notion_id(page_id).ok_or(Failure::InvalidPayload)?;
    let plan = record.plan(&adapter.config)?;
    let mut operation = Operation::new(adapter, cancel, observer).await?;
    operation.page_id = Some(page_id.clone());
    operation.readback(&page_id, &plan).await
}

pub(super) async fn find(
    adapter: &Adapter,
    record: &Record,
    cancel: &CancellationToken,
    observer: &ProgressObserver<'_>,
) -> Result<Option<VerifiedPage>, PublicationError> {
    let plan = record.plan(&adapter.config)?;
    let mut operation = Operation::new(adapter, cancel, observer).await?;
    let destination = adapter
        .config
        .destination
        .as_ref()
        .ok_or(Failure::PendingConfiguration)?;
    let (field, parent, property) = destination_fields(destination);
    let mut cursor: Option<String> = None;
    let mut cursors = BTreeSet::new();
    let mut matches = BTreeSet::new();
    loop {
        let mut body = json!({"query":plan.page_title,"filter":{"property":"object","value":"page"},"page_size":100});
        if let Some(cursor) = &cursor {
            body["start_cursor"] = json!(cursor);
        }
        let response = operation
            .request("v1/search", "POST", Some(&body), None)
            .await?;
        let results = response
            .get("results")
            .and_then(Value::as_array)
            .filter(|r| r.len() <= 100)
            .ok_or_else(|| operation.error(Failure::InvalidOutput))?;
        if response["object"] != "list" {
            return Err(operation.error(Failure::InvalidOutput));
        }
        for page in results {
            let title = page
                .get("properties")
                .and_then(|v| v.get(property))
                .and_then(|v| v.get("title"))
                .and_then(rich_text);
            let actual_parent = page
                .get("parent")
                .and_then(|v| v.get(field))
                .and_then(Value::as_str)
                .and_then(notion_id);
            if title.as_deref() == Some(&plan.page_title) && actual_parent == notion_id(parent) {
                let id = page
                    .get("id")
                    .and_then(Value::as_str)
                    .and_then(notion_id)
                    .ok_or_else(|| operation.error(Failure::InvalidOutput))?;
                operation.check_page(page, &id, &plan)?;
                matches.insert(id);
                if matches.len() > 1 {
                    return Err(operation.error(Failure::Conflict));
                }
            }
        }
        match response.get("has_more").and_then(Value::as_bool) {
            Some(false) => break,
            Some(true) if !results.is_empty() => {}
            _ => return Err(operation.error(Failure::InvalidOutput)),
        }
        let next = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty() && c.len() <= 512 && !c.chars().any(char::is_control))
            .ok_or_else(|| operation.error(Failure::InvalidOutput))?;
        if !cursors.insert(next.to_owned()) {
            return Err(operation.error(Failure::InvalidOutput));
        }
        cursor = Some(next.to_owned());
    }
    let Some(page) = matches.into_iter().next() else {
        return Ok(None);
    };
    operation.page_id = Some(page.clone());
    operation.readback(&page, &plan).await.map(Some)
}
