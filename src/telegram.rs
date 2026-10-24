use crate::{config::Config, digest, sources::Identity, storage::Store};
use anyhow::{Context, Result, anyhow, ensure};
use futures_util::StreamExt;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub edited_message: Option<Message>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TelegramId {
    pub id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub chat: TelegramId,
    pub from: Option<TelegramId>,
    pub text: Option<String>,
    pub caption: Option<String>,
    #[serde(default)]
    pub entities: Vec<Entity>,
    #[serde(default)]
    pub caption_entities: Vec<Entity>,
    pub document: Option<Document>,
    pub reply_to_message: Option<Box<Message>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity {
    pub r#type: String,
    pub offset: usize,
    pub length: usize,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    Transform {
        operation: Operation,
        source: Box<Request>,
    },
    Url {
        url: String,
    },
    Text {
        text: String,
    },
    Document {
        file_id: String,
        unique_id: String,
        name: String,
        size: u64,
    },
    LocalDocument {
        digest: String,
        name: String,
        rename: bool,
    },
    Command {
        name: String,
        argument: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Translate,
    Tts,
    Summarize,
}

impl Request {
    pub fn source(&self) -> &Self {
        match self {
            Self::Transform { source, .. } => source,
            _ => self,
        }
    }

    pub(crate) fn job_config(&self, config: &Config) -> Config {
        let mut selected = config.clone();
        if let Self::Transform { operation, .. } = self {
            selected.model.enabled &= *operation == Operation::Summarize;
            selected.translation.enabled &= *operation == Operation::Translate;
            selected.synthesis.enabled &= *operation == Operation::Tts;
        }
        selected
    }
}

// Telegram entity offsets count UTF-16 code units, not UTF-8 bytes or characters.
fn entity_text(text: &str, offset: usize, length: usize) -> Result<String> {
    let units = text.encode_utf16().collect::<Vec<_>>();
    let end = offset
        .checked_add(length)
        .context("entity offset overflow")?;
    Ok(String::from_utf16(
        units.get(offset..end).context("entity outside text")?,
    )?)
}

pub fn parse_requests(message: &Message, config: &Config) -> Result<Vec<Request>> {
    let text = message.text.as_deref().unwrap_or("");
    let caption = message.caption.as_deref().unwrap_or("");
    ensure!(
        text.len().saturating_add(caption.len()) as u64 <= config.limits.input_bytes,
        "input exceeds byte limit"
    );
    if text.starts_with('/') {
        let (command, argument) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
        let name = command
            .trim_start_matches('/')
            .split('@')
            .next()
            .unwrap_or("");
        let operation = match name {
            "translate" => Some(Operation::Translate),
            "tts" => Some(Operation::Tts),
            "summarize" => Some(Operation::Summarize),
            _ => None,
        };
        if let Some(operation) = operation {
            let mut source = if let Some(reply) = &message.reply_to_message {
                ensure!(
                    reply.chat.id == message.chat.id,
                    "reply belongs to another chat"
                );
                ensure!(
                    argument.trim().is_empty(),
                    "reply commands take no inline input"
                );
                (**reply).clone()
            } else {
                ensure!(
                    !argument.trim().is_empty(),
                    "reply to a source or provide inline input"
                );
                let mut source = message.clone();
                source.text = Some(argument.trim().to_owned());
                source.entities.clear();
                source
            };
            source.reply_to_message = None;
            if source.document.is_some() {
                source.text = None;
                source.caption = None;
                source.entities.clear();
                source.caption_entities.clear();
            }
            ensure!(
                !source.text.as_deref().unwrap_or("").starts_with('/'),
                "a command is not source text"
            );
            return Ok(parse_requests(&source, config)?
                .into_iter()
                .map(|source| Request::Transform {
                    operation,
                    source: Box::new(source),
                })
                .collect());
        }
        let name = if name == "start" { "help" } else { name };
        ensure!(
            [
                "help", "status", "cancel", "retry", "get", "refresh", "resend"
            ]
            .contains(&name),
            "unsupported command"
        );
        return Ok(vec![Request::Command {
            name: name.into(),
            argument: argument.trim().into(),
        }]);
    }
    let mut urls = vec![];
    for (body, entities) in [
        (text, &message.entities),
        (caption, &message.caption_entities),
    ] {
        for entity in entities {
            if entity.r#type == "text_link" {
                entity_text(body, entity.offset, entity.length)?;
                if let Some(url) = &entity.url {
                    urls.push(url.clone());
                }
            } else if entity.r#type == "url" {
                urls.push(entity_text(body, entity.offset, entity.length)?);
            }
        }
        for word in body.split_whitespace() {
            let word = word.trim_matches(|c: char| "<>\"".contains(c));
            if word.split_once("://").is_some_and(|(scheme, _)| {
                scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("http")
            }) {
                urls.push(word.into());
            }
        }
    }
    let mut seen = BTreeSet::new();
    let mut requests = vec![];
    for url in urls {
        let identity = Identity::from_url(&url)?;
        if seen.insert(identity.id()) {
            requests.push(Request::Url { url });
        }
    }
    ensure!(requests.len() <= config.limits.max_urls, "too many URLs");
    if let Some(document) = &message.document {
        let name = document.file_name.as_deref().unwrap_or("document.txt");
        let extension = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
        ensure!(
            ["txt", "md", "markdown", "pdf"].contains(&extension.as_str()),
            "unsupported document type"
        );
        let size = document.file_size.context("document size is required")?;
        ensure!(
            size > 0 && size <= config.limits.input_bytes,
            "document exceeds byte limit"
        );
        if let Some(mime) = &document.mime_type {
            ensure!(
                [
                    "text/plain",
                    "text/markdown",
                    "text/x-markdown",
                    "application/octet-stream",
                    "application/pdf"
                ]
                .contains(&mime.as_str()),
                "unsupported document MIME"
            );
        }
        requests.push(Request::Document {
            file_id: document.file_id.clone(),
            unique_id: document.file_unique_id.clone(),
            name: crate::documents::display_name(name),
            size,
        });
    }
    if requests.is_empty() && !text.trim().is_empty() {
        requests.push(Request::Text { text: text.into() });
    }
    ensure!(!requests.is_empty(), "no supported input");
    Ok(requests)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ingress {
    Scheduled(usize),
    Replayed,
    Rejected(&'static str),
}

impl Store {
    pub fn polling_offset(&self, bot_id: i64) -> Result<i64> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT next_offset FROM polling_cursors WHERE bot_id=?1",
                [bot_id],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn ingest(
        &self,
        config: &Config,
        update: &Update,
        tool_versions: &Value,
    ) -> Result<Ingress> {
        self.ingest_inner(config, update, tool_versions, || Ok(()))
    }

    fn ingest_inner(
        &self,
        config: &Config,
        update: &Update,
        tool_versions: &Value,
        before_commit: impl FnOnce() -> Result<()>,
    ) -> Result<Ingress> {
        ensure!(
            update.update_id >= 0 && update.update_id < i64::MAX,
            "invalid update ID"
        );
        let bot = config.telegram.bot_id;
        let message = update.message.as_ref().or(update.edited_message.as_ref());
        let authorized = message.is_some_and(|m| {
            m.from
                .as_ref()
                .is_some_and(|u| config.telegram.authorized(m.chat.id, u.id))
        });
        let parsed = if authorized {
            parse_requests(message.unwrap(), config).ok()
        } else {
            None
        };
        let invalid = authorized && parsed.is_none();
        let parsed = parsed.or_else(|| {
            (invalid && config.telegram.enabled).then(|| {
                vec![Request::Command {
                    name: "_invalid_input".into(),
                    argument: String::new(),
                }]
            })
        });
        let reason = if !authorized {
            "unauthorized_or_unsupported"
        } else {
            "invalid_input"
        };
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let replay=tx.query_row("SELECT EXISTS(SELECT 1 FROM inbox WHERE bot_id=?1 AND update_id=?2 UNION ALL SELECT 1 FROM ingress_rejections WHERE bot_id=?1 AND update_id=?2)",params![bot,update.update_id],|r|r.get::<_,bool>(0))?;
        let outcome = if replay {
            Ingress::Replayed
        } else if let Some(requests) = parsed {
            let m = message.unwrap();
            let tools = serde_json::to_string(tool_versions)?;
            // Retain bounded normalized requests instead of the full provider payload.
            tx.execute(
                "INSERT INTO inbox VALUES(?1,?2,?3,?4,?5)",
                params![
                    bot,
                    update.update_id,
                    m.chat.id,
                    m.from.as_ref().unwrap().id,
                    serde_json::to_string(&requests)?
                ],
            )?;
            if m.message_id > 0 {
                tx.execute(
                    "INSERT INTO message_origins VALUES(?1,?2,?3)",
                    params![bot, update.update_id, m.message_id],
                )?;
            }
            for (ordinal, request) in requests.iter().enumerate() {
                let config_json = serde_json::to_string(&request.job_config(config))?;
                let config_id = digest(format!("{config_json}\n{tools}"));
                tx.execute(
                    "INSERT INTO config_snapshots VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                    params![config_id, config_json, tools],
                )?;
                let id = digest(format!("{bot}:{}:{ordinal}", update.update_id));
                tx.execute("INSERT INTO jobs(id,bot_id,update_id,ordinal,request,config_id,state) VALUES(?1,?2,?3,?4,?5,?6,'queued')",params![id,bot,update.update_id,ordinal,serde_json::to_string(request)?,config_id])?;
                if config.telegram.enabled && !matches!(request, Request::Command { .. }) {
                    crate::delivery::enqueue_progress_tx(&tx, &id)?;
                }
            }
            if invalid {
                tx.execute(
                    "INSERT INTO ingress_rejections VALUES(?1,?2,?3)",
                    params![bot, update.update_id, reason],
                )?;
                Ingress::Rejected(reason)
            } else {
                Ingress::Scheduled(requests.len())
            }
        } else {
            tx.execute(
                "INSERT INTO ingress_rejections VALUES(?1,?2,?3)",
                params![bot, update.update_id, reason],
            )?;
            Ingress::Rejected(reason)
        };
        tx.execute("INSERT INTO polling_cursors VALUES(?1,?2) ON CONFLICT(bot_id) DO UPDATE SET next_offset=MAX(next_offset,excluded.next_offset)",params![bot,update.update_id+1])?;
        before_commit()?;
        tx.commit()?;
        Ok(outcome)
    }
}

pub struct Telegram {
    pub(crate) client: reqwest::Client,
    token: String,
    base: String,
}

impl Telegram {
    pub fn new(token: String) -> Result<Self> {
        ensure!(
            !token.is_empty() && !token.contains(['/', '?', '#']),
            "invalid bot token syntax"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            token,
            base: "https://api.telegram.org".into(),
        })
    }

    pub(crate) fn endpoint(&self, method: &str) -> String {
        format!("{}/bot{}/{method}", self.base, self.token)
    }

    #[cfg(test)]
    pub(crate) fn fixture_base(mut self, base: String) -> Self {
        self.base = base;
        self
    }

    pub(crate) async fn envelope(response: reqwest::Response, max_bytes: usize) -> Result<Value> {
        let mut bytes = vec![];
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("Telegram response interrupted"))?;
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= max_bytes,
                "Telegram response exceeds byte limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        let body: Value =
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid Telegram JSON"))?;
        Ok(body)
    }

    pub(crate) async fn response(response: reqwest::Response, max_bytes: usize) -> Result<Value> {
        let body = Self::envelope(response, max_bytes).await?;
        ensure!(
            body["ok"] == true,
            "Telegram API rejected request (code {})",
            body["error_code"].as_i64().unwrap_or(0)
        );
        Ok(body["result"].clone())
    }

    pub async fn poll_once(
        &self,
        store: &Store,
        config: &Config,
        versions: &Value,
        cancel: &CancellationToken,
    ) -> Result<usize> {
        let offset = store.polling_offset(config.telegram.bot_id)?;
        let request=self.client.post(self.endpoint("getUpdates")).json(&json!({"offset":offset,"limit":100,"timeout":20,"allowed_updates":["message","edited_message"]})).send();
        let response = tokio::select! { biased; _=cancel.cancelled()=>return Ok(0),response=request=>response.map_err(|_|anyhow!("Telegram polling transport failed"))? };
        let result = tokio::select! { biased; _=cancel.cancelled()=>return Ok(0),result=Self::response(response,8*1024*1024)=>result? };
        let mut updates: Vec<Update> =
            serde_json::from_value(result).map_err(|_| anyhow!("invalid Telegram update shape"))?;
        updates.sort_by_key(|u| u.update_id);
        let count = updates.len();
        for update in updates {
            store.ingest(config, &update, versions)?;
        }
        Ok(count)
    }

    pub async fn download_document(
        &self,
        id: &str,
        limit: u64,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let request = async {
            let response = self
                .client
                .post(self.endpoint("getFile"))
                .json(&json!({"file_id":id}))
                .send()
                .await
                .map_err(|_| anyhow!("Telegram file lookup transport failed"))?;
            let file = Self::response(response, 65536).await?;
            let path = file["file_path"]
                .as_str()
                .ok_or_else(|| anyhow!("Telegram file path missing"))?;
            ensure!(
                file["file_size"].as_u64().is_none_or(|size| size <= limit)
                    && path.len() < 1024
                    && !path.is_empty()
                    && path
                        .split('/')
                        .all(|p| !p.is_empty() && p != "." && p != "..")
                    && path
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"/_-.".contains(&b)),
                "invalid Telegram file descriptor"
            );
            let response = self
                .client
                .get(format!("{}/file/bot{}/{path}", self.base, self.token))
                .send()
                .await
                .map_err(|_| anyhow!("Telegram file download transport failed"))?;
            ensure!(
                response.status().is_success(),
                "Telegram file download rejected"
            );
            let mut bytes = vec![];
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| anyhow!("Telegram file download interrupted"))?;
                ensure!(
                    (bytes.len() as u64).saturating_add(chunk.len() as u64) <= limit,
                    "Telegram document exceeds byte limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        };
        tokio::select! {biased;_=cancel.cancelled()=>Err(anyhow!("Telegram file download cancelled")),result=tokio::time::timeout(Duration::from_secs(60),request)=>result.map_err(|_|anyhow!("Telegram file download deadline"))?}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::Permissions, os::unix::fs::PermissionsExt};

    #[tokio::test]
    async fn document_download_bounds_paths_bytes_and_redacts_transport_errors() -> Result<()> {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        for (path, declared, body, limit, success, expected_requests) in [
            ("documents/file.txt", 3, "abc", 4, true, 2),
            ("../escape.txt", 3, "abc", 4, false, 1),
            ("documents/file.txt", 5, "abcde", 4, false, 1),
            ("documents/file.txt", 3, "oversized", 4, false, 2),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = tokio::spawn(async move {
                for n in 0..expected_requests {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut input = vec![];
                    loop {
                        let mut block = [0u8; 1024];
                        let count = socket.read(&mut block).await.unwrap();
                        assert!(count > 0);
                        input.extend_from_slice(&block[..count]);
                        if let Some(end) = input.windows(4).position(|w| w == b"\r\n\r\n") {
                            let length = String::from_utf8_lossy(&input[..end])
                                .lines()
                                .find_map(|l| {
                                    l.to_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|n| n.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if input.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let first_line = String::from_utf8_lossy(&input)
                        .lines()
                        .next()
                        .unwrap()
                        .to_owned();
                    assert!(first_line.starts_with(if n == 0 {
                        "POST /botsynthetic-token/getFile "
                    } else {
                        "GET /file/botsynthetic-token/documents/file.txt "
                    }));
                    let output = if n == 0 {
                        json!({"ok":true,"result":{"file_path":path,"file_size":declared}})
                            .to_string()
                    } else {
                        body.into()
                    };
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{output}",output.len()).as_bytes()).await.unwrap();
                }
            });
            let client =
                Telegram::new("synthetic-token".into())?.fixture_base(format!("http://{address}"));
            let result = client
                .download_document("synthetic-file", limit, &CancellationToken::new())
                .await;
            assert_eq!(result.is_ok(), success);
            if let Ok(bytes) = result {
                assert_eq!(bytes, b"abc");
            }
            tokio::time::timeout(Duration::from_secs(5), server).await??;
            let error = client
                .download_document("synthetic-file", limit, &CancellationToken::new())
                .await
                .unwrap_err();
            assert!(!format!("{error:?}").contains("synthetic-token"));
            let cancel = CancellationToken::new();
            cancel.cancel();
            assert!(
                client
                    .download_document("synthetic-file", limit, &cancel)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("cancelled")
            );
        }
        Ok(())
    }

    fn config() -> Config {
        let mut config = Config::default();
        config.telegram.bot_id = 10;
        config.telegram.allowed_chats = vec![20];
        config.telegram.allowed_users = vec![30];
        config
    }

    fn update() -> Update {
        serde_json::from_value(json!({"update_id":42,"message":{"message_id":1,"chat":{"id":20},"from":{"id":30},"text":"😀 https://x.com/a/status/123 https://example.org/?s=two","entities":[{"type":"url","offset":3,"length":26}]}})).unwrap()
    }

    #[test]
    fn unannotated_http_schemes_are_case_insensitive() -> Result<()> {
        let config = config();
        let mut u = update();
        let message = u.message.as_mut().unwrap();
        message.entities.clear();
        for url in [
            "HTTP://example.org/Case?A=B#Fragment",
            "hTtPs://example.net/Route",
        ] {
            for caption in [false, true] {
                message.text = (!caption).then(|| url.into());
                message.caption = caption.then(|| url.into());
                let requests = parse_requests(message, &config)?;
                assert_eq!(requests.len(), 1);
                assert!(
                    matches!(&requests[0], Request::Url { url: submitted } if submitted == url)
                );
            }
        }
        message.caption = None;
        message.text = Some("HTTPS://example.org/Original".into());
        message.reply_to_message = Some(Box::new(message.clone()));
        message.text = Some("/tts".into());
        assert!(
            matches!(&parse_requests(message, &config)?[0], Request::Transform { source, .. }
            if matches!(source.as_ref(), Request::Url { url } if url == "HTTPS://example.org/Original"))
        );
        message.reply_to_message = None;
        for text in [
            "ordinary text",
            "HTTPX://example.org/",
            "HTTPSx://example.org/",
        ] {
            message.text = Some(text.into());
            assert!(matches!(
                &parse_requests(message, &config)?[0],
                Request::Text { .. }
            ));
        }
        message.text = Some("HTTP://[".into());
        assert!(parse_requests(message, &config).is_err());
        Ok(())
    }

    #[test]
    fn unicode_entities_captions_documents_and_limits() -> Result<()> {
        let config = config();
        let mut u = update();
        // The exact UTF-16 offset is tested independently of fallback word scanning.
        assert_eq!(entity_text("😀 abc", 3, 3)?, "abc");
        assert!(entity_text("😀", 1, 1).is_err());
        let m = u.message.as_mut().unwrap();
        m.entities.clear();
        assert_eq!(parse_requests(m, &config)?.len(), 2);
        m.text = None;
        m.caption = Some("click".into());
        m.caption_entities = vec![Entity {
            r#type: "text_link".into(),
            offset: 0,
            length: 5,
            url: Some("https://example.org/#/route".into()),
        }];
        m.document = Some(Document {
            file_id: "id".into(),
            file_unique_id: "unique".into(),
            file_name: Some("../../same name.md".into()),
            mime_type: Some("text/markdown".into()),
            file_size: Some(10),
        });
        let requests = parse_requests(m, &config)?;
        assert_eq!(requests.len(), 2);
        assert!(matches!(&requests[1],Request::Document{name,..} if name=="same name.md"));
        m.document.as_mut().unwrap().file_name = Some("..\\a.pdf".into());
        m.document.as_mut().unwrap().mime_type = Some("application/pdf".into());
        assert!(
            matches!(&parse_requests(m, &config)?[1], Request::Document{name,..} if name=="a.pdf")
        );
        m.document.as_mut().unwrap().file_size = Some(config.limits.input_bytes + 1);
        assert!(parse_requests(m, &config).is_err());
        Ok(())
    }

    #[test]
    fn inbox_children_and_cursor_commit_together_and_replay_is_bot_scoped() -> Result<()> {
        let dir = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(dir.path())?;
        let mut config = config();
        let mut update = update();
        update.message.as_mut().unwrap().entities.clear();
        assert!(
            store
                .ingest_inner(&config, &update, &json!({}), || Err(anyhow!(
                    "injected precommit failure"
                )))
                .is_err()
        );
        assert_eq!(store.polling_offset(10)?, 0);
        assert_eq!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Scheduled(2)
        );
        assert_eq!(store.polling_offset(10)?, 43);
        assert_eq!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Replayed
        );
        let conn = store.connection()?;
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, u64>(0))?,
            2
        );
        config.telegram.bot_id = 11;
        assert_eq!(store.polling_offset(11)?, 0);
        assert_eq!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Scheduled(2)
        );
        update.update_id = 43;
        update.message.as_mut().unwrap().from.as_mut().unwrap().id = 999;
        assert!(matches!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Rejected(_)
        ));
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, u64>(0))?,
            4
        );
        assert_eq!(store.polling_offset(11)?, 44);
        Ok(())
    }

    #[tokio::test]
    async fn next_poll_confirms_only_committed_offsets_and_transport_errors_hide_token()
    -> Result<()> {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut input = update();
        input.message.as_mut().unwrap().entities.clear();
        let server = tokio::spawn(async move {
            let mut offsets = vec![];
            for result in [json!([input]), json!([])] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![];
                let body_start = loop {
                    let mut chunk = [0; 1024];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break index + 4;
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..body_start]);
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        line.to_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < body_start + length {
                    let mut chunk = [0; 1024];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let request: Value =
                    serde_json::from_slice(&bytes[body_start..body_start + length]).unwrap();
                offsets.push(request["offset"].as_i64().unwrap());
                let body = serde_json::to_string(&json!({"ok":true,"result":result})).unwrap();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            offsets
        });
        let dir = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(dir.path())?;
        let mut client = Telegram::new("synthetic-sensitive-token".into())?;
        client.base = format!("http://{address}");
        let cancel = CancellationToken::new();
        assert_eq!(
            client
                .poll_once(&store, &config(), &json!({}), &cancel)
                .await?,
            1
        );
        assert_eq!(store.polling_offset(10)?, 43);
        assert_eq!(
            client
                .poll_once(&store, &config(), &json!({}), &cancel)
                .await?,
            0
        );
        assert_eq!(server.await?, vec![0, 43]);
        let error = client
            .poll_once(&store, &config(), &json!({}), &cancel)
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("synthetic-sensitive-token"));
        cancel.cancel();
        assert_eq!(
            client
                .poll_once(&store, &config(), &json!({}), &cancel)
                .await?,
            0
        );
        Ok(())
    }
}
