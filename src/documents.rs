use crate::{
    config::Config,
    jobs::Failure,
    network::{PublicHttp, Response},
    process::{Tool, ToolFailure, confined_file},
    sources::{Capture, Content, ContentMetadata, FetchStatus, Identity},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, ffi::OsString, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Original {
    pub digest: String,
    pub mime: String,
    pub display_name: String,
    pub validated: bool,
}

#[cfg(test)]
mod browser_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn browser_result_keeps_raw_evidence_and_fails_without_losing_initial_capture()
    -> Result<()> {
        let root = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let adapter = Documents {
            store: Store::open(root.path())?,
            config: Config::default(),
            http: PublicHttp::default(),
        };
        let url = "https://example.org/article";
        let initial_html = b"<html><title>Just a moment</title><body><form id='challenge-form'>Checking your browser</form></body></html>";
        let cancel = CancellationToken::new();
        let initial = adapter
            .response(
                Response {
                    submitted: url.parse()?,
                    resolved: url.parse()?,
                    redirects: vec![],
                    status: 200,
                    content_type: "text/html".into(),
                    charset: None,
                    bytes: initial_html.to_vec(),
                },
                &cancel,
            )
            .await?;
        assert_eq!(initial.capture.status, FetchStatus::Forbidden);
        let html = format!(
            "<html><title>Rendered article</title><body><article><h1>Rendered article</h1><p>{}</p></article></body></html>",
            "An independently rendered article contains enough substantive prose for extraction. "
                .repeat(20)
        );
        let result = adapter
            .finish_browser_attempt(
                url,
                initial.clone(),
                Ok((
                    html.as_bytes().to_vec(),
                    json!({"http_status":null,"resolved_url":null}),
                )),
                &cancel,
            )
            .await?;
        assert_eq!(result.capture.status, FetchStatus::Complete);
        assert!(result.capture.extractor.starts_with("browser+"));
        assert!(result.capture.evidence["http_status"].is_null());
        assert!(result.capture.evidence["resolved_url"].is_null());
        assert_eq!(result.capture.evidence["initial_fetch"]["http_status"], 200);
        assert_eq!(
            result.capture.evidence["initial_fetch"]["original"]["digest"],
            initial.original.digest
        );
        assert_eq!(
            adapter.store.read_blob(&initial.original.digest, 10000)?,
            initial_html
        );
        assert_eq!(
            adapter.store.read_blob(&result.original.digest, 10000)?,
            html.as_bytes()
        );
        let blocked = adapter
            .finish_browser_attempt(
                url,
                initial.clone(),
                Ok((initial_html.to_vec(), json!({}))),
                &cancel,
            )
            .await?;
        assert_eq!(blocked.original.digest, initial.original.digest);
        assert_eq!(blocked.capture.status, FetchStatus::Forbidden);
        assert!(blocked.capture.content.is_none() && blocked.text_digest.is_none());
        assert_eq!(
            blocked.capture.evidence["browser_attempt"]["failure"],
            "verification"
        );
        let failed = adapter
            .finish_browser_attempt(
                url,
                initial.clone(),
                Err(Failure::ToolMissing.into()),
                &cancel,
            )
            .await?;
        assert_eq!(failed.original.digest, initial.original.digest);
        assert_eq!(
            failed.capture.evidence["browser_attempt"]["failure"],
            "tool_missing"
        );
        cancel.cancel();
        let error = adapter
            .finish_browser_attempt(url, initial, Err(Failure::Cancelled.into()), &cancel)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Failure>(),
            Some(Failure::Cancelled)
        ));
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Extraction {
    pub capture: Capture,
    pub original: Original,
    pub text_digest: Option<String>,
}

pub fn display_name(input: &str) -> String {
    let basename = input.rsplit(['/', '\\']).next().unwrap_or("");
    let name: String = basename
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        .take(120)
        .collect();
    let name = name.trim_matches([' ', '.']);
    if name.is_empty() {
        "document".into()
    } else {
        name.into()
    }
}

fn label(input: &str) -> Option<&str> {
    let start = input.find("charset")? + "charset".len();
    let tail = input[start..].trim_start().strip_prefix('=')?.trim_start();
    tail.trim_start_matches(['\'', '"'])
        .split(['\'', '"', ' ', ';', '>'])
        .next()
}

fn decode(bytes: &[u8], declared: Option<&str>, html: bool) -> Result<(String, String)> {
    let meta = if html {
        let prefix = String::from_utf8_lossy(&bytes[..bytes.len().min(1024)]).to_ascii_lowercase();
        prefix
            .split("<meta")
            .skip(1)
            .find_map(|tag| label(tag.split('>').next()?).map(str::to_owned))
    } else {
        None
    };
    let (encoding, skip) = if let Some((encoding, skip)) = encoding_rs::Encoding::for_bom(bytes) {
        (encoding, skip)
    } else {
        let encoding = match declared.or(meta.as_deref()) {
            Some(label) => encoding_rs::Encoding::for_label(label.as_bytes())
                .ok_or_else(|| anyhow::anyhow!("unknown text encoding"))?,
            None => encoding_rs::UTF_8,
        };
        (encoding, 0)
    };
    let (text, errors) = encoding.decode_without_bom_handling(&bytes[skip..]);
    ensure!(
        !errors && !text.contains('\0'),
        "invalid text encoding or binary text"
    );
    Ok((
        text.replace("\r\n", "\n").replace('\r', "\n"),
        encoding.name().into(),
    ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum HtmlRejection {
    Verification,
    Login,
    BrowserError,
}
impl std::fmt::Display for HtmlRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HTML is an access or browser interstitial")
    }
}
impl std::error::Error for HtmlRejection {}

fn html_content(html: String, url: Option<&str>) -> Result<(Content, bool, String)> {
    let mut reader = dom_smoothie::Readability::new(
        html,
        url,
        Some(dom_smoothie::Config {
            max_elements_to_parse: 50000,
            ..Default::default()
        }),
    )?;
    ensure!(
        reader.doc.select("*").length() <= 50000,
        "HTML element limit exceeded"
    );
    let mut pending = vec![(reader.doc.root(), 0usize)];
    while let Some((node, depth)) = pending.pop() {
        ensure!(depth <= 128, "HTML depth limit exceeded");
        pending.extend(node.children_it(false).map(|child| (child, depth + 1)));
    }
    if reader
        .doc
        .select("#main-frame-error, body.neterror")
        .length()
        > 0
    {
        return Err(HtmlRejection::BrowserError.into());
    }
    let challenge = reader
        .doc
        .select("#challenge-form, #cf-challenge-running, #cf-error-details, #px-captcha")
        .length()
        > 0;
    let password = reader.doc.select("input[type='password']").length() > 0;
    reader
        .doc
        .select("script, style, noscript, iframe")
        .remove();
    let visible = reader.doc.select("body").text().to_string();
    let heading = format!(
        "{} {}",
        reader.get_article_title(),
        reader.doc.select("h1").text()
    )
    .to_lowercase();
    if visible.len() < 4000 && reader.doc.select("article").text().len() < 600 {
        if challenge
            || [
                "checking your browser",
                "just a moment",
                "verify you are human",
                "security check",
                "access denied",
                "attention required",
            ]
            .iter()
            .any(|phrase| heading.contains(phrase))
        {
            return Err(HtmlRejection::Verification.into());
        }
        if password
            && ["sign in", "log in", "login"]
                .iter()
                .any(|phrase| heading.contains(phrase))
        {
            return Err(HtmlRejection::Login.into());
        }
    }
    let converter = htmd::HtmlToMarkdown::builder()
        .skip_tags(vec!["script", "style", "noscript", "iframe", "form"])
        .build();
    // ThreadReader's explicit thread container avoids unrelated recommendations and controls.
    let thread = reader.doc.select(".threadreader-content");
    if url
        .and_then(|s| url::Url::parse(s).ok())
        .and_then(|u| u.host_str().map(str::to_owned))
        .is_some_and(|h| h == "threadreaderapp.com" || h == "www.threadreaderapp.com")
        && !thread.text().trim().is_empty()
    {
        let body = converter.convert(&thread.html())?;
        return Ok((
            Content {
                body,
                metadata: ContentMetadata {
                    title: Some(reader.get_article_title().to_string()),
                    ..Default::default()
                },
            },
            false,
            "threadreader-container+htmd-0.5.6".into(),
        ));
    }
    reader
        .doc
        .select("script, style, noscript, iframe, form, nav, footer")
        .remove();
    let fallback_html = reader.doc.select("body").html().to_string();
    if let Ok(article) = reader.parse() {
        let body = converter.convert(&article.content)?;
        if !body.trim().is_empty() {
            return Ok((
                Content {
                    body,
                    metadata: ContentMetadata {
                        title: Some(article.title),
                        author: article.byline,
                        authored_at: article.published_time,
                        language: article.lang,
                    },
                },
                false,
                "dom_smoothie-0.18.2+htmd-0.5.6".into(),
            ));
        }
    }
    let body = converter.convert(&fallback_html)?;
    ensure!(!body.trim().is_empty(), "no readable HTML text");
    Ok((
        Content {
            body,
            metadata: ContentMetadata::default(),
        },
        true,
        "whole-body-fallback+htmd-0.5.6".into(),
    ))
}

pub struct Documents {
    pub store: Store,
    pub config: Config,
    pub http: PublicHttp,
}
impl Documents {
    pub async fn url(&self, url: &str, cancel: &CancellationToken) -> Result<Extraction> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.limits.download_seconds);
        let response = self
            .http
            .get(
                url,
                self.config.limits.input_bytes,
                Duration::from_secs(self.config.limits.download_seconds),
                None,
                cancel,
            )
            .await?;
        let html = matches!(
            response.content_type.as_str(),
            "text/html" | "application/xhtml+xml"
        );
        let initial = self.response(response, cancel).await?;
        if !self.config.sources.browser_enabled
            || !html
            || initial.capture.status == FetchStatus::Complete
        {
            return Ok(initial);
        }
        let rendered = crate::browser::render(
            &self.store,
            &self.config,
            url,
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            cancel,
        )
        .await;
        self.finish_browser_attempt(url, initial, rendered, cancel)
            .await
    }

    async fn finish_browser_attempt(
        &self,
        url: &str,
        mut initial: Extraction,
        rendered: Result<(Vec<u8>, serde_json::Value)>,
        cancel: &CancellationToken,
    ) -> Result<Extraction> {
        match rendered {
            Ok((bytes, evidence)) => {
                let mut result = self
                    .extract(
                        Identity::from_url(url)?,
                        Some(url),
                        None,
                        "rendered.html",
                        "text/html",
                        Some("utf-8"),
                        &bytes,
                        None,
                        cancel,
                    )
                    .await?;
                result.capture.evidence["browser"] = evidence;
                result.capture.evidence["initial_fetch"] = initial.capture.evidence.clone();
                if result.capture.content.is_some() {
                    result.capture.extractor = format!("browser+{}", result.capture.extractor);
                    return Ok(result);
                }
                initial.capture.evidence["browser_attempt"] = result.capture.evidence;
            }
            Err(error) => {
                if cancel.is_cancelled() {
                    return Err(Failure::Cancelled.into());
                }
                initial.capture.evidence["browser_attempt"] = json!({"status":"failed",
                    "tool_failure":error.downcast_ref::<crate::process::ToolError>().map(|e|e.kind),
                    "failure":error.downcast_ref::<Failure>()});
            }
        }
        Ok(initial)
    }

    pub async fn response(
        &self,
        response: Response,
        cancel: &CancellationToken,
    ) -> Result<Extraction> {
        let identity = Identity::from_url(response.submitted.as_str())?;
        let name = response
            .resolved
            .path_segments()
            .and_then(|mut p| p.next_back())
            .unwrap_or("document");
        let mut result = self
            .extract(
                identity,
                Some(response.submitted.as_str()),
                Some(response.resolved.as_str()),
                name,
                &response.content_type,
                response.charset.as_deref(),
                &response.bytes,
                Some(response.status),
                cancel,
            )
            .await?;
        result.capture.evidence["redirects"] = json!(response.redirects);
        Ok(result)
    }

    pub async fn attachment(
        &self,
        unique_id: &str,
        name: &str,
        bytes: &[u8],
        cancel: &CancellationToken,
    ) -> Result<Extraction> {
        ensure!(
            !unique_id.is_empty() && unique_id.len() <= 256,
            "invalid document identity"
        );
        let mime = if name.to_ascii_lowercase().ends_with(".pdf") {
            "application/pdf"
        } else {
            "text/plain"
        };
        self.extract(
            Identity {
                provider: "telegram_document".into(),
                external_id: unique_id.into(),
            },
            None,
            None,
            name,
            mime,
            None,
            bytes,
            None,
            cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn extract(
        &self,
        identity: Identity,
        submitted: Option<&str>,
        resolved: Option<&str>,
        name: &str,
        mime: &str,
        charset: Option<&str>,
        bytes: &[u8],
        http_status: Option<u16>,
        cancel: &CancellationToken,
    ) -> Result<Extraction> {
        ensure!(
            bytes.len() as u64 <= self.config.limits.input_bytes,
            "document byte limit exceeded"
        );
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled.into());
        }
        let raw = self.store.publish_bytes(bytes)?.digest;
        let original = Original {
            digest: raw.clone(),
            mime: mime.into(),
            display_name: display_name(name),
            validated: false,
        };
        let mut result = Extraction {
            capture: Capture {
                identity,
                submitted_url: submitted.map(str::to_owned),
                content: None,
                status: FetchStatus::Error,
                extractor: "document-v1".into(),
                evidence: json!({"submitted_url":submitted,"resolved_url":resolved,"http_status":http_status,"retrieved_at":now(),"original":original,"text_status":"unavailable"}),
                raw_digest: Some(raw),
                relations: vec![],
            },
            original,
            text_digest: None,
        };
        if let Some(status) = http_status
            && !(200..300).contains(&status)
        {
            result.capture.status = match status {
                401 | 403 => FetchStatus::Forbidden,
                404 | 410 => FetchStatus::Missing,
                _ => FetchStatus::Error,
            };
            return Ok(result);
        }
        if mime == "application/pdf"
            || (mime == "application/octet-stream" && bytes.starts_with(b"%PDF-"))
        {
            self.pdf(&mut result, bytes, cancel).await?;
        } else if matches!(
            mime,
            "text/html"
                | "application/xhtml+xml"
                | "text/plain"
                | "text/markdown"
                | "text/x-markdown"
        ) || (mime.is_empty() && std::str::from_utf8(bytes).is_ok())
        {
            let html = matches!(mime, "text/html" | "application/xhtml+xml");
            if let Ok((text, encoding)) = decode(bytes, charset, html) {
                result.capture.evidence["encoding"] = json!(encoding);
                let extracted = if html {
                    let url = resolved.map(str::to_owned);
                    tokio::task::spawn_blocking(move || html_content(text, url.as_deref())).await?
                } else {
                    Ok((
                        Content {
                            body: text,
                            metadata: ContentMetadata::default(),
                        },
                        false,
                        "text-decoder-v1".into(),
                    ))
                };
                match extracted {
                    Ok((content, partial, extractor))
                        if !content.body.trim().is_empty()
                            && content.body.len() as u64 <= self.config.limits.input_bytes =>
                    {
                        result.original.validated = true;
                        result.capture.content = Some(content);
                        result.capture.extractor = extractor;
                        result.capture.status = if partial {
                            FetchStatus::Partial
                        } else {
                            FetchStatus::Complete
                        };
                    }
                    Err(error) => {
                        if let Some(reason) = error.downcast_ref::<HtmlRejection>() {
                            result.capture.status = if matches!(reason, HtmlRejection::BrowserError)
                            {
                                FetchStatus::Error
                            } else {
                                FetchStatus::Forbidden
                            };
                            result.capture.evidence["failure"] = json!(reason);
                        }
                    }
                    _ => {}
                }
            } else {
                result.capture.evidence["failure"] = json!("invalid_or_unsupported_encoding");
            }
        } else {
            result.capture.evidence["failure"] = json!("unsupported_content_type");
        }
        if let Some(content) = &result.capture.content {
            result.text_digest = Some(self.store.publish_bytes(content.body.as_bytes())?.digest);
            result.capture.evidence["text_status"] = json!(result.capture.status);
        }
        result.capture.evidence["original"] = json!(result.original);
        result.capture.evidence["text_digest"] = json!(result.text_digest);
        Ok(result)
    }

    async fn pdf(
        &self,
        result: &mut Extraction,
        bytes: &[u8],
        cancel: &CancellationToken,
    ) -> Result<()> {
        let work = self.store.workspace("pdf")?;
        std::fs::write(work.path().join("input.pdf"), bytes)?;
        let timeout = Duration::from_secs(self.config.limits.media_seconds);
        let deadline = tokio::time::Instant::now() + timeout;
        let tool = |executable, args: Vec<OsString>| Tool {
            executable,
            args,
            directory: work.path().to_owned(),
            environment: BTreeMap::new(),
            timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
            log_bytes: self.config.limits.log_bytes,
            stdout_bytes: None,
            work_bytes: self.config.limits.work_bytes,
            secrets: vec![],
        };
        let info = match tool(self.config.tools.pdfinfo.clone(), vec!["input.pdf".into()])
            .run(cancel)
            .await
        {
            Ok(info) => info,
            Err(error) => {
                if matches!(error.kind, ToolFailure::Cancelled | ToolFailure::Timeout) {
                    return Err(error.into());
                }
                result.capture.evidence["pdf_validation"] =
                    json!({"status":"failed","kind":error.kind,"tool":error.report});
                return Ok(());
            }
        };
        let pages = info
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("Pages:")?.trim().parse::<u32>().ok());
        if pages.is_none_or(|p| p == 0) {
            return Ok(());
        }
        result.original.validated = true;
        result.original.mime = "application/pdf".into();
        result.capture.evidence["pdf_validation"] =
            json!({"status":"validated","pages":pages,"tool":info});
        let extracted = tool(
            self.config.tools.pdftotext.clone(),
            vec![
                "-layout".into(),
                "-enc".into(),
                "UTF-8".into(),
                "input.pdf".into(),
                "text.txt".into(),
            ],
        )
        .run(cancel)
        .await;
        match extracted {
            Ok(report) => {
                let path = confined_file(work.path(), &work.path().join("text.txt"));
                if let Ok(path) = path
                    && path.metadata()?.len() <= self.config.limits.input_bytes
                {
                    let text = std::fs::read(path)?;
                    if let Ok((text, _)) = decode(&text, Some("utf-8"), false)
                        && !text.trim().is_empty()
                    {
                        result.capture.content = Some(Content {
                            body: text,
                            metadata: ContentMetadata::default(),
                        });
                        result.capture.status = if report.stderr.trim().is_empty() {
                            FetchStatus::Complete
                        } else {
                            FetchStatus::Partial
                        };
                    }
                }
                result.capture.evidence["pdf_text"] = json!({"tool":report});
            }
            Err(error) => {
                if matches!(error.kind, ToolFailure::Cancelled | ToolFailure::Timeout) {
                    return Err(error.into());
                }
                result.capture.evidence["pdf_text"] =
                    json!({"status":"failed","kind":error.kind,"tool":error.report});
            }
        }
        result.capture.extractor = "poppler-pdfinfo+pdftotext".into();
        Ok(())
    }
}
