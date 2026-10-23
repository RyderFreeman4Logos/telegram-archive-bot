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
        self.response(response, cancel).await
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
                response.status,
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
            200,
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
        http_status: u16,
        cancel: &CancellationToken,
    ) -> Result<Extraction> {
        ensure!(
            !bytes.is_empty() && bytes.len() as u64 <= self.config.limits.input_bytes,
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
        if !(200..300).contains(&http_status) {
            result.capture.status = match http_status {
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
                if let Ok((content, partial, extractor)) = extracted
                    && !content.body.trim().is_empty()
                    && content.body.len() as u64 <= self.config.limits.input_bytes
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
