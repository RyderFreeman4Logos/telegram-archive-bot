use anyhow::Result;
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{
    config::Config,
    documents::*,
    network::{PublicHttp, Response},
    sources::FetchStatus,
    storage::Store,
};
use tokio_util::sync::CancellationToken;

fn fixture() -> Result<(tempfile::TempDir, Documents)> {
    let dir = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()?;
    let adapter = Documents {
        store: Store::open(dir.path())?,
        config: Config::default(),
        http: PublicHttp::default(),
    };
    Ok((dir, adapter))
}
fn response(url: &str, mime: &str, bytes: &[u8]) -> Response {
    Response {
        submitted: url.parse().unwrap(),
        resolved: url.parse().unwrap(),
        redirects: vec![],
        status: 200,
        content_type: mime.into(),
        charset: None,
        bytes: bytes.to_vec(),
    }
}
fn pdf(text: &str) -> Vec<u8> {
    let stream = format!("BT /F1 12 Tf 40 100 Td ({text}) Tj ET\n");
    let objects = ["<< /Type /Catalog /Pages 2 0 R >>".into(), "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".into(), "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".into(), "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".into(), format!("<< /Length {} >>\nstream\n{stream}endstream", stream.len())];
    let mut output = String::from("%PDF-1.4\n");
    let mut offsets = vec![];
    for (i, object) in objects.iter().enumerate() {
        offsets.push(output.len());
        output.push_str(&format!("{} 0 obj\n{object}\nendobj\n", i + 1));
    }
    let xref = output.len();
    output.push_str("xref\n0 6\n0000000000 65535 f \n");
    for offset in offsets {
        output.push_str(&format!("{offset:010} 00000 n \n"));
    }
    output.push_str(&format!(
        "trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    ));
    output.into_bytes()
}

#[tokio::test]
async fn html_and_threadreader_keep_markdown_and_raw_provenance() -> Result<()> {
    let (_dir, adapter) = fixture()?;
    let prose = "An article explains a precise observation, with enough useful sentences to separate its body from navigation. ".repeat(15);
    let html = format!(
        "<html lang='en'><head><title>Fixture article</title></head><body><nav>irrelevant navigation</nav><article><h1>Fixture article</h1><p>{prose}</p><h2>Details</h2><ul><li>First fact</li><li>Second fact</li></ul><pre><code>let a = 1;</code></pre></article></body></html>"
    );
    let cancel = CancellationToken::new();
    let extracted = adapter
        .response(
            response(
                "https://example.org/?article=1",
                "text/html",
                html.as_bytes(),
            ),
            &cancel,
        )
        .await?;
    assert_eq!(extracted.capture.status, FetchStatus::Complete);
    let content = extracted.capture.content.as_ref().unwrap();
    assert!(
        content.body.contains("## Details")
            && content.body.contains("First fact")
            && content.body.contains("let a = 1;")
    );
    assert!(!content.body.contains("irrelevant navigation"));
    assert_eq!(
        adapter.store.read_blob(
            &extracted.original.digest,
            adapter.config.limits.input_bytes
        )?,
        html.as_bytes()
    );
    assert_ne!(
        extracted.text_digest.as_ref().unwrap(),
        &extracted.original.digest
    );
    adapter.store.capture(&extracted.capture)?;
    let thread = b"<html><title>A thread</title><body><div class='threadreader-content'><p>First post</p><p>Second <b>post</b></p></div><aside>unrelated</aside></body></html>";
    let extracted = adapter
        .response(
            response(
                "https://threadreaderapp.com/thread/123.html",
                "text/html",
                thread,
            ),
            &cancel,
        )
        .await?;
    assert!(
        extracted
            .capture
            .extractor
            .starts_with("threadreader-container")
    );
    assert_eq!(extracted.capture.status, FetchStatus::Complete);
    let body = &extracted.capture.content.unwrap().body;
    assert!(
        body.contains("First post") && body.contains("**post**") && !body.contains("unrelated")
    );
    Ok(())
}

#[tokio::test]
async fn verification_login_and_browser_error_pages_are_not_article_text() -> Result<()> {
    let (_dir, adapter) = fixture()?;
    for html in [
        "<html><title>Just a moment...</title><body><h1>Checking your browser before accessing the site</h1><form id='challenge-form'><div class='g-recaptcha'></div></form><p>Please wait while we verify your browser.</p></body></html>",
        "<html><title>Sign in</title><body><h1>Sign in to continue</h1><form><input type='password'></form></body></html>",
        "<html><body class='neterror'><div id='main-frame-error'><h1>This site cannot be reached</h1><p>ERR_CONNECTION_REFUSED</p></div></body></html>",
    ] {
        let result = adapter
            .response(
                response("https://example.org/article", "text/html", html.as_bytes()),
                &CancellationToken::new(),
            )
            .await?;
        assert!(
            result.capture.content.is_none(),
            "interstitial must not feed summary or speech"
        );
        assert!(matches!(
            result.capture.status,
            FetchStatus::Forbidden | FetchStatus::Error
        ));
        assert!(result.text_digest.is_none() && !result.original.validated);
        assert_eq!(
            adapter.store.read_blob(&result.original.digest, 10000)?,
            html.as_bytes()
        );
    }
    let prose = "This article explains checking your browser and how a sign in form differs from a captcha. ".repeat(30);
    let html = format!(
        "<html><title>Browser verification explained</title><body><article><h1>Browser verification explained</h1><p>{prose}</p></article><form><input type='password'></form></body></html>"
    );
    let result = adapter
        .response(
            response("https://example.org/guide", "text/html", html.as_bytes()),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(result.capture.status, FetchStatus::Complete);
    assert!(
        result
            .capture
            .content
            .unwrap()
            .body
            .contains(prose.trim_end())
    );
    Ok(())
}

#[tokio::test]
async fn unicode_attachments_encoding_isolation_and_bounds() -> Result<()> {
    let (_dir, mut adapter) = fixture()?;
    let cancel = CancellationToken::new();
    let markdown = "# 标题 🎧\n\n[链接](https://example.org/?a=1)\n\n```rust\nlet x = 1;\n```\n";
    let utf16: Vec<u8> = [
        vec![0xff, 0xfe],
        "第二份文本"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect(),
    ]
    .concat();
    let (a, b) = tokio::join!(
        adapter.attachment("first", "../../notes.md", markdown.as_bytes(), &cancel),
        adapter.attachment("second", "../../notes.md", &utf16, &cancel)
    );
    let a = a?;
    let b = b?;
    assert_eq!(a.capture.content.unwrap().body, markdown);
    assert_eq!(b.capture.content.unwrap().body, "第二份文本");
    assert_ne!(a.original.digest, b.original.digest);
    assert_eq!(a.original.display_name, "notes.md");
    assert_ne!(a.capture.identity.id(), b.capture.identity.id());
    assert_eq!(display_name("C:\\private\\bad\nname?.md"), "badname.md");
    assert_eq!(display_name("../.."), "document");
    let bad = adapter
        .attachment("bad", "bad.txt", &[0xff, 0, 0x80], &cancel)
        .await?;
    assert_eq!(bad.capture.status, FetchStatus::Error);
    assert!(!bad.original.validated && bad.text_digest.is_none());
    adapter.config.limits.input_bytes = 2;
    assert!(
        adapter
            .attachment("large", "large.txt", b"123", &cancel)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn empty_http_body_is_retained_without_a_text_revision() -> Result<()> {
    let (_dir, adapter) = fixture()?;
    let extracted = adapter
        .response(
            response("https://example.org/empty", "text/html", b""),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(extracted.capture.status, FetchStatus::Error);
    assert!(extracted.capture.content.is_none() && extracted.text_digest.is_none());
    assert!(!extracted.original.validated);
    assert_eq!(extracted.capture.evidence["http_status"], 200);
    assert!(
        adapter
            .store
            .read_blob(&extracted.original.digest, 1)?
            .is_empty()
    );
    let local = adapter
        .attachment(
            "local",
            "article.md",
            b"Local text",
            &CancellationToken::new(),
        )
        .await?;
    assert!(local.capture.evidence["http_status"].is_null());
    Ok(())
}

#[tokio::test]
async fn original_pdf_survives_failed_or_empty_text_extraction() -> Result<()> {
    let (_dir, mut adapter) = fixture()?;
    let cancel = CancellationToken::new();
    let bytes = pdf("Original PDF survives");
    let good = adapter
        .attachment("pdf-good", "file.pdf", &bytes, &cancel)
        .await?;
    assert!(good.original.validated);
    assert_eq!(good.capture.status, FetchStatus::Complete);
    assert!(
        good.capture
            .content
            .unwrap()
            .body
            .contains("Original PDF survives")
    );
    let other_bytes = pdf("Concurrent independent original");
    let (first, second) = tokio::join!(
        adapter.attachment("pdf-concurrent-1", "same.pdf", &bytes, &cancel),
        adapter.attachment("pdf-concurrent-2", "same.pdf", &other_bytes, &cancel)
    );
    assert!(
        first?
            .capture
            .content
            .unwrap()
            .body
            .contains("Original PDF survives")
    );
    assert!(
        second?
            .capture
            .content
            .unwrap()
            .body
            .contains("Concurrent independent original")
    );
    adapter.config.tools.pdftotext = "/usr/bin/false".into();
    let failed = adapter
        .attachment("pdf-failure", "file.pdf", &bytes, &cancel)
        .await?;
    assert!(failed.original.validated);
    assert_eq!(failed.capture.status, FetchStatus::Error);
    assert!(failed.text_digest.is_none() && failed.capture.content.is_none());
    assert_eq!(
        adapter
            .store
            .read_blob(&failed.original.digest, adapter.config.limits.input_bytes)?,
        bytes
    );
    adapter.store.capture(&failed.capture)?;
    assert!(
        adapter
            .store
            .current_revision(&failed.capture.identity)?
            .is_none()
    );
    adapter.config.tools.pdftotext = "/usr/bin/pdftotext".into();
    let empty = adapter
        .attachment("pdf-empty", "scan.pdf", &pdf(""), &cancel)
        .await?;
    assert!(empty.original.validated && empty.text_digest.is_none());
    let bad = adapter
        .attachment("pdf-bad", "invalid.pdf", b"%PDF-1.4\ninvalid", &cancel)
        .await?;
    assert!(!bad.original.validated && bad.capture.content.is_none());
    Ok(())
}

#[tokio::test]
async fn declared_html_encoding_and_http_errors_are_explicit() -> Result<()> {
    let (_dir, adapter) = fixture()?;
    let cancel = CancellationToken::new();
    let mut encoded =
        b"<html><head><meta charset='windows-1252'></head><body><article><p>Caf".to_vec();
    encoded.extend_from_slice(b"\xe9 and an observation.</p></article></body></html>");
    let result = adapter
        .response(
            response("https://example.org/latin", "text/html", &encoded),
            &cancel,
        )
        .await?;
    assert!(result.capture.content.unwrap().body.contains("Café"));
    assert_eq!(result.capture.evidence["encoding"], "windows-1252");
    let mut denied = response(
        "https://example.org/missing",
        "text/html",
        b"<h1>Not found</h1>",
    );
    denied.status = 404;
    let result = adapter.response(denied, &cancel).await?;
    assert_eq!(result.capture.status, FetchStatus::Missing);
    assert!(result.capture.content.is_none() && !result.original.validated);
    let deep = format!(
        "{}unsafe nesting{}",
        "<div>".repeat(200),
        "</div>".repeat(200)
    );
    let rejected = adapter
        .response(
            response("https://example.org/deep", "text/html", deep.as_bytes()),
            &cancel,
        )
        .await?;
    assert!(rejected.capture.content.is_none());
    let unsupported = adapter
        .response(
            response("https://example.org/image", "image/png", b"not text"),
            &cancel,
        )
        .await?;
    assert_eq!(unsupported.capture.status, FetchStatus::Error);
    Ok(())
}
