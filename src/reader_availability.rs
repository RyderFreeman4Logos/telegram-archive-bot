//! DA09 / #76. Indexed operational evidence supplements immutable selected pins.
use super::*;
use rusqlite::{OptionalExtension, Transaction, params};

/// One durable input cursor, at most 64 dependent scopes per reader tick.
pub(super) fn admit(tx: &Transaction<'_>) -> Result<usize> {
    let input: Option<(String, String, String)> = tx
        .query_row(
            "SELECT kind,entity,cursor FROM reader_dirty_inputs ORDER BY rowid LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((kind, entity, cursor)) = input else {
        return Ok(0);
    };
    // An activity can be rediscovered in a new window before that window has a head.
    // Its reverse mapping, rather than existing head dependencies, is authoritative.
    let scopes = if kind == "activity" {
        tx.prepare("SELECT window_id FROM reader_activity_windows WHERE activity_id=?1 AND window_id>?2 ORDER BY window_id LIMIT 64")?.query_map(params![entity,cursor],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        tx.prepare("SELECT scope FROM reader_dependencies WHERE kind=?1 AND entity=?2 AND scope>?3 ORDER BY scope LIMIT 64")?.query_map(params![kind,entity,cursor],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for scope in &scopes {
        if kind == "activity" {
            tx.execute("INSERT INTO reader_dirty SELECT 'window:'||id,id,local_date,timezone,NULL FROM daily_windows WHERE id=?1 ON CONFLICT(scope) DO UPDATE SET error=NULL",[scope])?;
        } else {
            tx.execute("INSERT INTO reader_dirty SELECT scope,window_id,local_date,timezone,NULL FROM reader_heads WHERE scope=?1 ON CONFLICT(scope) DO UPDATE SET error=NULL",[scope])?;
        }
    }
    if scopes.len() == 64 {
        tx.execute(
            "UPDATE reader_dirty_inputs SET cursor=?3 WHERE kind=?1 AND entity=?2",
            params![kind, entity, scopes.last()],
        )?;
    } else {
        tx.execute(
            "DELETE FROM reader_dirty_inputs WHERE kind=?1 AND entity=?2",
            params![kind, entity],
        )?;
    }
    Ok(scopes.len())
}

pub(super) fn reports(tx: &Transaction<'_>, scope: &str, stream: &str) -> Result<Vec<String>> {
    let key = digest(stream);
    tx.execute(
        "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,'report_stream',?2)",
        params![scope, key],
    )?;
    let fingerprints=tx.prepare("SELECT fingerprint FROM failure_streams WHERE stream_key=?1 ORDER BY fingerprint LIMIT 65")?.query_map([key],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        fingerprints.len() <= 64,
        "reader report fingerprints exceed bound"
    );
    let mut urls = Vec::new();
    for fp in fingerprints {
        tx.execute(
            "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,'report',?2)",
            params![scope, fp],
        )?;
        let rows=tx.prepare("SELECT issue_url FROM failure_reports WHERE fingerprint=?1 AND notified_sequence>0 AND issue_url IS NOT NULL ORDER BY repository LIMIT 65")?.query_map([fp],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(rows.len() <= 64, "reader report repositories exceed bound");
        for url in rows {
            crate::network::source_url(&url)?;
            urls.push(url);
        }
    }
    urls.sort();
    urls.dedup();
    ensure!(urls.len() <= 64, "reader report references exceed bound");
    Ok(urls)
}

fn category(value: &str) -> String {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        value.into()
    } else {
        "unclassified_capture_limitation".into()
    }
}

fn pdf(
    tx: &Transaction<'_>,
    config: &Config,
    scope: &str,
    source: &str,
    revision: Option<&str>,
    observation: i64,
    evidence: &Value,
) -> Result<Option<Attachment>> {
    if evidence["original"]["mime"] != "application/pdf" {
        return Ok(None);
    }
    tx.execute(
        "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,'pdf_observation',?2)",
        params![scope, observation.to_string()],
    )?;
    type Row = (
        Option<String>,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row:Option<Row>=tx.query_row("SELECT CASE WHEN length(CAST(q.record AS BLOB))<=?2 THEN q.record END,p.upload_state,p.attachment_state,p.verified_sha256,q.page_id,p.block_id,p.reason FROM notion_queue q JOIN notion_pdf p ON p.queue_id=q.id WHERE q.kind='pdf_attachment' AND q.observation_id=?1 ORDER BY q.id LIMIT 1",params![observation,config.notion.record_bytes],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    if let Some((raw, upload, attachment, verified, page, block, gap)) = row {
        let job: crate::notion_pdf::PdfJob = serde_json::from_str(
            &raw.ok_or_else(|| anyhow::anyhow!("reader PDF record exceeds bound"))?,
        )?;
        ensure!(
            job.original.source_id == source
                && job.original.revision_id.as_deref() == revision
                && job.original.observation_id == observation,
            "reader PDF pin mismatch"
        );
        let proven = attachment == "verified"
            && job.original.validated
            && verified.as_deref() == Some(&job.original.sha256);
        let gap = if proven {
            None
        } else {
            Some(gap.unwrap_or_else(|| "pdf_attachment_unverified".into()))
        };
        return Ok(Some(Attachment {
            original_url: job.original.canonical_url,
            landing_url: job.original.landing_urls.first().cloned(),
            direct_url: job.original.direct_url,
            upload_state: upload,
            attachment_state: attachment,
            verified_sha256: proven.then_some(job.original.sha256),
            page_id: page,
            block_id: block,
            gap,
        }));
    }
    let (provider, external): (String, String) = tx.query_row(
        "SELECT provider,external_id FROM sources WHERE id=?1",
        [source],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(Some(Attachment {
        original_url: crate::sources::Identity {
            provider,
            external_id: external,
        }
        .url(),
        landing_url: None,
        direct_url: evidence["resolved_url"].as_str().map(str::to_owned),
        upload_state: "pending".into(),
        attachment_state: "pending".into(),
        verified_sha256: None,
        page_id: None,
        block_id: None,
        gap: Some("pdf_attachment_pending".into()),
    }))
}

pub(super) fn source(
    tx: &Transaction<'_>,
    config: &Config,
    scope: &str,
    source: &str,
    revision: Option<&str>,
    observation: Option<i64>,
    prior: Option<SourceEnrichment>,
) -> Result<Option<SourceEnrichment>> {
    tx.execute(
        "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,'capture',?2)",
        params![scope, source],
    )?;
    let mut value = prior.unwrap_or(SourceEnrichment {
        source_id: source.into(),
        revision_id: revision.map(str::to_owned),
        attachments: vec![],
        gaps: vec![],
        failure_report_urls: vec![],
        capture: None,
    });
    value
        .failure_report_urls
        .extend(reports(tx, scope, &format!("capture:{source}"))?);
    if let Some(revision) = revision {
        value.failure_report_urls.extend(reports(
            tx,
            scope,
            &format!("notion:revision:{revision}"),
        )?);
    }
    if revision.is_none() {
        value.capture = tx
            .query_row(
                "SELECT state,reason,cycle,attempts FROM daily_capture_work WHERE source_id=?1",
                [source],
                |r| {
                    Ok(CaptureAvailability {
                        state: r.get(0)?,
                        reason: r.get::<_, Option<String>>(1)?.map(|s| category(&s)),
                        cycle: r.get(2)?,
                        attempts: r.get(3)?,
                    })
                },
            )
            .optional()?;
    }
    if let Some(observation) = observation {
        let (raw,observed_revision):(Option<String>,Option<String>) = tx.query_row("SELECT CASE WHEN length(CAST(evidence AS BLOB))<=?3 THEN evidence END,revision_id FROM observations WHERE id=?1 AND source_id=?2",params![observation,source,config.notion.record_bytes],|r|Ok((r.get(0)?,r.get(1)?)))?;
        let evidence: Value = serde_json::from_str(
            &raw.ok_or_else(|| anyhow::anyhow!("reader source evidence exceeds bound"))?,
        )?;
        if let Some(limits) = evidence["limitations"].as_array() {
            ensure!(limits.len() <= 256, "reader source gaps exceed bound");
            value.gaps.extend(limits.iter().map(|v| {
                v.as_str()
                    .map(category)
                    .unwrap_or_else(|| "unclassified_capture_limitation".into())
            }));
        }
        for field in ["limitation", "media_text_status"] {
            if let Some(gap) = evidence[field].as_str() {
                value.gaps.push(category(gap));
            }
        }
        if observed_revision.as_deref() == revision {
            if let Some(attachment) =
                pdf(tx, config, scope, source, revision, observation, &evidence)?
            {
                value.attachments.push(attachment);
            }
            let streams=tx.prepare("SELECT id FROM notion_queue WHERE kind='pdf_attachment' AND observation_id=?1 ORDER BY id LIMIT 65")?.query_map([observation],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            ensure!(streams.len() <= 64, "reader PDF jobs exceed bound");
            for stream in streams {
                tx.execute(
                    "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,'queue',?2)",
                    params![scope, stream],
                )?;
                value
                    .failure_report_urls
                    .extend(reports(tx, scope, &format!("pdf:{stream}"))?);
            }
        }
    }
    value.gaps.sort();
    value.gaps.dedup();
    value.failure_report_urls.sort();
    value.failure_report_urls.dedup();
    value
        .attachments
        .sort_by_cached_key(|a| serde_json::to_string(a).unwrap_or_default());
    value.attachments.dedup();
    ensure!(
        value.gaps.len() <= 256
            && value.attachments.len() <= 256
            && value.failure_report_urls.len() <= 64,
        "reader source enrichment exceeds bound"
    );
    Ok((value.capture.is_some()
        || !value.gaps.is_empty()
        || !value.attachments.is_empty()
        || !value.failure_report_urls.is_empty())
    .then_some(value))
}
