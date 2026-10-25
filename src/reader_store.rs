use super::*;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;

fn dirty(tx: &Transaction<'_>, window: &str) -> Result<()> {
    tx.execute("INSERT OR IGNORE INTO reader_dirty SELECT 'window:'||id,id,local_date,timezone,NULL FROM daily_windows WHERE id=?1",[window])?;
    Ok(())
}
fn depend(tx: &Transaction<'_>, scope: &str, kind: &str, entity: &str) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO reader_dependencies VALUES(?1,?2,?3)",
        params![scope, kind, entity],
    )?;
    Ok(())
}
pub(super) fn read_document(conn: &Connection, id: &str, bytes: usize) -> Result<Record> {
    let size: i64 = conn.query_row(
        "SELECT length(CAST(record AS BLOB)) FROM notion_documents WHERE id=?1",
        [id],
        |r| r.get(0),
    )?;
    ensure!(
        size >= 0 && size as usize <= bytes,
        "reader document exceeds bound"
    );
    Ok(serde_json::from_str(&conn.query_row::<String, _, _>(
        "SELECT record FROM notion_documents WHERE id=?1",
        [id],
        |r| r.get(0),
    )?)?)
}
pub(super) fn manifest(conn: &Connection, id: &str, bytes: usize) -> Result<Manifest> {
    let record = read_document(conn, id, bytes)?;
    let manifest: Manifest = serde_json::from_str(&record.content)?;
    ensure!(manifest.contract == CONTRACT, "not a reader day manifest");
    Ok(manifest)
}
fn reference(
    conn: &Connection,
    kind: &str,
    entity: &str,
    record_kind: &str,
    record_id: &str,
) -> Result<DocumentRef> {
    let key = digest(serde_json::to_vec(&json!([1, record_kind, record_id]))?);
    type PublicationRow = (String, Option<String>, Option<String>, Option<String>);
    let row: Option<PublicationRow> = conn
        .query_row(
            "SELECT state,page_id,receipt,expected_digest FROM notion_queue WHERE id=?1",
            [format!("{kind}:{entity}")],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let mut page_id = None;
    let mut verified = false;
    if let Some((state, page, receipt, expected)) = row {
        if let Some(receipt) =
            receipt.and_then(|v| serde_json::from_str::<crate::notion::VerifiedPage>(&v).ok())
        {
            verified = state == "complete"
                && receipt.publication_key == key
                && Some(&receipt.page_id) == page.as_ref()
                && Some(&receipt.canonical_digest) == expected.as_ref();
        }
        // Only verified identities become reader navigation links.
        if verified {
            page_id = page;
        }
    }
    Ok(DocumentRef {
        document_id: key.clone(),
        record_id: record_id.into(),
        lookup_title: format!("archive-v1/{record_kind}/{key}"),
        page_url: page_id
            .as_ref()
            .map(|id| format!("https://www.notion.so/{}", id.replace('-', ""))),
        page_id,
        verified,
    })
}
pub(super) fn document_reference(
    conn: &Connection,
    key: &str,
    record: &Record,
) -> Result<DocumentRef> {
    reference(conn, "document", key, record.data.kind(), &record.id)
}
fn save(
    tx: &Transaction<'_>,
    config: &Config,
    parent: &Manifest,
    payload: &impl Serialize,
) -> Result<DocumentRef> {
    let content = serde_json::to_string(payload)?;
    let record = document(parent, digest(&content), content);
    record.plan(&config.notion)?;
    let key = crate::notion_records::save_document(tx, &record)?;
    document_reference(tx, &key, &record)
}
fn bootstrap(tx: &Transaction<'_>, config: &Config) -> Result<usize> {
    let inputs = super::availability::admit(tx)?;
    let settings = digest(serde_json::to_vec(&json!([
        config.reader,
        config.notion.record_bytes,
        config.notion.max_blocks,
        config.limits.input_bytes,
        config.limits.graph_nodes
    ]))?);
    let previous: String = tx.query_row("SELECT digest FROM reader_settings", [], |r| r.get(0))?;
    if previous != settings {
        tx.execute("UPDATE reader_settings SET digest=?1", [settings])?;
        tx.execute(
            "UPDATE reader_bootstrap SET cursor='',done=0 WHERE name='windows'",
            [],
        )?;
        tx.execute("UPDATE reader_dirty SET error=NULL", [])?;
    }
    let mut count = 0;
    let (cursor, done): (String, bool) = tx.query_row(
        "SELECT cursor,done FROM reader_bootstrap WHERE name='windows'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if !done {
        let rows = tx
            .prepare("SELECT id FROM daily_windows WHERE id>?1 ORDER BY id LIMIT 64")?
            .query_map([&cursor], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for id in &rows {
            dirty(tx, id)?;
        }
        tx.execute(
            "UPDATE reader_bootstrap SET cursor=?1,done=?2 WHERE name='windows'",
            params![rows.last().unwrap_or(&cursor), rows.len() < 64],
        )?;
        count += rows.len();
    }
    let (cursor, done): (String, bool) = tx.query_row(
        "SELECT cursor,done FROM reader_bootstrap WHERE name='activities'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if !done {
        let rows=tx.prepare("SELECT rowid,activity_id,json_extract(manifest,'$.discovery.provenance.window_id') FROM activity_observations WHERE rowid>CAST(?1 AS INTEGER) ORDER BY rowid LIMIT 64")?.query_map([&cursor],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for (_, activity, window) in &rows {
            if let Some(window) = window {
                tx.execute("INSERT OR IGNORE INTO reader_activity_windows SELECT id,?2 FROM daily_windows WHERE id=?1",params![window,activity])?;
                dirty(tx, window)?;
            }
        }
        tx.execute(
            "UPDATE reader_bootstrap SET cursor=?1,done=?2 WHERE name='activities'",
            params![
                rows.last().map(|r| r.0.to_string()).unwrap_or(cursor),
                rows.len() < 64
            ],
        )?;
        if rows.len() < 64 {
            tx.execute(
                "UPDATE reader_bootstrap SET cursor='',done=0 WHERE name='refresh'",
                [],
            )?;
        }
        count += rows.len();
    }
    // Earlier heads built during historical admission must be revisited, boundedly.
    let (cursor, done): (String, bool) = tx.query_row(
        "SELECT cursor,done FROM reader_bootstrap WHERE name='refresh'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if !done {
        let rows = tx
            .prepare("SELECT id FROM daily_windows WHERE id>?1 ORDER BY id LIMIT 64")?
            .query_map([&cursor], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for id in &rows {
            dirty(tx, id)?;
        }
        tx.execute(
            "UPDATE reader_bootstrap SET cursor=?1,done=?2 WHERE name='refresh'",
            params![rows.last().unwrap_or(&cursor), rows.len() < 64],
        )?;
    }
    Ok(count + inputs)
}

fn entry(
    tx: &Transaction<'_>,
    config: &Config,
    scope: &str,
    window: &str,
    id: &str,
    text_hashes: &mut BTreeMap<String, String>,
) -> Result<ActivityEntry> {
    let (observation_id,size):(String,i64)=tx.query_row("SELECT id,length(CAST(manifest AS BLOB)) FROM activity_observations WHERE activity_id=?1 AND json_extract(manifest,'$.discovery.provenance.window_id')=?2 ORDER BY json_extract(manifest,'$.retrieved_at') DESC,source_observation_id DESC,id DESC LIMIT 1",params![id,window],|r|Ok((r.get(0)?,r.get(1)?)))?;
    ensure!(
        size >= 0 && size as usize <= config.notion.record_bytes,
        "reader activity exceeds bound"
    );
    let raw: String = tx.query_row(
        "SELECT manifest FROM activity_observations WHERE id=?1",
        [observation_id],
        |r| r.get(0),
    )?;
    let observation: crate::activities::ActivityObservation = serde_json::from_str(&raw)?;
    let view_size: i64 = tx.query_row(
        "SELECT length(CAST(manifest AS BLOB)) FROM views WHERE id=?1",
        [&observation.view_id],
        |r| r.get(0),
    )?;
    ensure!(
        view_size >= 0 && view_size as usize <= config.notion.record_bytes,
        "reader view exceeds bound"
    );
    let view_raw: String = tx.query_row(
        "SELECT manifest FROM views WHERE id=?1",
        [&observation.view_id],
        |r| r.get(0),
    )?;
    ensure!(
        view_raw.len() <= config.notion.record_bytes,
        "reader view exceeds bound"
    );
    let view: crate::sources::View = serde_json::from_str(&view_raw)?;
    ensure!(
        view.members.len() <= config.limits.graph_nodes,
        "reader view exceeds node bound"
    );
    let mut sources = vec![];
    for member in &view.members {
        depend(tx, scope, "source", &member.source_id)?;
        let (provider, external): (String, String) = tx.query_row(
            "SELECT provider,external_id FROM sources WHERE id=?1",
            [&member.source_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let identity = crate::sources::Identity {
            provider,
            external_id: external,
        };
        let mut urls = tx
            .prepare("SELECT url FROM source_aliases WHERE source_id=?1 ORDER BY url LIMIT 257")?
            .query_map([&member.source_id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(urls.len() <= 256, "reader original links exceed bound");
        let canonical = identity.url();
        if crate::network::source_url(&canonical).is_ok() && !urls.contains(&canonical) {
            urls.push(canonical);
        }
        let (text_sha256, publication) = if let Some(revision) = &member.revision_id {
            let hash = if let Some(hash) = text_hashes.get(revision) {
                hash.clone()
            } else {
                let bytes:i64=tx.query_row("SELECT length(CAST(body AS BLOB)) FROM source_revisions WHERE id=?1 AND source_id=?2",params![revision,member.source_id],|r|r.get(0))?;
                ensure!(
                    bytes >= 0 && bytes as u64 <= config.limits.input_bytes,
                    "reader source exceeds byte bound"
                );
                let body: String = tx.query_row(
                    "SELECT body FROM source_revisions WHERE id=?1 AND source_id=?2",
                    params![revision, member.source_id],
                    |r| r.get(0),
                )?;
                let hash = digest(body);
                text_hashes.insert(revision.clone(), hash.clone());
                hash
            };
            depend(tx, scope, "queue", &format!("revision:{revision}"))?;
            (
                Some(hash),
                Some(reference(tx, "revision", revision, "revision", revision)?),
            )
        } else {
            (None, None)
        };
        let enrichment: Option<String> = tx
            .query_row(
                "SELECT payload FROM reader_enrichments WHERE source_id=?1",
                [&member.source_id],
                |r| r.get(0),
            )
            .optional()?;
        let enrichment = enrichment
            .map(|v| serde_json::from_str::<SourceEnrichment>(&v))
            .transpose()?
            .filter(|v| v.revision_id.is_none() || v.revision_id == member.revision_id);
        let source_observation = if member.source_id == view.root_id {
            Some(observation.source_observation_id)
        } else {
            view.retrieval
                .as_ref()
                .and_then(|v| v["contexts"].as_array())
                .and_then(|rows| {
                    rows.iter().find(|r| {
                        serde_json::from_value::<crate::sources::Identity>(r["identity"].clone())
                            .ok()
                            .as_ref()
                            == Some(&identity)
                            && r["revision_id"].as_str() == member.revision_id.as_deref()
                    })
                })
                .and_then(|r| r["observation_id"].as_i64())
        };
        let enrichment = super::availability::source(
            tx,
            config,
            scope,
            &member.source_id,
            member.revision_id.as_deref(),
            source_observation,
            enrichment,
        )?;
        let mut enrichment = enrichment;
        if member.source_id == view.root_id {
            let reports = super::availability::reports(
                tx,
                scope,
                &format!("notion:activity:{}", observation.id),
            )?;
            if !reports.is_empty() {
                let value = enrichment.get_or_insert_with(|| SourceEnrichment {
                    source_id: member.source_id.clone(),
                    revision_id: member.revision_id.clone(),
                    attachments: vec![],
                    gaps: vec![],
                    failure_report_urls: vec![],
                    capture: None,
                });
                value.failure_report_urls.extend(reports);
                value.failure_report_urls.sort();
                value.failure_report_urls.dedup();
            }
        }
        let capture_status = if member.source_id == view.root_id {
            observation.fetch_status
        } else if let Some(revision) = &member.revision_id {
            let complete:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM observations WHERE source_id=?1 AND revision_id=?2 AND status='complete')",params![member.source_id,revision],|r|r.get(0))?;
            if complete {
                crate::sources::FetchStatus::Complete
            } else {
                crate::sources::FetchStatus::Partial
            }
        } else {
            crate::sources::FetchStatus::Missing
        };
        sources.push(SourceRef {
            source_id: member.source_id.clone(),
            revision_id: member.revision_id.clone(),
            original_urls: urls,
            capture_status,
            role: if member.source_id == view.root_id {
                "activity_source"
            } else {
                "context_only"
            }
            .into(),
            missing_reason: member.missing_reason.clone(),
            text_sha256,
            publication,
            enrichment,
        });
    }
    let disposition = match observation.fetch_status {
        crate::sources::FetchStatus::Complete
            if sources.iter().all(|s| {
                s.revision_id.is_some()
                    && s.capture_status == crate::sources::FetchStatus::Complete
                    && s.enrichment.as_ref().is_none_or(|e| {
                        e.gaps.is_empty() && e.attachments.iter().all(|a| a.gap.is_none())
                    })
            }) && view.omissions.is_empty() =>
        {
            "captured"
        }
        crate::sources::FetchStatus::Complete | crate::sources::FetchStatus::Partial => "partial",
        crate::sources::FetchStatus::Error => "failed",
        _ => {
            let evidence: String = tx.query_row(
                "SELECT evidence FROM observations WHERE id=?1",
                [observation.source_observation_id],
                |r| r.get(0),
            )?;
            if sources
                .iter()
                .find(|s| s.source_id == view.root_id)
                .and_then(|s| s.enrichment.as_ref())
                .and_then(|e| e.capture.as_ref())
                .is_some_and(|c| c.state == "failed")
            {
                "failed"
            } else if serde_json::from_str::<Value>(&evidence)?["reason"]
                == "awaiting_body_hydration"
            {
                "pending"
            } else {
                "unavailable"
            }
        }
    }
    .into();
    depend(tx, scope, "queue", &format!("activity:{}", observation.id))?;
    Ok(ActivityEntry {
        activity_id: id.into(),
        observation_id: observation.id.clone(),
        window_ids: vec![window.into()],
        disposition,
        observation,
        sources,
        context_omissions: view.omissions,
    })
}

fn index_pages(
    tx: &Transaction<'_>,
    config: &Config,
    parent: &Manifest,
    entries: Vec<ActivityEntry>,
) -> Result<(Vec<DocumentRef>, bool)> {
    let mut refs = vec![];
    for chunk in entries.chunks(16) {
        refs.push(save(
            tx,
            config,
            parent,
            &IndexPage {
                contract: CONTRACT.into(),
                entries: chunk.to_vec(),
                children: vec![],
            },
        )?);
    }
    let mut verified = refs.iter().all(|p| p.verified);
    for child in &refs {
        depend(
            tx,
            &parent.scope,
            "queue",
            &format!("document:{}", child.document_id),
        )?;
    }
    // Fixed fan-out bounds the number of references and page size at each level.
    while refs.len() > 32 {
        refs = refs
            .chunks(32)
            .map(|children| {
                save(
                    tx,
                    config,
                    parent,
                    &IndexPage {
                        contract: CONTRACT.into(),
                        entries: vec![],
                        children: children.to_vec(),
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        verified &= refs.iter().all(|p| p.verified);
        for child in &refs {
            depend(
                tx,
                &parent.scope,
                "queue",
                &format!("document:{}", child.document_id),
            )?;
        }
    }
    for child in &refs {
        depend(
            tx,
            &parent.scope,
            "queue",
            &format!("document:{}", child.document_id),
        )?;
    }
    Ok((refs, verified))
}
fn window_manifest(
    tx: &Transaction<'_>,
    config: &Config,
    scope: &str,
    window: &str,
) -> Result<Manifest> {
    let (date,tz,start,end,account,config_id,state,reason,attempts):(String,String,i64,i64,String,String,String,Option<String>,u32)=tx.query_row("SELECT local_date,timezone,utc_start,utc_end,account_id,config_id,state,reason,attempts FROM daily_windows WHERE id=?1",[window],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)))?;
    let provider:String=tx.query_row("SELECT coalesce(json_extract(config,'$.daily.provider.kind'),'disabled') FROM config_snapshots WHERE id=?1",[&config_id],|r|r.get(0))?;
    let evidence: Option<String> = tx
        .query_row(
            "SELECT evidence FROM daily_attempts WHERE window_id=?1 ORDER BY attempt DESC LIMIT 1",
            [window],
            |r| r.get(0),
        )
        .optional()?;
    let ids=tx.prepare("SELECT activity_id FROM reader_activity_windows WHERE window_id=?1 ORDER BY activity_id LIMIT ?2")?.query_map(params![window,config.reader.max_activities+1],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        ids.len() <= config.reader.max_activities,
        "reader activity admission bound exceeded"
    );
    let admission_complete: bool = tx.query_row(
        "SELECT done FROM reader_bootstrap WHERE name='activities'",
        [],
        |r| r.get(0),
    )?;
    let admitted:u64=tx.query_row("SELECT count(DISTINCT activity_id) FROM activity_observations WHERE json_extract(manifest,'$.discovery.provenance.window_id')=?1",[window],|r|r.get(0))?;
    let pending_activity_count = admitted.saturating_sub(ids.len() as u64);
    let mut m = Manifest {
        contract: CONTRACT.into(),
        scope: scope.into(),
        window_id: Some(window.into()),
        local_date: date,
        timezone: tz,
        utc_start: start,
        utc_end: end,
        sop: config.reader.sop.clone(),
        sop_configured: config.reader.sop.page_url.is_some() || config.reader.sop.page_id.is_some(),
        windows: vec![WindowEvidence {
            window_id: window.into(),
            account_id: account,
            config_id,
            provider,
            state: state.clone(),
            reason,
            attempts,
            evidence: evidence
                .map(|v| serde_json::from_str(&v))
                .transpose()?
                .unwrap_or(json!({"reason":"not_collected"})),
            failure_report_urls: super::availability::reports(
                tx,
                scope,
                &format!("timeline:{window}"),
            )?,
        }],
        counts: BTreeMap::new(),
        index: vec![],
        accounts: vec![],
        admission_complete,
        pending_activity_count,
        collection_complete: state == "complete"
            && admission_complete
            && pending_activity_count == 0,
        body_complete: admission_complete && pending_activity_count == 0,
        publication_verified: admission_complete && pending_activity_count == 0,
        published_activity_count: 0,
        analysis_complete: false,
    };
    let mut entries = vec![];
    let mut total = 0usize;
    let mut text_hashes = BTreeMap::new();
    for id in ids {
        let item = entry(tx, config, scope, window, &id, &mut text_hashes)?;
        total = total.saturating_add(serde_json::to_vec(&item)?.len());
        ensure!(
            total <= config.reader.export_bytes,
            "reader window byte budget exceeded"
        );
        *m.counts.entry(item.disposition.clone()).or_default() += 1;
        m.body_complete &= item.disposition == "captured";
        // A published index can verify an explicit missing/link-only record without a body page.
        let published = item.sources.iter().all(|s| {
            if s.revision_id.is_none() {
                s.missing_reason.is_some()
            } else {
                s.publication.as_ref().is_some_and(|p| p.verified)
            }
        }) && reference(
            tx,
            "activity",
            &item.observation_id,
            "activity",
            &item.observation_id,
        )?
        .verified;
        m.published_activity_count += u64::from(published);
        m.publication_verified &= published;
        entries.push(item);
    }
    let (index, verified) = index_pages(tx, config, &m, entries)?;
    m.index = index;
    m.publication_verified &= verified;
    Ok(m)
}
fn combined(
    tx: &Transaction<'_>,
    config: &Config,
    scope: &str,
    date: &str,
    tz: &str,
) -> Result<Manifest> {
    let day = date.parse::<chrono::NaiveDate>()?;
    let (start, end) = crate::planner::local_day_bounds(tz, day)?;
    let windows = tx
        .prepare(
            "SELECT id FROM daily_windows WHERE local_date=?1 AND timezone=?2 ORDER BY id LIMIT ?3",
        )?
        .query_map(params![date, tz, config.reader.max_windows + 1], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        windows.len() <= config.reader.max_windows,
        "reader window admission bound exceeded"
    );
    let mut m = Manifest {
        contract: CONTRACT.into(),
        scope: scope.into(),
        window_id: None,
        local_date: date.into(),
        timezone: tz.into(),
        utc_start: start,
        utc_end: end,
        sop: config.reader.sop.clone(),
        sop_configured: config.reader.sop.page_url.is_some() || config.reader.sop.page_id.is_some(),
        windows: vec![],
        counts: BTreeMap::new(),
        index: vec![],
        accounts: vec![],
        admission_complete: true,
        pending_activity_count: 0,
        collection_complete: !windows.is_empty(),
        body_complete: true,
        publication_verified: true,
        published_activity_count: 0,
        analysis_complete: false,
    };
    for window in windows {
        let key: Option<String> = tx
            .query_row(
                "SELECT manifest_id FROM reader_heads WHERE scope=?1",
                [format!("window:{window}")],
                |r| r.get(0),
            )
            .optional()?;
        let Some(key) = key else {
            m.admission_complete = false;
            m.collection_complete = false;
            m.body_complete = false;
            m.publication_verified = false;
            continue;
        };
        let record = read_document(tx, &key, config.notion.record_bytes)?;
        let child = manifest(tx, &key, config.notion.record_bytes)?;
        depend(tx, scope, "queue", &format!("document:{key}"))?;
        m.accounts.push(document_reference(tx, &key, &record)?);
        m.collection_complete &= child.collection_complete && child.sop == m.sop;
        m.admission_complete &= child.admission_complete;
        m.pending_activity_count += child.pending_activity_count;
        m.body_complete &= child.body_complete;
        m.published_activity_count += child.published_activity_count;
        m.publication_verified &= child.publication_verified && m.accounts.last().unwrap().verified;
        m.windows.extend(child.windows.into_iter().map(|mut w|{w.evidence=json!({"account_manifest_id":key,"evidence_sha256":digest(serde_json::to_vec(&w.evidence).unwrap_or_default())});w}));
        for (kind, count) in child.counts {
            *m.counts.entry(kind).or_default() += count;
        }
    }
    // Account references are themselves paginated, with the same bounded fan-out.
    while m.accounts.len() > 32 {
        m.accounts = m
            .accounts
            .chunks(32)
            .map(|children| {
                save(
                    tx,
                    config,
                    &m,
                    &IndexPage {
                        contract: CONTRACT.into(),
                        entries: vec![],
                        children: children.to_vec(),
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        for child in &m.accounts {
            depend(
                tx,
                scope,
                "queue",
                &format!("document:{}", child.document_id),
            )?;
        }
        m.publication_verified &= m.accounts.iter().all(|p| p.verified);
    }
    Ok(m)
}

impl Store {
    /// 64 old windows +64 old observations are admitted per turn; at most 16 affected scopes build.
    /// Unchanged snapshots create no document/head revision. Errors stay visible and are not retried blindly.
    pub fn reader_tick(&self, config: &Config, limit: usize) -> Result<ReaderTick> {
        ensure!(
            (1..=16).contains(&limit),
            "reader build limit must be 1..16"
        );
        config.reader.validate()?;
        let mut out = ReaderTick::default();
        if !config.reader.enabled || self.recovery_paused()? {
            out.paused = true;
            return Ok(out);
        }
        let mut conn = self.connection()?;
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            out.admitted = bootstrap(&tx, config)?;
            tx.commit()?;
        }
        for _ in 0..limit {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let row:Option<(String,Option<String>,String,String)>=tx.query_row("SELECT scope,window_id,local_date,timezone FROM reader_dirty WHERE error IS NULL ORDER BY rowid LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
            let Some((scope, window, date, tz)) = row else {
                break;
            };
            tx.execute("DELETE FROM reader_dependencies WHERE scope=?1", [&scope])?;
            let result = (|| -> Result<()> {
                let m = match &window {
                    Some(id) => window_manifest(&tx, config, &scope, id)?,
                    None => combined(&tx, config, &scope, &date, &tz)?,
                };
                let record = save(&tx, config, &m, &m)?;
                let old: Option<String> = tx
                    .query_row(
                        "SELECT manifest_id FROM reader_heads WHERE scope=?1",
                        [&scope],
                        |r| r.get(0),
                    )
                    .optional()?;
                tx.execute("INSERT INTO reader_heads VALUES(?1,?2,?3,?4,?5) ON CONFLICT(scope) DO UPDATE SET manifest_id=excluded.manifest_id",params![scope,window,date,tz,record.document_id])?;
                if old.as_deref() != Some(&record.document_id) {
                    tx.execute(
                        "UPDATE reader_checkpoints SET stale=1 WHERE scope=?1 AND manifest_id!=?2",
                        params![scope, record.document_id],
                    )?;
                    if window.is_some() {
                        tx.execute(
                            "INSERT OR IGNORE INTO reader_dirty VALUES(?1,NULL,?2,?3,NULL)",
                            params![format!("day:{tz}:{date}"), date, tz],
                        )?;
                    }
                }
                tx.execute("DELETE FROM reader_dirty WHERE scope=?1", [&scope])?;
                Ok(())
            })();
            match result {
                Ok(()) => {
                    tx.commit()?;
                    out.processed += 1;
                }
                Err(_) => {
                    drop(tx);
                    conn.execute("UPDATE reader_dirty SET error='reader_bound_or_invalid_input' WHERE scope=?1",[&scope])?;
                    out.errors += 1;
                }
            }
        }
        out.pending = conn.query_row(
            "SELECT (SELECT count(*) FROM reader_dirty)+(SELECT count(*) FROM reader_dirty_inputs)",
            [],
            |r| r.get(0),
        )?;
        Ok(out)
    }
    pub fn reader_heads(
        &self,
        date: &str,
        tz: &str,
        limit: usize,
        after: Option<&str>,
    ) -> Result<Vec<ManifestHead>> {
        ensure!(
            (1..=1000).contains(&limit),
            "reader list limit must be 1..1000"
        );
        crate::planner::local_day_bounds(tz, date.parse()?)?;
        Ok(self.connection()?.prepare("SELECT scope,window_id,local_date,timezone,manifest_id FROM reader_heads WHERE local_date=?1 AND timezone=?2 AND scope>?3 ORDER BY scope LIMIT ?4")?.query_map(params![date,tz,after.unwrap_or(""),limit],|r|Ok(ManifestHead{scope:r.get(0)?,window_id:r.get(1)?,local_date:r.get(2)?,timezone:r.get(3)?,manifest_id:r.get(4)?}))?.collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn reader_note_source(&self, value: &SourceEnrichment) -> Result<()> {
        self.blob_path(&value.source_id)?;
        ensure!(
            value.attachments.len() <= 256
                && value.gaps.len() <= 256
                && value.failure_report_urls.len() <= 64,
            "reader enrichment exceeds bounds"
        );
        for url in value
            .failure_report_urls
            .iter()
            .chain(value.attachments.iter().flat_map(|a| {
                std::iter::once(&a.original_url)
                    .chain(a.landing_url.iter())
                    .chain(a.direct_url.iter())
            }))
        {
            crate::network::source_url(url)?;
        }
        if let Some(revision) = &value.revision_id {
            ensure!(
                self.revision(revision)?.source_id == value.source_id,
                "reader enrichment revision owner mismatch"
            );
        }
        ensure!(
            serde_json::to_vec(value)?.len() <= 1_048_576,
            "reader enrichment exceeds byte bound"
        );
        let mut canonical = value.clone();
        canonical.gaps.sort();
        canonical.gaps.dedup();
        canonical.failure_report_urls.sort();
        canonical.failure_report_urls.dedup();
        canonical
            .attachments
            .sort_by_cached_key(|a| serde_json::to_string(a).unwrap_or_default());
        canonical.attachments.dedup();
        let value = &canonical;
        let raw = serde_json::to_string(value)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<String> = tx
            .query_row(
                "SELECT payload FROM reader_enrichments WHERE source_id=?1",
                [&value.source_id],
                |r| r.get(0),
            )
            .optional()?;
        if old.as_deref() != Some(&raw) {
            tx.execute("INSERT INTO reader_enrichments VALUES(?1,?2) ON CONFLICT(source_id) DO UPDATE SET payload=excluded.payload",params![value.source_id,raw])?;
            tx.execute("INSERT INTO reader_dirty_inputs(kind,entity) VALUES('source',?1) ON CONFLICT(kind,entity) DO UPDATE SET cursor=''",[&value.source_id])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn reader_status(&self) -> Result<Value> {
        let conn = self.connection()?;
        let dirty = conn
            .prepare("SELECT scope,error FROM reader_dirty ORDER BY scope LIMIT 100")?
            .query_map([], |r| {
                Ok(json!({"scope":r.get::<_,String>(0)?,"error":r.get::<_,Option<String>>(1)?}))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            json!({"contract":CONTRACT,"heads":conn.query_row("SELECT count(*) FROM reader_heads",[],|r|r.get::<_,u64>(0))?,"pending":conn.query_row("SELECT (SELECT count(*) FROM reader_dirty)+(SELECT count(*) FROM reader_dirty_inputs)",[],|r|r.get::<_,u64>(0))?,"pending_inputs":conn.query_row("SELECT count(*) FROM reader_dirty_inputs",[],|r|r.get::<_,u64>(0))?,"dirty_sample":dirty,"bootstrap_pending":conn.query_row("SELECT count(*) FROM reader_bootstrap WHERE done=0",[],|r|r.get::<_,u64>(0))?,"stale_checkpoints":conn.query_row("SELECT count(*) FROM reader_checkpoints WHERE stale=1",[],|r|r.get::<_,u64>(0))?,"external_effects_paused":self.recovery_paused()?}),
        )
    }
}
