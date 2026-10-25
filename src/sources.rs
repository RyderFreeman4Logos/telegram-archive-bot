use crate::{
    digest,
    storage::{Store, now},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub provider: String,
    pub external_id: String,
}

impl Identity {
    pub fn from_url(input: &str) -> Result<Self> {
        let url = Url::parse(input)?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "unsupported source URL"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "source URL credentials are forbidden"
        );
        let host = url.host_str().unwrap();
        if [
            "x.com",
            "www.x.com",
            "mobile.x.com",
            "twitter.com",
            "www.twitter.com",
            "mobile.twitter.com",
            "m.twitter.com",
        ]
        .contains(&host)
        {
            let segments = url
                .path_segments()
                .context("invalid X path")?
                .collect::<Vec<_>>();
            if segments.len() >= 3 && segments[1] == "status" {
                return Self::x(segments[2]);
            }
        }
        Ok(Self {
            provider: "web".into(),
            external_id: url.to_string(),
        })
    }

    pub fn x(id: &str) -> Result<Self> {
        ensure!(
            !id.is_empty() && id.len() <= 30 && id.bytes().all(|b| b.is_ascii_digit()),
            "invalid X status ID"
        );
        Ok(Self {
            provider: "x".into(),
            external_id: id.to_owned(),
        })
    }

    pub fn id(&self) -> String {
        digest(serde_json::to_vec(self).expect("identity serializes"))
    }

    pub fn url(&self) -> String {
        if self.provider == "x" {
            format!("https://x.com/i/status/{}", self.external_id)
        } else {
            self.external_id.clone()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMetadata {
    pub title: Option<String>,
    pub author: Option<String>,
    pub authored_at: Option<String>,
    pub language: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Content {
    pub body: String,
    pub metadata: ContentMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchStatus {
    Complete,
    Partial,
    Missing,
    Deleted,
    Forbidden,
    Error,
}

impl FetchStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Missing => "missing",
            Self::Deleted => "deleted",
            Self::Forbidden => "forbidden",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    Quotes,
    RepliesTo,
    Retweets,
    Links,
    EditOf,
}

impl RelationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::RepliesTo => "replies_to",
            Self::Retweets => "retweets",
            Self::Links => "links",
            Self::EditOf => "edit_of",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Related {
    pub target: Identity,
    pub kind: RelationKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capture {
    pub identity: Identity,
    pub submitted_url: Option<String>,
    pub content: Option<Content>,
    pub status: FetchStatus,
    pub extractor: String,
    pub evidence: Value,
    pub raw_digest: Option<String>,
    pub relations: Vec<Related>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    pub id: String,
    pub source_id: String,
    pub content_digest: String,
    pub content: Content,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewPolicy {
    pub max_nodes: usize,
    pub max_depth: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewMember {
    pub source_id: String,
    pub revision_id: Option<String>,
    pub missing_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewEdge {
    pub source_id: String,
    pub target_id: String,
    pub kind: String,
    pub observation_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Omission {
    pub source_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct View {
    pub schema_version: u32,
    pub root_id: String,
    pub policy: ViewPolicy,
    pub members: Vec<ViewMember>,
    pub edges: Vec<ViewEdge>,
    pub omissions: Vec<Omission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieval: Option<Value>,
}

impl View {
    pub fn id(&self) -> Result<String> {
        Ok(digest(serde_json::to_vec(self)?))
    }
}

pub(crate) fn ensure_source(tx: &Transaction<'_>, identity: &Identity) -> Result<String> {
    ensure!(
        !identity.provider.is_empty() && !identity.external_id.is_empty(),
        "empty provider identity"
    );
    let id = identity.id();
    tx.execute("INSERT INTO sources(id,provider,external_id) VALUES(?1,?2,?3) ON CONFLICT(provider,external_id) DO NOTHING", params![id,identity.provider,identity.external_id])?;
    Ok(id)
}

impl Store {
    pub fn capture(&self, capture: &Capture) -> Result<Option<String>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = self.capture_in_tx(&tx, capture)?;
        tx.commit()?;
        Ok(result)
    }
    pub(crate) fn capture_in_tx(
        &self,
        tx: &Transaction<'_>,
        capture: &Capture,
    ) -> Result<Option<String>> {
        self.capture_in_tx_verified(tx, capture, None)
    }
    pub(crate) fn capture_in_tx_verified(
        &self,
        tx: &Transaction<'_>,
        capture: &Capture,
        verified: Option<&crate::storage::VerifiedBlob>,
    ) -> Result<Option<String>> {
        let has_content = matches!(capture.status, FetchStatus::Complete | FetchStatus::Partial);
        ensure!(
            has_content == capture.content.is_some(),
            "content/status mismatch"
        );
        if let Some(content) = &capture.content {
            ensure!(!content.body.trim().is_empty(), "empty capture body");
        }
        if let Some(raw) = &capture.raw_digest {
            if let Some(verified) = verified {
                ensure!(
                    verified.covers(self, raw),
                    "raw verification belongs to another store or blob"
                );
            } else {
                self.verify_blob(raw)?;
            }
        }
        let event_start: i64 = tx.query_row(
            "SELECT COALESCE(MAX(id),0) FROM projection_events",
            [],
            |r| r.get(0),
        )?;
        let source = ensure_source(tx, &capture.identity)?;
        if let Some(alias) = &capture.submitted_url {
            ensure!(
                Identity::from_url(alias)? == capture.identity,
                "URL does not match source identity"
            );
            tx.execute("INSERT INTO source_aliases VALUES(?1,?2,'provider_identity_v1') ON CONFLICT DO NOTHING",params![source,alias])?;
        }
        let previous: Option<String> = tx.query_row(
            "SELECT current_revision FROM sources WHERE id=?1",
            [&source],
            |r| r.get(0),
        )?;
        let complete = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM observations WHERE source_id=?1 AND revision_id=?2 AND status='complete')",
            params![source, previous],
            |r| r.get::<_, bool>(0),
        )?;
        let mut resulting = None;
        if let Some(content) = capture
            .content
            .as_ref()
            .filter(|_| capture.status == FetchStatus::Complete || !complete)
        {
            let content_digest = digest(serde_json::to_vec(content)?);
            let revision = digest(format!("{source}:{content_digest}"));
            tx.execute("INSERT INTO source_revisions(id,source_id,content_digest,body,metadata) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(source_id,content_digest) DO NOTHING",params![revision,source,content_digest,content.body,serde_json::to_string(&content.metadata)?])?;
            tx.execute(
                "UPDATE sources SET current_revision=?1 WHERE id=?2",
                params![revision, source],
            )?;
            tx.execute("INSERT INTO projection_events(kind,entity_id) VALUES('revision',?1) ON CONFLICT DO NOTHING",[&revision])?;
            resulting = Some(revision);
        }
        tx.execute("INSERT INTO observations(source_id,revision_id,observed_at,status,extractor,evidence,raw_digest) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![source,resulting,now(),capture.status.as_str(),capture.extractor,serde_json::to_string(&capture.evidence)?,capture.raw_digest])?;
        let observation = tx.last_insert_rowid();
        for related in &capture.relations {
            let target = ensure_source(tx, &related.target)?;
            tx.execute(
                "INSERT INTO relations VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",
                params![source, target, related.kind.as_str(), observation],
            )?;
        }
        tx.execute(
            "INSERT INTO projection_events(kind,entity_id) VALUES('observation',?1)",
            [observation.to_string()],
        )?;
        tx.execute(
            "UPDATE projection_events SET group_key=?1 WHERE id>?2",
            params![format!("capture:{observation}"), event_start],
        )?;
        Ok(resulting.or(previous))
    }

    pub fn current_revision(&self, identity: &Identity) -> Result<Option<String>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT current_revision FROM sources WHERE id=?1",
                [identity.id()],
                |r| r.get(0),
            )
            .optional()?
            .flatten())
    }

    pub fn revision(&self, id: &str) -> Result<Revision> {
        let row = self.connection()?.query_row(
            "SELECT source_id,content_digest,body,metadata FROM source_revisions WHERE id=?1",
            [id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )?;
        let revision = Revision {
            id: id.into(),
            source_id: row.0,
            content_digest: row.1,
            content: Content {
                body: row.2,
                metadata: serde_json::from_str(&row.3)?,
            },
        };
        ensure!(
            digest(serde_json::to_vec(&revision.content)?) == revision.content_digest,
            "revision content digest mismatch"
        );
        ensure!(
            digest(format!(
                "{}:{}",
                revision.source_id, revision.content_digest
            )) == revision.id,
            "revision identity mismatch"
        );
        Ok(revision)
    }

    pub fn create_view(&self, root: &Identity, policy: ViewPolicy) -> Result<View> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let view = self.create_view_in_tx(&tx, root, policy)?;
        tx.commit()?;
        Ok(view)
    }
    pub(crate) fn create_view_in_tx(
        &self,
        tx: &Transaction<'_>,
        root: &Identity,
        policy: ViewPolicy,
    ) -> Result<View> {
        ensure!(
            policy.max_nodes > 0 && policy.max_nodes <= 10000 && policy.max_depth <= 10000,
            "invalid view budget"
        );
        let root_id = ensure_source(tx, root)?;
        let mut view = View {
            schema_version: 1,
            root_id: root_id.clone(),
            policy,
            members: vec![],
            edges: vec![],
            omissions: vec![],
            retrieval: None,
        };
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::from([(root_id, 0usize)]);
        while let Some((source, depth)) = queue.pop_front() {
            if !visited.insert(source.clone()) {
                continue;
            }
            if view.members.len() >= view.policy.max_nodes {
                view.omissions.push(Omission {
                    source_id: source,
                    reason: "node_limit".into(),
                });
                continue;
            }
            let revision: Option<String> = tx.query_row(
                "SELECT current_revision FROM sources WHERE id=?1",
                [&source],
                |r| r.get(0),
            )?;
            let missing_reason = if revision.is_none() {
                Some(tx.query_row("SELECT status FROM observations WHERE source_id=?1 ORDER BY id DESC LIMIT 1",[&source],|r|r.get::<_,String>(0)).optional()?.unwrap_or("unfetched_stub".into()))
            } else {
                None
            };
            view.members.push(ViewMember {
                source_id: source.clone(),
                revision_id: revision.clone(),
                missing_reason,
            });
            let edges=tx.prepare("SELECT r.target_id,r.kind,MAX(r.observation_id) FROM relations r JOIN observations o ON o.id=r.observation_id WHERE r.source_id=?1 AND (o.revision_id=?2 OR (?2 IS NULL AND o.revision_id IS NULL)) GROUP BY r.target_id,r.kind ORDER BY r.kind,r.target_id")?.query_map(params![source,revision],|r|Ok(ViewEdge{source_id:source.clone(),target_id:r.get(0)?,kind:r.get(1)?,observation_id:r.get(2)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for edge in edges {
                if !visited.contains(&edge.target_id) {
                    if depth < view.policy.max_depth {
                        queue.push_back((edge.target_id.clone(), depth + 1));
                    } else {
                        view.omissions.push(Omission {
                            source_id: edge.target_id.clone(),
                            reason: "depth_limit".into(),
                        });
                    }
                }
                view.edges.push(edge);
            }
        }
        save_view(tx, &view)?;
        Ok(view)
    }

    pub fn view(&self, id: &str) -> Result<View> {
        let json: String =
            self.connection()?
                .query_row("SELECT manifest FROM views WHERE id=?1", [id], |r| r.get(0))?;
        let view: View = serde_json::from_str(&json)?;
        ensure!(view.id()? == id, "view manifest digest mismatch");
        Ok(view)
    }

    pub fn source_counts(&self) -> Result<(u64, u64, u64)> {
        Ok(self.connection()?.query_row("SELECT (SELECT COUNT(*) FROM sources),(SELECT COUNT(*) FROM source_revisions),(SELECT COUNT(*) FROM observations)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?)
    }
}

pub(crate) fn save_view(tx: &Transaction<'_>, view: &View) -> Result<()> {
    let id = view.id()?;
    tx.execute(
        "INSERT INTO views VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
        params![id, view.root_id, serde_json::to_string(&view)?],
    )?;
    for (ordinal, member) in view.members.iter().enumerate() {
        tx.execute(
            "INSERT INTO view_members VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
            params![
                id,
                ordinal,
                member.source_id,
                member.revision_id,
                member.missing_reason
            ],
        )?;
    }
    tx.execute(
        "INSERT INTO projection_events(kind,entity_id) VALUES('view',?1) ON CONFLICT DO NOTHING",
        [&id],
    )?;
    Ok(())
}
