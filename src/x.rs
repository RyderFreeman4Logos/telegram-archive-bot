use crate::{
    config::Config,
    jobs::Failure,
    network::PublicHttp,
    sources::{
        Capture, Content, ContentMetadata, FetchStatus, Identity, Omission, Related, RelationKind,
    },
    storage::Store,
};
use futures_util::{FutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[path = "x_guest.rs"]
mod guest;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Post {
    pub id: String,
    pub content: Option<Content>,
    pub status: FetchStatus,
    pub relations: Vec<Related>,
    pub previews: Vec<(String, Content)>,
    pub evidence: Value,
    pub raw: Vec<u8>,
}
impl Post {
    fn unavailable(id: &str, status: FetchStatus, reason: &str) -> Self {
        Self {
            id: id.into(),
            content: None,
            status,
            relations: vec![],
            previews: vec![],
            evidence: json!({"reason":reason}),
            raw: vec![],
        }
    }
}

pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn article_only(&self) -> bool {
        false
    }
    fn fetch<'a>(
        &'a self,
        id: &'a str,
        max_bytes: u64,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Post, Failure>>;
}

pub struct Official {
    pub http: PublicHttp,
    pub bearer: String,
    pub timeout: Duration,
}
impl Provider for Official {
    fn name(&self) -> &'static str {
        "x-api-v2"
    }
    fn fetch<'a>(
        &'a self,
        id: &'a str,
        max_bytes: u64,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Post, Failure>> {
        async move {
            let url=format!("https://api.x.com/2/tweets/{id}?tweet.fields=created_at,lang,author_id,referenced_tweets,note_tweet,public_metrics,entities,attachments&expansions=author_id&user.fields=name,username");
            let response=self.http.get(&url,max_bytes,self.timeout,Some(&self.bearer),cancel).await?;
            if matches!(response.status,401|403) {return Ok(Post::unavailable(id,FetchStatus::Forbidden,"provider_authentication_or_access_denied"));}
            if response.status==404 {return Ok(Post::unavailable(id,FetchStatus::Missing,"not_found_is_not_proof_of_deletion"));}
            if response.status==429 {return Err(Failure::RateLimited);}
            if response.status>=500 {return Err(Failure::Unavailable);}
            if response.status!=200 {return Err(Failure::InvalidOutput);}
            let payload:Value=serde_json::from_slice(&response.bytes).map_err(|_|Failure::InvalidOutput)?;
            let mut post=parse_official(id,&payload)?;post.raw=response.bytes;Ok(post)
        }.boxed()
    }
}

pub struct FxEmbed {
    pub http: PublicHttp,
    pub timeout: Duration,
}
impl Provider for FxEmbed {
    fn name(&self) -> &'static str {
        "fxembed-api-v2"
    }
    fn fetch<'a>(
        &'a self,
        id: &'a str,
        max_bytes: u64,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Post, Failure>> {
        async move {
            let response = self
                .http
                .get(
                    &format!("https://api.fxtwitter.com/2/status/{id}"),
                    max_bytes,
                    self.timeout,
                    None,
                    cancel,
                )
                .await?;
            if matches!(response.status, 401 | 403) {
                return Ok(Post::unavailable(
                    id,
                    FetchStatus::Forbidden,
                    "fallback_access_denied",
                ));
            }
            if response.status == 404 {
                return Ok(Post::unavailable(
                    id,
                    FetchStatus::Missing,
                    "fallback_not_found",
                ));
            }
            if response.status == 429 {
                return Err(Failure::RateLimited);
            }
            if response.status >= 500 {
                return Err(Failure::Unavailable);
            }
            let payload: Value =
                serde_json::from_slice(&response.bytes).map_err(|_| Failure::InvalidOutput)?;
            let mut post = parse_fx(id, &payload)?;
            post.raw = response.bytes;
            Ok(post)
        }
        .boxed()
    }
}

fn text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}
fn article_link(body: &str) -> bool {
    body.split(|c: char| c.is_whitespace() || "<>\"'()[]".contains(c))
        .filter_map(|word| url::Url::parse(word).ok())
        .any(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && matches!(url.host_str(), Some("x.com" | "www.x.com" | "mobile.x.com" | "twitter.com" | "www.twitter.com" | "mobile.twitter.com" | "m.twitter.com"))
                && url.path_segments().is_some_and(|parts| {
                    let parts = parts.collect::<Vec<_>>();
                    matches!(parts.as_slice(), ["i", "article", id] | ["i", "article", id, ""] if Identity::x(id).is_ok())
                })
        })
}
fn related(id: &str, kind: RelationKind) -> Result<Related, Failure> {
    Ok(Related {
        target: Identity::x(id).map_err(|_| Failure::InvalidOutput)?,
        kind,
    })
}

// DA04: expanded entity destinations, not display labels or invented URLs.
fn outbound(data: &Value, relations: &mut Vec<Related>) -> Vec<String> {
    let mut candidates = vec![];
    for path in ["/entities/urls", "/note_tweet/entities/urls"] {
        if let Some(entries) = data.pointer(path).and_then(Value::as_array) {
            for entry in entries {
                if let Some(url) = entry["unwound_url"]
                    .as_str()
                    .or_else(|| entry["expanded_url"].as_str())
                {
                    candidates.push(url);
                }
            }
        }
    }
    if let Some(facets) = data["raw_text"]["facets"].as_array() {
        for facet in facets {
            if facet["type"] == "url"
                && let Some(url) = facet["replacement"].as_str()
            {
                candidates.push(url);
            }
        }
    }
    let entities = &data["article"]["content"]["entityMap"];
    let entities = entities
        .as_array()
        .map(|v| v.iter().collect::<Vec<_>>())
        .or_else(|| entities.as_object().map(|v| v.values().collect()))
        .unwrap_or_default();
    for entry in entities {
        let entity = entry.get("value").unwrap_or(entry);
        if entity["type"] == "LINK"
            && let Some(url) = entity["data"]["url"].as_str()
        {
            candidates.push(url);
        }
    }
    if candidates.is_empty()
        && let Some(body) = data["text"].as_str()
    {
        for word in body.split(|c: char| c.is_whitespace() || "<>\"'()[]".contains(c)) {
            if word.starts_with("https://") || word.starts_with("http://") {
                candidates.push(word.trim_end_matches(['.', ',', ';', ':']));
            }
        }
    }
    let mut normalized = std::collections::BTreeSet::new();
    for raw in candidates {
        let Ok(mut url) = crate::network::source_url(raw) else {
            continue;
        };
        url.set_fragment(None);
        let mut pairs = url
            .query_pairs()
            .filter(|(k, _)| !k.starts_with("utm_") && !matches!(k.as_ref(), "fbclid" | "gclid"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<Vec<_>>();
        pairs.sort();
        url.set_query(None);
        if !pairs.is_empty() {
            url.query_pairs_mut().extend_pairs(pairs);
        }
        if let Ok(identity) = Identity::from_url(url.as_str()) {
            // X Article links are handled by selective Article enrichment.
            if identity.provider == "web"
                && matches!(
                    url.host_str(),
                    Some("x.com" | "twitter.com" | "www.x.com" | "www.twitter.com")
                )
            {
                continue;
            }
            if normalized.insert(identity.url()) {
                relations.push(Related {
                    target: identity,
                    kind: RelationKind::Links,
                });
            }
        }
    }
    normalized.into_iter().collect()
}

pub fn parse_official(id: &str, payload: &Value) -> Result<Post, Failure> {
    let data = &payload["data"];
    if data.is_null() {
        return Ok(Post::unavailable(
            id,
            FetchStatus::Error,
            "provider_returned_no_post",
        ));
    }
    if data["id"] != id {
        return Err(Failure::InvalidOutput);
    }
    let body = data["note_tweet"]["text"]
        .as_str()
        .or_else(|| data["text"].as_str())
        .filter(|s| !s.trim().is_empty())
        .ok_or(Failure::InvalidOutput)?;
    let mut relations = vec![];
    if let Some(refs) = data["referenced_tweets"].as_array() {
        for reference in refs {
            let kind = match reference["type"].as_str() {
                Some("quoted") => RelationKind::Quotes,
                Some("replied_to") => RelationKind::RepliesTo,
                Some("retweeted") => RelationKind::Retweets,
                _ => continue,
            };
            relations.push(related(
                reference["id"].as_str().ok_or(Failure::InvalidOutput)?,
                kind,
            )?);
        }
    }
    let author = payload["includes"]["users"]
        .as_array()
        .and_then(|users| users.iter().find(|u| u["id"] == data["author_id"]))
        .and_then(|u| text(u, "name"));
    let article_link = article_link(body) || data["article"].is_object();
    let outbound_links = outbound(data, &mut relations);
    Ok(Post {
        id: id.into(),
        content: Some(Content {
            body: body.into(),
            metadata: ContentMetadata {
                title: None,
                author,
                authored_at: text(data, "created_at"),
                language: text(data, "lang"),
            },
        }),
        status: if article_link {
            FetchStatus::Partial
        } else {
            FetchStatus::Complete
        },
        relations,
        previews: vec![],
        evidence: json!({"public_metrics":data["public_metrics"],"author_id":data["author_id"],"outbound_links":outbound_links,"attachments":data["attachments"],"media_text_status":if data["attachments"].is_object(){Some("references_only_not_transcribed")}else{None},"article_hydrated":false,"limitation":if article_link{Some("article_link_requires_fallback_hydration")}else{None}}),
        raw: vec![],
    })
}

pub fn parse_fx(id: &str, payload: &Value) -> Result<Post, Failure> {
    let code = payload["code"].as_u64().unwrap_or(200);
    if code == 401 || code == 403 {
        return Ok(Post::unavailable(
            id,
            FetchStatus::Forbidden,
            "private_or_forbidden",
        ));
    }
    if code == 404 {
        return Ok(Post::unavailable(
            id,
            FetchStatus::Missing,
            "provider_not_found",
        ));
    }
    if code >= 500 {
        return Err(Failure::Unavailable);
    }
    let data = payload
        .get("status")
        .or_else(|| payload.get("tweet"))
        .ok_or(Failure::InvalidOutput)?;
    if data["type"] == "tombstone" {
        let status = match data["reason"].as_str() {
            Some("deleted") => FetchStatus::Deleted,
            Some("private" | "blocked") => FetchStatus::Forbidden,
            _ => FetchStatus::Missing,
        };
        return Ok(Post::unavailable(
            id,
            status,
            data["reason"].as_str().unwrap_or("unavailable"),
        ));
    }
    let actual = data["id"].as_str().ok_or(Failure::InvalidOutput)?;
    if actual != id {
        let mut post = Post::unavailable(
            id,
            FetchStatus::Partial,
            "fallback_returned_original_repost_object",
        );
        post.content = Some(Content {
            body: format!("Repost of https://x.com/i/status/{actual}"),
            metadata: ContentMetadata::default(),
        });
        post.relations
            .push(related(actual, RelationKind::Retweets)?);
        return Ok(post);
    }
    let mut body = text(data, "text").unwrap_or_default();
    if let Some(raw) = data["raw_text"]["text"]
        .as_str()
        .filter(|raw| raw.len() > body.len())
    {
        body = raw.into();
    }
    let mut title = None;
    let mut status = FetchStatus::Complete;
    let mut limitations = vec![];
    let mut article_hydrated = false;
    let article = &data["article"];
    if article.is_object() {
        title = text(article, "title");
        if let Some(blocks) = article["content"]["blocks"]
            .as_array()
            .filter(|b| !b.is_empty())
        {
            let mut rendered = vec![];
            for block in blocks {
                let text = block["text"].as_str().unwrap_or("");
                let prefix = match block["type"].as_str() {
                    Some("header-one") => "# ",
                    Some("header-two") => "## ",
                    Some("header-three") => "### ",
                    Some("blockquote") => "> ",
                    Some("unordered-list-item") => "- ",
                    Some("ordered-list-item") => "1. ",
                    _ => "",
                };
                if block["type"] == "atomic" {
                    status = FetchStatus::Partial;
                    limitations.push("article_nontext_entity_retained_only_in_raw");
                } else if block["type"] == "code-block" {
                    article_hydrated |= !text.trim().is_empty();
                    rendered.push(format!("```\n{text}\n```"));
                } else {
                    article_hydrated |= !text.trim().is_empty();
                    rendered.push(format!("{prefix}{text}"));
                }
            }
            if let Some(entities) = article["content"]["entityMap"].as_array() {
                for entity in entities {
                    if entity["value"]["type"] == "MARKDOWN"
                        && let Some(markdown) = entity["value"]["data"]["markdown"].as_str()
                    {
                        article_hydrated |= !markdown.trim().is_empty();
                        rendered.push(markdown.into());
                    }
                }
            }
            body = format!(
                "{}\n\n{}\n\n{}",
                body,
                title.as_deref().unwrap_or("Article"),
                rendered.join("\n\n")
            );
        } else {
            status = FetchStatus::Partial;
            limitations.push("article_preview_only");
            if let Some(preview) = article["preview_text"].as_str() {
                body.push_str(&format!("\n\n{preview}"));
            }
        }
    }
    if (article.is_object() || article_link(&body)) && !article_hydrated {
        status = FetchStatus::Partial;
        if !limitations.contains(&"article_preview_only") {
            limitations.push("article_preview_only");
        }
    }
    if body.trim().is_empty() {
        return Err(Failure::InvalidOutput);
    }
    let mut relations = vec![];
    if let Some(parent) = data["replying_to"]["status"]
        .as_str()
        .or_else(|| data["replying_to_status"].as_str())
    {
        relations.push(related(parent, RelationKind::RepliesTo)?);
    }
    let mut previews = vec![];
    if let Some(quote) = data.get("quote").filter(|q| !q.is_null())
        && let Some(target) = quote["id"].as_str()
    {
        relations.push(related(target, RelationKind::Quotes)?);
        if let Some(body) = quote["text"].as_str().filter(|t| !t.trim().is_empty()) {
            previews.push((
                target.into(),
                Content {
                    body: body.into(),
                    metadata: ContentMetadata::default(),
                },
            ));
        }
    }
    let outbound_links = outbound(data, &mut relations);
    Ok(Post {
        id: id.into(),
        content: Some(Content {
            body,
            metadata: ContentMetadata {
                title,
                author: text(&data["author"], "name"),
                authored_at: text(data, "created_at"),
                language: text(data, "lang"),
            },
        }),
        status,
        relations,
        previews,
        evidence: json!({"likes":data["likes"],"views":data["views"],"author_id":data["author"]["id"],"outbound_links":outbound_links,"attachments":data["media"],"media_text_status":if data["media"].is_object(){Some("references_only_not_transcribed")}else{None},"limitations":limitations,"article_hydrated":article_hydrated}),
        raw: vec![],
    })
}

#[derive(Clone)]
struct Cached {
    completed: Instant,
    posts: Vec<(String, Post)>,
}
type Cell = Arc<Mutex<Option<Cached>>>;

pub struct XGraph {
    providers: Vec<Arc<dyn Provider>>,
    cache: Mutex<BTreeMap<String, Cell>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Traversal {
    pub root: Identity,
    pub captures: Vec<Capture>,
    pub omissions: Vec<Omission>,
    pub visited: Vec<String>,
    pub bytes: u64,
    pub max_nodes: usize,
    pub max_depth: usize,
    pub reply_ancestors: usize,
}

impl Traversal {
    pub fn persist(&self, store: &Store) -> anyhow::Result<crate::sources::View> {
        let mut conn = store.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let view = self.persist_in_tx(store, &tx)?;
        tx.commit()?;
        Ok(view)
    }
    pub(crate) fn persist_in_tx(
        &self,
        store: &Store,
        tx: &rusqlite::Transaction<'_>,
    ) -> anyhow::Result<crate::sources::View> {
        use crate::sources::{View, ViewEdge, ViewMember, ViewPolicy, ensure_source, save_view};
        use rusqlite::params;
        for capture in &self.captures {
            store.capture_in_tx(tx, capture)?;
        }
        let root_id = ensure_source(tx, &self.root)?;
        let mut ids = self.visited.clone();
        if !ids.contains(&root_id) {
            ids.insert(0, root_id.clone());
        }
        let mut view = View {
            schema_version: 1,
            root_id,
            policy: ViewPolicy {
                max_nodes: self.max_nodes,
                max_depth: self.max_depth,
            },
            members: vec![],
            edges: vec![],
            omissions: self.omissions.clone(),
            retrieval: Some(
                json!({"adapter":"x-graph-v1","bytes":self.bytes,"reply_ancestors":self.reply_ancestors,"providers":self.captures.iter().map(|c|&c.extractor).collect::<BTreeSet<_>>()}),
            ),
        };
        for source in ids {
            let revision: Option<String> = tx.query_row(
                "SELECT current_revision FROM sources WHERE id=?1",
                [&source],
                |r| r.get(0),
            )?;
            let missing_reason = if revision.is_none() {
                Some("not_captured_in_traversal".into())
            } else {
                None
            };
            view.members.push(ViewMember {
                source_id: source.clone(),
                revision_id: revision.clone(),
                missing_reason,
            });
            let edges=tx.prepare("SELECT r.target_id,r.kind,MAX(r.observation_id) FROM relations r JOIN observations o ON o.id=r.observation_id WHERE r.source_id=?1 AND (o.revision_id=?2 OR (?2 IS NULL AND o.revision_id IS NULL)) GROUP BY r.target_id,r.kind ORDER BY r.kind,r.target_id")?.query_map(params![source,revision],|r|Ok(ViewEdge{source_id:source.clone(),target_id:r.get(0)?,kind:r.get(1)?,observation_id:r.get(2)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            view.edges.extend(edges);
        }
        save_view(tx, &view)?;
        Ok(view)
    }
}

impl XGraph {
    /// Daily capture prefers the configured free fallback before other providers.
    pub fn configured_daily(config: &Config) -> anyhow::Result<Self> {
        let mut graph = Self::configured(config)?;
        graph.providers.sort_by_key(|p| {
            if p.name() == "fxembed-api-v2" {
                0
            } else if p.article_only() {
                2
            } else {
                1
            }
        });
        Ok(graph)
    }
    /// One durable-queue node, preserving raw responses and embedded previews.
    pub async fn capture_one(
        &self,
        store: &Store,
        source: &Identity,
        config: &Config,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Vec<Capture>> {
        anyhow::ensure!(source.provider == "x", "invalid X capture identity");
        let posts = self
            .fetch(&source.external_id, config.limits.input_bytes, true, cancel)
            .await?;
        let mut captures = vec![];
        for (provider, post) in posts {
            let raw_digest = if post.raw.is_empty() {
                None
            } else {
                Some(store.publish_bytes(&post.raw)?.digest)
            };
            captures.push(Capture {
                identity: source.clone(),
                submitted_url: Some(source.url()),
                content: post.content,
                status: post.status,
                extractor: provider,
                evidence: post.evidence,
                raw_digest,
                relations: post.relations,
            });
            for (id, preview) in post.previews {
                captures.push(Capture {
                    identity: Identity::x(&id)?,
                    submitted_url: None,
                    content: Some(preview),
                    status: FetchStatus::Partial,
                    extractor: "embedded-quote-preview".into(),
                    evidence: json!({"limitation":"excerpt_only"}),
                    raw_digest: None,
                    relations: vec![],
                });
            }
        }
        Ok(captures)
    }
    pub fn new(providers: Vec<Arc<dyn Provider>>) -> Self {
        Self {
            providers,
            cache: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn configured(config: &Config) -> anyhow::Result<Self> {
        let timeout = Duration::from_secs(config.limits.download_seconds);
        let mut providers: Vec<Arc<dyn Provider>> = vec![];
        if config.sources.x_enabled {
            let bearer = config
                .sources
                .x_bearer
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("X bearer reference missing"))?
                .read()?;
            providers.push(Arc::new(Official {
                http: PublicHttp::default(),
                bearer,
                timeout,
            }));
        }
        if config.sources.x_fallback_enabled {
            providers.push(Arc::new(FxEmbed {
                http: PublicHttp::default(),
                timeout,
            }));
        }
        if config.sources.x_guest_enabled {
            let bearer = config
                .sources
                .x_guest_bearer
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("guest bearer reference missing"))?
                .read()?;
            providers.push(Arc::new(guest::Guest::new(
                PublicHttp::default(),
                bearer,
                timeout,
            )));
        }
        anyhow::ensure!(!providers.is_empty(), "no X provider configured");
        Ok(Self::new(providers))
    }

    async fn fetch(
        &self,
        id: &str,
        max_bytes: u64,
        force: bool,
        cancel: &CancellationToken,
    ) -> Result<Vec<(String, Post)>, Failure> {
        let requested = Instant::now();
        let cell = {
            let mut cache = self.cache.lock().await;
            if !cache.contains_key(id) && cache.len() >= 128 {
                let stale = cache
                    .iter()
                    .find(|(_, cell)| Arc::strong_count(cell) == 1)
                    .map(|(id, _)| id.clone())
                    .ok_or(Failure::Unavailable)?;
                cache.remove(&stale);
            }
            cache.entry(id.into()).or_default().clone()
        };
        let mut cached = tokio::select! {_=cancel.cancelled()=>return Err(Failure::Cancelled),cached=cell.lock()=>cached};
        if let Some(value) = cached.as_ref().filter(|c| {
            c.completed.elapsed() < Duration::from_secs(30) && (!force || c.completed >= requested)
        }) {
            let mut posts = value.posts.clone();
            for (_, post) in &mut posts {
                post.evidence["cache_reused"] = json!(true);
            }
            return Ok(posts);
        }
        let mut posts: Vec<(String, Post)> = vec![];
        let mut bytes = 0u64;
        for provider in &self.providers {
            if provider.article_only()
                && (!posts.iter().any(|(_, p)| {
                    p.evidence["limitation"] == "article_link_requires_fallback_hydration"
                        || p.evidence["limitations"]
                            .as_array()
                            .is_some_and(|v| v.iter().any(|v| v == "article_preview_only"))
                }) || posts
                    .iter()
                    .any(|(_, p)| p.evidence["article_hydrated"] == true))
            {
                continue;
            }
            let mut post = match provider
                .fetch(id, max_bytes.saturating_sub(bytes), cancel)
                .await
            {
                Ok(post) => post,
                Err(Failure::Cancelled) => return Err(Failure::Cancelled),
                Err(error) => Post::unavailable(id, FetchStatus::Error, &error.to_string()),
            };
            if post.id != id {
                return Err(Failure::InvalidOutput);
            }
            post.evidence["retrieved_at"] = json!(crate::storage::now());
            bytes = bytes.saturating_add(post.raw.len() as u64);
            let complete = post.status == FetchStatus::Complete;
            posts.push((provider.name().into(), post));
            if complete {
                break;
            }
        }
        let cache_bytes: usize = posts
            .iter()
            .map(|(_, post)| {
                post.raw.len() + post.content.as_ref().map(|c| c.body.len()).unwrap_or(0)
            })
            .sum();
        *cached = if cache_bytes <= 256 * 1024 {
            Some(Cached {
                completed: Instant::now(),
                posts: posts.clone(),
            })
        } else {
            None
        };
        Ok(posts)
    }

    pub async fn traverse(
        &self,
        store: &Store,
        url: &str,
        config: &Config,
        force: bool,
        cancel: &CancellationToken,
    ) -> Result<Traversal, Failure> {
        let root = Identity::from_url(url).map_err(|_| Failure::InvalidInput)?;
        if root.provider != "x" || self.providers.is_empty() {
            return Err(Failure::InvalidInput);
        }
        let mut result = Traversal {
            root: root.clone(),
            captures: vec![],
            omissions: vec![],
            visited: vec![],
            bytes: 0,
            max_nodes: config.limits.graph_nodes,
            max_depth: config.limits.graph_depth,
            reply_ancestors: config.limits.reply_ancestors,
        };
        let mut queue = VecDeque::from([(root.external_id.clone(), 0usize, 0usize)]);
        let mut visited = BTreeSet::new();
        let mut previews = BTreeMap::new();
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(config.limits.download_seconds);
        while let Some((id, depth, replies)) = queue.pop_front() {
            if visited.contains(&id) {
                continue;
            }
            let identity = Identity::x(&id).map_err(|_| Failure::InvalidOutput)?;
            let omission = if result.visited.len() >= result.max_nodes {
                Some("node_limit")
            } else if depth > result.max_depth {
                Some("depth_limit")
            } else if replies > result.reply_ancestors {
                Some("reply_ancestor_limit")
            } else if result.bytes >= config.limits.input_bytes {
                Some("byte_limit")
            } else {
                None
            };
            if let Some(reason) = omission {
                result.omissions.push(Omission {
                    source_id: identity.id(),
                    reason: reason.into(),
                });
                continue;
            }
            visited.insert(id.clone());
            let posts = tokio::select! {biased; _=cancel.cancelled()=>return Err(Failure::Cancelled),posts=tokio::time::timeout_at(deadline,self.fetch(&id,config.limits.input_bytes-result.bytes,force,cancel))=>match posts {Ok(result)=>result?,Err(_)=>{result.omissions.push(Omission{source_id:identity.id(),reason:"time_limit".into()});break;}}};
            result.visited.push(identity.id());
            let mut useful = false;
            for (provider, post) in posts {
                let size = post.raw.len() as u64
                    + post
                        .content
                        .as_ref()
                        .map(|c| c.body.len() as u64)
                        .unwrap_or(0);
                if size > config.limits.input_bytes - result.bytes {
                    result.omissions.push(Omission {
                        source_id: identity.id(),
                        reason: "byte_limit".into(),
                    });
                    break;
                }
                result.bytes += size;
                let raw_digest = if post.raw.is_empty() {
                    None
                } else {
                    Some(
                        store
                            .publish_bytes(&post.raw)
                            .map_err(|_| Failure::DiskFull)?
                            .digest,
                    )
                };
                useful |= post.content.is_some();
                for (id, preview) in post.previews {
                    previews.entry(id).or_insert(preview);
                }
                let mut relations = post.relations;
                relations.sort_by_key(|r| (r.kind, r.target.external_id.clone()));
                for relation in &relations {
                    if relation.target.provider != "x" {
                        continue;
                    }
                    queue.push_back((
                        relation.target.external_id.clone(),
                        depth + 1,
                        if relation.kind == RelationKind::RepliesTo {
                            replies + 1
                        } else {
                            0
                        },
                    ));
                }
                result.captures.push(Capture {
                    identity: identity.clone(),
                    submitted_url: Some(if identity == root {
                        url.into()
                    } else {
                        identity.url()
                    }),
                    content: post.content,
                    status: post.status,
                    extractor: provider,
                    evidence: post.evidence,
                    raw_digest,
                    relations,
                });
            }
            if !useful && let Some(content) = previews.get(&id) {
                result.captures.push(Capture {
                    identity,
                    submitted_url: None,
                    content: Some(content.clone()),
                    status: FetchStatus::Partial,
                    extractor: "embedded-quote-preview".into(),
                    evidence: json!({"limitation":"excerpt_only"}),
                    raw_digest: None,
                    relations: vec![],
                });
            }
        }
        Ok(result)
    }
}
