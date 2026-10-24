use crate::{
    audio::Profile,
    config::Config,
    digest,
    jobs::{Lease, Scope, fence},
    storage::{Store, now},
    telegram::Telegram,
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Delivery {
    Audio {
        artifact_id: String,
        title: String,
        performer: String,
        caption: String,
    },
    Document {
        blob_digest: String,
        name: String,
        mime: String,
        caption: String,
    },
    Text {
        text: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub message_id: i64,
    pub chat_id: i64,
    pub media_type: String,
    pub duration_seconds: Option<u64>,
    pub file_id: Option<String>,
    pub file_unique_id: Option<String>,
    pub mime: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxStatus {
    pub id: String,
    pub job_id: String,
    pub state: String,
    pub attempts: u32,
    pub error: Option<String>,
    pub receipt: Option<Receipt>,
}
struct Prepared {
    method: &'static str,
    field: &'static str,
    profile: String,
    representation: String,
    blob: Option<String>,
    name: String,
    mime: String,
    fields: Value,
    duration: Option<u64>,
}
struct Attempt {
    id: String,
    outbox_id: String,
    bot_id: i64,
    chat_id: i64,
    reply_to: Option<i64>,
    number: u32,
    config: Config,
    cached_file_id: Option<String>,
    prepared: Prepared,
}

pub fn caption(text: &str) -> String {
    if text.encode_utf16().count() <= 1024 {
        return text.into();
    }
    let mut result = String::new();
    let mut units = 0;
    for c in text.chars() {
        if units + c.len_utf16() > 1023 {
            result.push('…');
            return result;
        }
        result.push(c);
        units += c.len_utf16();
    }
    result
}
fn utf16(text: &str, max: usize) -> Result<()> {
    ensure!(
        text.encode_utf16().count() <= max && !text.contains('\0'),
        "Telegram text exceeds its presentation limit"
    );
    Ok(())
}
impl Delivery {
    pub(crate) fn validate(&self, store: &Store, config: &Config) -> Result<()> {
        self.prepare(store, config)?;
        Ok(())
    }
    fn prepare(&self, store: &Store, config: &Config) -> Result<Prepared> {
        match self {
            Self::Text { text } => {
                utf16(text, 4096)?;
                ensure!(!text.trim().is_empty(), "empty message");
                Ok(Prepared {
                    method: "sendMessage",
                    field: "text",
                    profile: "text".into(),
                    representation: digest(text),
                    blob: None,
                    name: String::new(),
                    mime: String::new(),
                    fields: json!({"text":text}),
                    duration: None,
                })
            }
            Self::Document {
                blob_digest,
                name,
                mime,
                caption,
            } => {
                utf16(caption, 1024)?;
                ensure!(
                    name == &crate::documents::display_name(name)
                        && !name.is_empty()
                        && name.len() <= 200
                        && !name.chars().any(char::is_control),
                    "invalid document display name"
                );
                ensure!(
                    matches!(
                        mime.as_str(),
                        "application/pdf" | "text/markdown" | "text/plain"
                    ),
                    "unsupported document MIME"
                );
                let blob = store.verify_blob(blob_digest)?;
                ensure!(
                    blob.size > 0 && blob.size <= config.limits.input_bytes,
                    "document size limit"
                );
                let bytes = store.read_blob(blob_digest, config.limits.input_bytes)?;
                if mime == "application/pdf" {
                    ensure!(bytes.starts_with(b"%PDF-"), "PDF signature missing");
                } else {
                    std::str::from_utf8(&bytes)?;
                }
                Ok(Prepared {
                    method: "sendDocument",
                    field: "document",
                    profile: format!("document:{mime}"),
                    representation: digest(format!("{blob_digest}:{name}:{mime}")),
                    blob: Some(blob_digest.clone()),
                    name: name.clone(),
                    mime: mime.clone(),
                    fields: json!({"caption":caption}),
                    duration: None,
                })
            }
            Self::Audio {
                artifact_id,
                title,
                performer,
                caption,
            } => {
                utf16(caption, 1024)?;
                utf16(title, 256)?;
                utf16(performer, 256)?;
                let a = store.artifact(artifact_id)?;
                ensure!(
                    a.byte_length <= config.limits.media_bytes,
                    "audio size limit"
                );
                let voice = a.effective_profile == Profile::VoiceNote;
                let duration = (a.duration_ms + 500) / 1000;
                ensure!(duration > 0, "audio duration rounds to zero");
                let mut fields = json!({"caption":caption,"duration":duration});
                if !voice {
                    fields["title"] = json!(title);
                    fields["performer"] = json!(performer);
                }
                Ok(Prepared {
                    method: if voice { "sendVoice" } else { "sendAudio" },
                    field: if voice { "voice" } else { "audio" },
                    profile: a.effective_profile.as_str().into(),
                    representation: a.blob_digest.clone(),
                    blob: Some(a.blob_digest),
                    name: format!(
                        "audio-{}.{}",
                        &a.artifact_id[..16],
                        if voice {
                            "ogg"
                        } else if a.codec == "aac" {
                            "m4a"
                        } else {
                            "mp3"
                        }
                    ),
                    mime: a.mime,
                    fields,
                    duration: Some(duration),
                })
            }
        }
    }
}
pub(crate) fn enqueue_progress_tx(tx: &Transaction<'_>, job_id: &str) -> Result<()> {
    let id = digest(format!("progress-v1:{job_id}"));
    let payload = serde_json::to_string(&Delivery::Text {
        text: format!("Queued.\n/status {job_id}\n/cancel {job_id}"),
    })?;
    // A progress notice has no ordering authority over the actual deliverables.
    tx.execute("INSERT INTO outbox(id,job_id,kind,payload,state,ordinal) VALUES(?1,?2,'text',?3,'pending',-1) ON CONFLICT(id) DO NOTHING", params![id, job_id, payload])?;
    Ok(())
}

pub(crate) fn enqueue_tx(
    tx: &Transaction<'_>,
    job_id: &str,
    deliveries: &[Delivery],
) -> Result<Vec<String>> {
    let mut ids = vec![];
    let mut ordinal: i64 = tx.query_row(
        "SELECT COALESCE(MAX(ordinal),-1)+1 FROM outbox WHERE job_id=?1",
        [job_id],
        |row| row.get(0),
    )?;
    for (order, payload) in deliveries.iter().enumerate() {
        let value = serde_json::to_string(payload)?;
        let id = digest(format!("delivery-v1:{job_id}:{order}:{value}"));
        let kind = match payload {
            Delivery::Audio { .. } => "audio",
            Delivery::Document { .. } => "document",
            Delivery::Text { .. } => "text",
        };
        ordinal += tx.execute("INSERT INTO outbox(id,job_id,kind,payload,state,ordinal) VALUES(?1,?2,?3,?4,'pending',?5) ON CONFLICT(id) DO NOTHING",params![id,job_id,kind,value,ordinal])? as i64;
        ids.push(id);
    }
    Ok(ids)
}
fn scoped_outbox(tx: &Transaction<'_>, scope: Scope, id: &str) -> Result<()> {
    ensure!(tx.query_row("SELECT EXISTS(SELECT 1 FROM outbox o JOIN jobs j ON j.id=o.job_id JOIN inbox i ON i.bot_id=j.bot_id AND i.update_id=j.update_id WHERE o.id=?1 AND j.bot_id=?2 AND i.chat_id=?3 AND i.user_id=?4)",params![id,scope.bot_id,scope.chat_id,scope.user_id],|r|r.get::<_,bool>(0))?,"delivery unavailable in this scope");
    Ok(())
}
pub(crate) fn retry_delivery_tx(
    tx: &Transaction<'_>,
    scope: Scope,
    id: &str,
    accept_duplicate_risk: bool,
) -> Result<()> {
    scoped_outbox(tx, scope, id)?;
    let state: String = tx.query_row("SELECT state FROM outbox WHERE id=?1", [id], |r| r.get(0))?;
    ensure!(
        matches!(
            state.as_str(),
            "retryable" | "terminal" | "paused" | "unknown" | "confirmed"
        ),
        "delivery is already pending or active"
    );
    ensure!(
        state == "retryable" || state == "paused" || accept_duplicate_risk,
        "resending this delivery requires explicit acceptance of duplicate risk"
    );
    // Explicit user action grants a new bounded retry budget while retaining attempt history.
    tx.execute("UPDATE outbox SET state='pending',available_at=CASE WHEN error='rate_limited' THEN available_at ELSE 0 END,error=NULL,lease_until=NULL,retry_base=attempt WHERE id=?1",[id])?;
    Ok(())
}
impl Store {
    pub fn enqueue_deliveries(
        &self,
        lease: &Lease,
        deliveries: &[Delivery],
    ) -> Result<Vec<String>> {
        ensure!(
            deliveries.len() <= lease.config.limits.graph_nodes.saturating_add(8),
            "too many deliveries"
        );
        for delivery in deliveries {
            delivery.prepare(self, &lease.config)?;
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        let ids = enqueue_tx(&tx, &lease.job.id, deliveries)?;
        tx.commit()?;
        Ok(ids)
    }
    pub fn outbox_for(&self, scope: Scope, id: &str) -> Result<OutboxStatus> {
        let mut conn = self.connection()?;
        let tx = conn.transaction()?;
        scoped_outbox(&tx, scope, id)?;
        let result = tx.query_row(
            "SELECT job_id,state,attempt,error,receipt FROM outbox WHERE id=?1",
            [id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, u32>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            },
        )?;
        Ok(OutboxStatus {
            id: id.into(),
            job_id: result.0,
            state: result.1,
            attempts: result.2,
            error: result.3,
            receipt: result.4.map(|s| serde_json::from_str(&s)).transpose()?,
        })
    }
    pub fn retry_delivery(
        &self,
        scope: Scope,
        id: &str,
        accept_duplicate_risk: bool,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        retry_delivery_tx(&tx, scope, id, accept_duplicate_risk)?;
        tx.commit()?;
        Ok(())
    }
    fn claim_delivery(&self, bot: i64) -> Result<Option<Attempt>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if tx.query_row("SELECT EXISTS(SELECT 1 FROM operational_holds)", [], |r| {
            r.get::<_, bool>(0)
        })? {
            tx.commit()?;
            return Ok(None);
        }
        let time = now();
        tx.execute("UPDATE delivery_attempts SET state='unknown',finished_at=?1,error='worker_lost' WHERE state='sending' AND outbox_id IN (SELECT id FROM outbox WHERE state='sending' AND (lease_until IS NULL OR lease_until<=?1))",[time])?;
        tx.execute("UPDATE outbox SET state='unknown',error='worker_lost',lease_until=NULL WHERE state='sending' AND (lease_until IS NULL OR lease_until<=?1)",[time])?;
        let row=tx.query_row("SELECT o.id,o.job_id,o.attempt,o.payload,c.config,i.chat_id,(SELECT message_id FROM message_origins m WHERE m.bot_id=j.bot_id AND m.update_id=j.update_id) FROM outbox o JOIN jobs j ON j.id=o.job_id JOIN inbox i ON i.bot_id=j.bot_id AND i.update_id=j.update_id JOIN config_snapshots c ON c.id=j.config_id WHERE j.bot_id=?1 AND COALESCE((SELECT until FROM telegram_cooldowns WHERE bot_id=?1),0)<=?2 AND o.state IN ('pending','retryable') AND o.available_at<=?2 AND NOT EXISTS(SELECT 1 FROM outbox earlier WHERE earlier.job_id=o.job_id AND earlier.ordinal>=0 AND earlier.ordinal<o.ordinal AND earlier.state!='confirmed') ORDER BY o.rowid LIMIT 1",params![bot,time],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u32>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,i64>(5)?,r.get::<_,Option<i64>>(6)?))).optional()?;
        let Some((outbox_id, _job_id, number, payload, config, chat_id, reply_to)) = row else {
            tx.commit()?;
            return Ok(None);
        };
        tx.commit()?;
        let config: Config = serde_json::from_str(&config)?;
        let payload: Delivery = serde_json::from_str(&payload)?;
        // Invalid local artifacts fail before the transmission boundary.
        let prepared = match payload.prepare(self, &config) {
            Ok(p) => p,
            Err(_) => {
                self.connection()?.execute("UPDATE outbox SET state='terminal',error='local_validation_failed' WHERE id=?1 AND attempt=?2 AND state IN ('pending','retryable')",params![outbox_id,number])?;
                return Ok(None);
            }
        };
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let time = now();
        let eligible = tx.query_row("SELECT EXISTS(SELECT 1 FROM outbox o WHERE o.id=?1 AND o.attempt=?2 AND o.state IN ('pending','retryable') AND o.available_at<=?3 AND COALESCE((SELECT until FROM telegram_cooldowns WHERE bot_id=?4),0)<=?3 AND NOT EXISTS(SELECT 1 FROM outbox earlier WHERE earlier.job_id=o.job_id AND earlier.ordinal>=0 AND earlier.ordinal<o.ordinal AND earlier.state!='confirmed'))",params![outbox_id,number,time,bot],|r|r.get::<_,bool>(0))?;
        if !eligible {
            tx.commit()?;
            return Ok(None);
        }
        let cached_file_id = if prepared.blob.is_some() {
            tx.query_row("SELECT file_id FROM telegram_file_ids WHERE bot_id=?1 AND representation=?2 AND profile=?3",params![bot,prepared.representation,prepared.profile],|r|r.get(0)).optional()?
        } else {
            None
        };
        let number = number
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("delivery attempt counter exhausted"))?;
        let id = digest(format!("send-attempt-v1:{outbox_id}:{number}"));
        let seconds = i64::try_from(config.limits.send_seconds)?
            .checked_add(15)
            .ok_or_else(|| anyhow::anyhow!("send timeout overflow"))?;
        tx.execute(
            "UPDATE outbox SET state='sending',attempt=?1,lease_until=?2 WHERE id=?3",
            params![
                number,
                time.checked_add(seconds)
                    .ok_or_else(|| anyhow::anyhow!("send deadline overflow"))?,
                outbox_id
            ],
        )?;
        tx.execute("INSERT INTO delivery_attempts(id,outbox_id,ordinal,started_at,state,used_file_id) VALUES(?1,?2,?3,?4,'sending',?5)",params![id,outbox_id,number,time,cached_file_id.is_some()])?;
        tx.commit()?;
        Ok(Some(Attempt {
            id,
            outbox_id,
            bot_id: bot,
            chat_id,
            reply_to,
            number,
            config,
            cached_file_id,
            prepared,
        }))
    }
}

fn receipt(result: &Value, p: &Prepared, chat: i64) -> Result<Receipt> {
    let message = result["message_id"]
        .as_i64()
        .filter(|v| *v > 0)
        .ok_or_else(|| anyhow::anyhow!("receipt has no message ID"))?;
    ensure!(
        result["chat"]["id"].as_i64() == Some(chat),
        "receipt chat mismatch"
    );
    if p.method == "sendMessage" {
        ensure!(result["text"] == p.fields["text"], "receipt text mismatch");
        return Ok(Receipt {
            message_id: message,
            chat_id: chat,
            media_type: "text".into(),
            duration_seconds: None,
            file_id: None,
            file_unique_id: None,
            mime: None,
        });
    }
    let media = &result[p.field];
    ensure!(media.is_object(), "receipt media type mismatch");
    let id = |name| -> Result<String> {
        let id = media[name]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("receipt lacks reusable identifiers"))?;
        ensure!(
            !id.is_empty() && id.len() <= 2048 && !id.chars().any(char::is_control),
            "invalid receipt identifier"
        );
        Ok(id.into())
    };
    let duration = if let Some(expected) = p.duration {
        let got = media["duration"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("receipt has no duration"))?;
        ensure!(
            got > 0 && expected.abs_diff(got) <= 1,
            "receipt duration mismatch"
        );
        Some(got)
    } else {
        None
    };
    let mime = media["mime_type"].as_str().map(String::from);
    if let Some(mime) = &mime {
        ensure!(mime == &p.mime, "receipt MIME mismatch");
    }
    Ok(Receipt {
        message_id: message,
        chat_id: chat,
        media_type: p.field.into(),
        duration_seconds: duration,
        file_id: Some(id("file_id")?),
        file_unique_id: Some(id("file_unique_id")?),
        mime,
    })
}
enum SendError {
    BeforeSend,
    Connect,
    Unknown,
    Cancelled,
}
pub struct DeliveryWorker {
    pub store: Store,
    pub telegram: Telegram,
    pub bot_id: i64,
}
impl DeliveryWorker {
    async fn transmit(&self, a: &Attempt, p: &Prepared) -> std::result::Result<Value, SendError> {
        let endpoint = self.telegram.endpoint(p.method);
        let mut fields = p.fields.clone();
        fields["chat_id"] = json!(a.chat_id);
        if let Some(message_id) = a.reply_to {
            fields["reply_parameters"] =
                json!({"message_id":message_id,"allow_sending_without_reply":true});
        }
        let request = if let Some(blob) = &p.blob
            && a.cached_file_id.is_none()
        {
            let max = if p.field == "document" {
                a.config.limits.input_bytes
            } else {
                a.config.limits.media_bytes
            };
            let bytes = self
                .store
                .read_blob(blob, max)
                .map_err(|_| SendError::BeforeSend)?;
            let part = reqwest::multipart::Part::bytes(bytes)
                .file_name(p.name.clone())
                .mime_str(&p.mime)
                .map_err(|_| SendError::BeforeSend)?;
            let mut form = reqwest::multipart::Form::new().part(p.field, part);
            for (key, value) in fields.as_object().unwrap() {
                let text = value
                    .as_str()
                    .map(String::from)
                    .unwrap_or_else(|| value.to_string());
                form = form.text(key.clone(), text);
            }
            self.telegram.client.post(endpoint).multipart(form)
        } else {
            if let Some(id) = &a.cached_file_id {
                fields[p.field] = json!(id);
            }
            self.telegram.client.post(endpoint).json(&fields)
        };
        let response = request
            .timeout(Duration::from_secs(a.config.limits.send_seconds))
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    SendError::Connect
                } else {
                    SendError::Unknown
                }
            })?;
        Telegram::envelope(response, 2 * 1024 * 1024)
            .await
            .map_err(|_| SendError::Unknown)
    }
    pub async fn send_next(&self, cancel: &CancellationToken) -> Result<bool> {
        if cancel.is_cancelled() {
            return Ok(false);
        }
        let Some(a) = self.store.claim_delivery(self.bot_id)? else {
            return Ok(false);
        };
        let prepared = &a.prepared;
        let result = tokio::select! {biased;
            _=cancel.cancelled()=>Err(SendError::Cancelled),
            result=tokio::time::timeout(Duration::from_secs(a.config.limits.send_seconds),self.transmit(&a,prepared))=>result.unwrap_or(Err(SendError::Unknown)),
        };
        let (mut state, error, delay, response, confirmed, invalidate) = match result {
            Err(SendError::BeforeSend) => {
                ("terminal", "local_validation_failed", 0, None, None, false)
            }
            Err(SendError::Connect) => (
                "retryable",
                "connection_failed_before_send",
                1u64 << a.number.min(8),
                None,
                None,
                false,
            ),
            Err(SendError::Cancelled) => ("unknown", "cancelled_during_send", 0, None, None, false),
            Err(SendError::Unknown) => (
                "unknown",
                "transmission_or_response_unknown",
                0,
                None,
                None,
                false,
            ),
            Ok(value) if value["ok"] == false => {
                let code = value["error_code"].as_i64().unwrap_or(0);
                if code == 400 && a.cached_file_id.is_some() {
                    (
                        "retryable",
                        "cached_identifier_rejected",
                        0,
                        Some(value),
                        None,
                        true,
                    )
                } else if code == 429 {
                    let delay = value["parameters"]["retry_after"]
                        .as_u64()
                        .unwrap_or(1)
                        .max(1);
                    ("retryable", "rate_limited", delay, Some(value), None, false)
                } else if (500..=599).contains(&code) {
                    (
                        "retryable",
                        "telegram_explicit_server_rejection",
                        1u64 << a.number.min(8),
                        Some(value),
                        None,
                        false,
                    )
                } else {
                    (
                        "terminal",
                        "telegram_explicit_rejection",
                        0,
                        Some(value),
                        None,
                        false,
                    )
                }
            }
            Ok(value) if value["ok"] == true => {
                match receipt(&value["result"], prepared, a.chat_id) {
                    Ok(receipt) => ("confirmed", "", 0, Some(value), Some(receipt), false),
                    Err(_) => (
                        "unknown",
                        "invalid_success_receipt",
                        0,
                        Some(value),
                        None,
                        false,
                    ),
                }
            }
            Ok(value) => (
                "unknown",
                "invalid_response_envelope",
                0,
                Some(value),
                None,
                false,
            ),
        };
        #[cfg(test)]
        if std::env::var_os("ARCHIVE_TEST_CRASH_AFTER_SEND_RESPONSE").is_some() {
            std::process::exit(93);
        }
        let response_digest = response
            .map(|v| {
                self.store
                    .publish_bytes(&serde_json::to_vec(&v)?)
                    .map(|b| b.digest)
            })
            .transpose()?;
        let mut conn = self.store.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let own = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM outbox WHERE id=?1 AND state='sending' AND attempt=?2)",
            params![a.outbox_id, a.number],
            |r| r.get::<_, bool>(0),
        )?;
        if !own {
            tx.execute("UPDATE delivery_attempts SET response_digest=?1,error='late_response_after_recovery' WHERE id=?2",params![response_digest,a.id])?;
            tx.commit()?;
            return Ok(true);
        }
        let base: u32 = tx.query_row(
            "SELECT retry_base FROM outbox WHERE id=?1",
            [&a.outbox_id],
            |r| r.get(0),
        )?;
        if state == "retryable"
            && !invalidate
            && a.number.saturating_sub(base) >= a.config.limits.max_attempts
        {
            state = "terminal";
        }
        if invalidate {
            tx.execute("DELETE FROM telegram_file_ids WHERE bot_id=?1 AND representation=?2 AND profile=?3",params![a.bot_id,prepared.representation,prepared.profile])?;
            tx.execute(
                "UPDATE outbox SET retry_base=retry_base+1 WHERE id=?1",
                [&a.outbox_id],
            )?;
        }
        let available = now().saturating_add(delay.min(i64::MAX as u64) as i64);
        if error == "rate_limited" {
            tx.execute("INSERT INTO telegram_cooldowns VALUES(?1,?2) ON CONFLICT(bot_id) DO UPDATE SET until=MAX(until,excluded.until)",params![a.bot_id,available])?;
        }

        tx.execute("UPDATE delivery_attempts SET state=?1,finished_at=?2,response_digest=?3,error=?4 WHERE id=?5",params![state,now(),response_digest,if error.is_empty(){None}else{Some(error)},a.id])?;
        tx.execute("UPDATE outbox SET state=?1,available_at=?2,lease_until=NULL,receipt=?3,error=?4 WHERE id=?5",params![state,available,confirmed.as_ref().map(serde_json::to_string).transpose()?,if error.is_empty(){None}else{Some(error)},a.outbox_id])?;
        if let Some(r) = confirmed
            && let (Some(id), Some(unique)) = (r.file_id, r.file_unique_id)
        {
            tx.execute("INSERT INTO telegram_file_ids VALUES(?1,?2,?3,?4,?5) ON CONFLICT(bot_id,representation,profile) DO UPDATE SET file_id=excluded.file_id,file_unique_id=excluded.file_unique_id",params![a.bot_id,prepared.representation,prepared.profile,id,unique])?;
        }
        tx.commit()?;
        crate::operator::log(
            "delivery_recorded",
            None,
            json!({"delivery_id":a.outbox_id,"attempt":a.number,"representation":prepared.representation,"state":state,"error_code":error}),
        );
        Ok(true)
    }
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
