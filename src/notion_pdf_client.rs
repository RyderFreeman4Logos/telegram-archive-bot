//! DA07 shares the pinned Tool transport. No write is retried after an unknown append.
use super::*;
use crate::{
    network::PublicHttp,
    notion_pdf::{MAX_PDF_BYTES, PdfJob, PdfState},
    storage::now,
};

type PdfObserver<'a> = dyn Fn(&PdfState) -> Result<(), PublicationError> + Send + Sync + 'a;
fn marker(id: &str) -> String {
    format!("DA07 PDF {id}")
}
fn status_marker(id: &str) -> String {
    format!("DA07 availability {id}")
}
fn text(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|v| {
                    v["text"]["content"]
                        .as_str()
                        .or_else(|| v["plain_text"].as_str())
                })
                .collect()
        })
        .unwrap_or_default()
}
fn id(value: &Value) -> Option<String> {
    value["id"].as_str().and_then(notion_id)
}
fn expiry(value: &Value) -> Option<i64> {
    value["expiry_time"]
        .as_str()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|t| t.timestamp())
}
fn paragraph(content: &str) -> Value {
    json!({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":content}}]}})
}
impl Operation<'_> {
    async fn pdf_children(&mut self, page: &str) -> Result<Vec<Value>, PublicationError> {
        let mut all = Vec::new();
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        loop {
            let value = self
                .request(
                    &format!("v1/blocks/{page}/children"),
                    "GET",
                    None,
                    cursor.as_deref(),
                )
                .await?;
            let rows = value["results"]
                .as_array()
                .filter(|v| v.len() <= 100)
                .ok_or_else(|| self.error(Failure::InvalidOutput))?;
            if value["object"] != "list" || all.len().saturating_add(rows.len()) > 10000 {
                return Err(self.error(Failure::Limit));
            }
            all.extend(rows.iter().cloned());
            if value["has_more"] == false {
                return Ok(all);
            }
            let next = value["next_cursor"]
                .as_str()
                .filter(|v| !v.is_empty() && v.len() <= 512 && !v.chars().any(char::is_control))
                .ok_or_else(|| self.error(Failure::InvalidOutput))?;
            if value["has_more"] != true || rows.is_empty() || !seen.insert(next.to_owned()) {
                return Err(self.error(Failure::InvalidOutput));
            }
            cursor = Some(next.to_owned());
        }
    }
    async fn pdf_send(&mut self, upload: &str, bytes: &[u8]) -> Result<Value, PublicationError> {
        let path = self.work.path().join("original.pdf");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| self.error(Failure::Unavailable))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| self.error(Failure::Unavailable))?;
        drop(file);
        self.work_bytes = MAX_PDF_BYTES + 4 * 1024 * 1024;
        let result = self
            .tool(
                vec![
                    "api".into(),
                    format!("v1/file_uploads/{upload}/send").into(),
                    "-X".into(),
                    "POST".into(),
                    "--file".into(),
                    path.clone().into(),
                    "--notion-version".into(),
                    API_VERSION.into(),
                ],
                true,
            )
            .await;
        std::fs::remove_file(&path).map_err(|_| self.uncertain(Failure::Unavailable))?;
        let value: Value =
            serde_json::from_str(&result?).map_err(|_| self.uncertain(Failure::InvalidOutput))?;
        if let Some(error) = self.remote_error(&value) {
            return Err(error);
        }
        Ok(value)
    }
    async fn pdf_proof(
        &mut self,
        block: &str,
        expected_marker: &str,
        job: &PdfJob,
        http: &PublicHttp,
    ) -> Result<(), PublicationError> {
        let value = self
            .request(&format!("v1/blocks/{block}"), "GET", None, None)
            .await?;
        if id(&value).as_deref() != Some(block)
            || value["type"] != "pdf"
            || value["archived"] == true
            || value["in_trash"] == true
            || text(&value["pdf"], "caption") != expected_marker
        {
            return Err(self.error(Failure::Conflict));
        }
        let url = value["pdf"]["file"]["url"]
            .as_str()
            .filter(|_| value["pdf"]["type"] == "file")
            .ok_or_else(|| self.error(Failure::Reconcile))?;
        let remaining = self
            .deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(self.adapter.config.request_seconds));
        let response = http
            .get(url, MAX_PDF_BYTES, remaining, None, self.cancel)
            .await
            .map_err(|_| self.error(Failure::Unavailable))?;
        if !(200..300).contains(&response.status)
            || Some(response.bytes.len() as u64) != job.original.bytes
            || crate::digest(&response.bytes) != job.original.sha256
        {
            return Err(self.error(Failure::Conflict));
        }
        Ok(())
    }
    async fn pdf_status(
        &mut self,
        key: &str,
        job: &PdfJob,
        state: &mut PdfState,
        persist: &PdfObserver<'_>,
    ) -> Result<(), PublicationError> {
        let caption = status_marker(key);
        let content = format!(
            "{caption}\n{}",
            json!({"schema_version":1,"source_id":job.original.source_id,"revision_id":job.original.revision_id,"sha256":job.original.sha256,"page_id":job.text_page.page_id,"block_id":state.block_id,"upload_id":state.upload_id,"upload_expiry":state.upload_expiry,"upload":state.upload_state,"attachment":state.attachment_state,"verified_sha256":state.verified_sha256,"verified_at":state.verified_at,"gap":state.reason,"file_access_guaranteed":false,"fallback":"Original/landing URLs and extracted text are in this page's PDF original record. File tools and external origin access are best-effort."})
        );
        if content.encode_utf16().count() > 2000 {
            return Err(self.error(Failure::Limit));
        }
        let paragraph = paragraph(&content);
        if let Some(block) = &state.status_block_id {
            self.request(
                &format!("v1/blocks/{block}"),
                "PATCH",
                Some(&json!({"paragraph":paragraph["paragraph"]})),
                None,
            )
            .await?;
            let proof = self
                .request(&format!("v1/blocks/{block}"), "GET", None, None)
                .await?;
            if id(&proof).as_ref() != Some(block)
                || text(&proof["paragraph"], "rich_text") != content
            {
                return Err(self.error(Failure::Conflict));
            }
        } else {
            if state.status_dispatched {
                return Err(self.error(Failure::Reconcile));
            }
            state.status_dispatched = true;
            persist(state)?;
            let value = self
                .request(
                    &format!("v1/blocks/{}/children", job.text_page.page_id),
                    "PATCH",
                    Some(&json!({"children":[paragraph]})),
                    None,
                )
                .await;
            match value {
                Ok(value) => {
                    state.status_block_id = value["results"]
                        .as_array()
                        .filter(|v| v.len() == 1)
                        .and_then(|v| id(&v[0]));
                    persist(state)?;
                    if state.status_block_id.is_none() {
                        return Err(self.error(Failure::Reconcile));
                    }
                }
                Err(error) => {
                    if matches!(
                        error.state,
                        Failure::BlockedAuth | Failure::RetryWait | Failure::InvalidPayload
                    ) {
                        state.status_dispatched = false;
                        persist(state)?;
                    }
                    return Err(error);
                }
            }
            let block = state
                .status_block_id
                .as_ref()
                .ok_or_else(|| self.error(Failure::Reconcile))?;
            let proof = self
                .request(&format!("v1/blocks/{block}"), "GET", None, None)
                .await?;
            if id(&proof).as_ref() != Some(block)
                || text(&proof["paragraph"], "rich_text") != content
            {
                return Err(self.error(Failure::Conflict));
            }
        }
        Ok(())
    }
}
impl Adapter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_pdf(
        &self,
        key: &str,
        job: &PdfJob,
        state: &mut PdfState,
        raw: Option<&[u8]>,
        allow_write: bool,
        cancel: &CancellationToken,
        http: &PublicHttp,
        persist: &PdfObserver<'_>,
    ) -> Result<(), PublicationError> {
        let noop = |_: &PublicationProgress| Ok(());
        let mut op = Operation::new(self, cancel, &noop).await?;
        let page = &job.text_page.page_id;
        op.page_id = Some(page.clone());
        let result = async {
  let destination=self.config.destination.as_ref().ok_or(Failure::PendingConfiguration)?;
  if destination.identity().as_deref()!=Some(&job.text_page.destination_key){return Err(op.error(Failure::Conflict))}
  let p=op.request(&format!("v1/pages/{page}"),"GET",None,None).await?;
  let (field,parent)=match destination {NotionDestination::Page{id}=>("page_id",id),NotionDestination::DataSource{id,..}=>("data_source_id",id)};
  if id(&p).as_ref()!=Some(page)||p["object"]!="page"||p["archived"]==true||p["in_trash"]==true||p["parent"][field].as_str().and_then(notion_id)!=notion_id(parent){return Err(op.error(Failure::Conflict))}
  // Complete scan before the first write also protects semantic restores whose local outbox is new.
  let blocks=op.pdf_children(page).await?;let marker=marker(key);let status=status_marker(key);
  let files:Vec<_>=blocks.iter().filter(|v|v["type"]=="pdf"&&text(&v["pdf"],"caption")==marker).collect();
  let statuses:Vec<_>=blocks.iter().filter(|v|v["type"]=="paragraph"&&text(&v["paragraph"],"rich_text").starts_with(&format!("{status}\n"))).collect();
  if files.len()>1||statuses.len()>1{return Err(op.error(Failure::Conflict))}
  if let Some(value)=statuses.first(){state.status_block_id=id(value);state.status_dispatched=true;persist(state)?;}
  if let Some(value)=files.first(){let found=id(value).ok_or_else(||op.error(Failure::InvalidOutput))?;if state.block_id.as_ref().is_some_and(|old|old!=&found){return Err(op.error(Failure::Conflict))}state.block_id=Some(found);state.attachment_dispatched=true;persist(state)?;}
  if let Some(block)=state.block_id.clone(){
   state.attachment_state="unverified".into();state.verified_sha256=None;state.verified_at=None;persist(state)?;
   op.pdf_proof(&block,&marker,job,http).await?;state.attachment_state="verified".into();state.verified_sha256=Some(job.original.sha256.clone());state.verified_at=Some(now());state.reason=None;persist(state)?;
   if !state.read_only{op.pdf_status(key,job,state,persist).await?;}return Ok(())
  }
  if state.read_only {state.reason=Some("read_only_reconciliation_file_not_visible".into());persist(state)?;return Err(op.error(Failure::Reconcile))}
  if state.attachment_dispatched {state.attachment_state="unknown".into();state.reason=Some("append_outcome_unknown_read_only_reconciliation".into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Err(op.error(Failure::Reconcile))}
  if state.status_dispatched&&state.status_block_id.is_none(){return Err(op.error(Failure::Reconcile))}
  if !allow_write {state.reason=Some("attempt_limit_operator_reconcile_required".into());persist(state)?;return Err(op.error(Failure::Limit))}
  if raw.is_none(){state.upload_state="unavailable".into();state.attachment_state="unavailable".into();state.reason=Some(if job.original.bytes.is_some_and(|n|n>MAX_PDF_BYTES){"single_part_size_limit"}else{"retained_validated_original_unavailable"}.into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Ok(())}
  let raw=raw.unwrap();if !raw.starts_with(b"%PDF-") || crate::digest(raw)!=job.original.sha256{return Err(op.error(Failure::Conflict))}
  op.pdf_status(key,job,state,persist).await?;
  let upload=if let Some(upload)=&state.upload_id {
   let value=op.request(&format!("v1/file_uploads/{upload}"),"GET",None,None).await?;
   if id(&value).as_ref()!=Some(upload){return Err(op.error(Failure::Conflict))}
   if value["status"]=="expired"||expiry(&value).is_some_and(|e|e<=now()){
    state.upload_state="expired".into();state.upload_id=None;state.upload_expiry=None;state.reason=Some("expired_unattached_upload_retry_from_retained_original".into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Err(op.error(Failure::RetryWait))
   }value
  }else {
   if state.upload_state=="creating"{state.reason=Some("upload_creation_outcome_unknown".into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Err(op.error(Failure::Reconcile))}
   state.upload_state="creating".into();persist(state)?;
   // Filename is a label, never a path. Stable ASCII avoids leaking local CAS paths.
   let filename=format!("{}.pdf",job.original.sha256);
   let result=op.request("v1/file_uploads","POST",Some(&json!({"mode":"single_part","filename":filename,"content_type":"application/pdf"})),None).await;
   match result {
    Ok(value)=>{state.upload_id=id(&value);state.upload_expiry=expiry(&value);persist(state)?;if state.upload_id.is_none(){return Err(op.error(Failure::Reconcile))}value}
    Err(error)=>{if error.state==Failure::InvalidPayload{state.upload_state="unsupported_or_rejected".into();state.attachment_state="unavailable".into();state.reason=Some("upload_endpoint_unsupported_or_rejected".into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Ok(())}else if matches!(error.state,Failure::BlockedAuth|Failure::RetryWait){state.upload_state="pending".into();persist(state)?;}return Err(error)}
   }
  };
  let upload_id=state.upload_id.clone().ok_or(Failure::Reconcile)?;
  let uploaded=if upload["status"]=="uploaded"{upload}else if upload["status"]=="pending"{
   state.upload_state="sending".into();persist(state)?;op.pdf_send(&upload_id,raw).await?
  }else {state.upload_state="failed".into();state.reason=Some("upload_not_ready".into());persist(state)?;op.pdf_status(key,job,state,persist).await?;return Err(op.error(Failure::RetryWait))};
  if id(&uploaded).as_ref()!=Some(&upload_id)||uploaded["status"]!="uploaded"||uploaded["content_length"].as_u64()!=job.original.bytes||uploaded["content_type"]!="application/pdf"{return Err(op.error(Failure::InvalidOutput))}
  state.upload_expiry=expiry(&uploaded);state.upload_state="uploaded".into();persist(state)?;
  if state.upload_expiry.is_some_and(|e|e<=now()){state.upload_id=None;state.upload_state="expired".into();persist(state)?;return Err(op.error(Failure::RetryWait))}
  state.attachment_dispatched=true;state.attachment_state="attaching".into();persist(state)?;
  let value=op.request(&format!("v1/blocks/{page}/children"),"PATCH",Some(&json!({"children":[{"object":"block","type":"pdf","pdf":{"type":"file_upload","file_upload":{"id":upload_id},"caption":[{"type":"text","text":{"content":marker}}]}}]})),None).await;
  match value {
   Ok(value)=>{state.block_id=value["results"].as_array().filter(|v|v.len()==1).and_then(|v|id(&v[0]));persist(state)?;}
   Err(error)=>{if matches!(error.state,Failure::BlockedAuth|Failure::RetryWait|Failure::InvalidPayload){state.attachment_dispatched=false;state.attachment_state="pending".into();}else{state.attachment_state="unknown".into();}state.reason=Some("attachment_not_verified".into());persist(state)?;let _=op.pdf_status(key,job,state,persist).await;return Err(error)}
  }
  let block=state.block_id.clone().ok_or_else(||op.error(Failure::Reconcile))?;
  op.pdf_proof(&block,&marker,job,http).await?;state.attachment_state="verified".into();state.verified_sha256=Some(job.original.sha256.clone());state.verified_at=Some(now());state.reason=None;persist(state)?;
  op.pdf_status(key,job,state,persist).await?;Ok(())
  }.await;
        if let Err(error) = &result {
            if state.reason.is_none() {
                state.reason = Some(
                    serde_json::to_value(error.state)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unavailable".into()),
                );
            }
            persist(state)?;
            // Best-effort truthful status never replaces the original failure or authorizes append retry.
            if !state.read_only {
                let _ = op.pdf_status(key, job, state, persist).await;
            }
        }
        result
    }
}
