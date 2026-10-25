//! DA06 / #73: automatic, durable Notion publication.
use crate::{
    config::Config,
    notion::{Adapter, ReadinessState, readiness},
    storage::Store,
};
use tokio_util::sync::CancellationToken;

/// A config change cancels and settles the old operation before taking a new snapshot.
pub(crate) async fn serve(
    store: Store,
    config: std::sync::Arc<tokio::sync::RwLock<crate::config::ConfigManager>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    while !shutdown.is_cancelled() {
        let (snapshot, changed) = config.read().await.notion_snapshot();
        let cancel = shutdown.child_token();
        let operation = tick(&store, &snapshot, &cancel);
        tokio::pin!(operation);
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => { cancel.cancel(); operation.await },
            _ = changed.cancelled() => { cancel.cancel(); operation.await },
            result = &mut operation => result,
        };
        if result.is_err() {
            crate::operator::log(
                "notion_worker_error",
                None,
                serde_json::json!({"reason":"local-publication-state"}),
            );
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = changed.cancelled() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
        }
    }
    Ok(())
}

pub async fn tick(
    store: &Store,
    config: &Config,
    cancel: &CancellationToken,
) -> anyhow::Result<bool> {
    let Some(_guard) = store.notion_lock()? else {
        return Ok(false);
    };
    let destination = store.notion_configure(&config.notion)?;
    store.notion_bootstrap(64)?;
    if cancel.is_cancelled()
        || store.recovery_paused()?
        || readiness(&config.notion).state != ReadinessState::Ready
    {
        return Ok(false);
    }
    let Some(destination) = destination else {
        return Ok(false);
    };
    let Some(mut intent) = store.notion_next(&destination, &config.notion)? else {
        return Ok(false);
    };
    let record = match store.notion_record(&intent, config.notion.record_bytes) {
        Ok(record) if record.plan(&config.notion).is_ok() => record,
        _ => {
            store.notion_invalid_record(&intent)?;
            return Ok(true);
        }
    };
    store.notion_begin(&mut intent, &record, &config.notion)?;
    let adapter = Adapter::new(&config.notion, &store.root().join("work"));
    let progress = |progress: &crate::notion::PublicationProgress| {
        if intent.candidate_page_id.is_some() {
            return Ok(());
        }
        store
            .notion_progress(&intent, progress)
            .map_err(|_| crate::notion::PublicationError {
                state: crate::notion::Failure::Reconcile,
                page_id: Some(progress.page_id.clone()),
                retry_after_seconds: None,
            })
    };
    let mut result = if intent.may_have_remote {
        adapter
            .reconcile(
                intent
                    .page_id
                    .as_deref()
                    .or(intent.candidate_page_id.as_deref()),
                &record,
                cancel,
                &progress,
            )
            .await
    } else {
        adapter
            .create_observed(&record, cancel, &progress)
            .await
            .map(Some)
    };
    if intent.candidate_page_id.is_some()
        && let Err(failure) = &mut result
    {
        failure.page_id = None; // An operator suggestion becomes known only after full verification.
    }
    store.notion_settle(&intent, &config.notion, &result)?;
    Ok(true)
}
