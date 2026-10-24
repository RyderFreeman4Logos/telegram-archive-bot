use crate::{
    delivery::{Delivery, enqueue_tx, retry_delivery_tx},
    digest,
    jobs::{Lease, Phase, Scope, cancel_in_tx, fence, finish_in_tx, retry_in_tx, scoped_job},
    storage::{Store, now},
    telegram::Request,
};
use anyhow::{Result, ensure};
use rusqlite::{TransactionBehavior, params};
use serde_json::{Value, json};

const HELP: &str = "Send a URL, text, or a .md/.txt/.pdf file to archive it.\nReply to a historical message or file with:\n/translate — translate to the configured language\n/tts — narrate the original text\n/summarize — create a summary\nThese commands also accept inline text or a URL.\n/help\n/status [JOB]\n/cancel JOB\n/retry JOB\n/retry delivery OUTBOX\n/get JOB\n/refresh JOB\n/resend OUTBOX --accept-duplicate-risk\nUnknown sends may already have arrived. Resend can duplicate a message. In-flight sends may finish after cancellation.";
fn one<'a>(args: &[&'a str]) -> Result<&'a str> {
    ensure!(args.len() == 1, "expected one job ID");
    Ok(args[0])
}
impl Store {
    /// Control effects, response queue and command completion share one live-lease transaction.
    pub fn execute_command(&self, lease: &Lease) -> Result<Value> {
        let Request::Command { name, argument } = &lease.job.request else {
            anyhow::bail!("not a command job");
        };
        ensure!(
            lease.job.phase == Phase::Fetch && argument.len() <= 1024,
            "invalid command boundary"
        );
        let scope = Scope {
            bot_id: lease.job.bot_id,
            chat_id: lease.job.chat_id,
            user_id: lease.job.user_id,
        };
        ensure!(
            lease
                .config
                .telegram
                .authorized(scope.chat_id, scope.user_id),
            "command not authorized"
        );
        let args: Vec<_> = argument.split_whitespace().collect();
        let saved = if name == "get" {
            let id = one(&args)?;
            self.job_for(scope, id)?;
            let payloads = self
                .connection()?
                .prepare(
                    "SELECT payload FROM outbox WHERE job_id=?1 AND ordinal>=0 ORDER BY ordinal",
                )?
                .query_map([id], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for payload in &payloads {
                serde_json::from_str::<Delivery>(payload)?.validate(self, &lease.config)?;
            }
            payloads
        } else {
            vec![]
        };
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        let mut deliveries = vec![];
        let result = match name.as_str() {
            "_invalid_input" => json!({"error":"unsupported_input"}),
            "help" => {
                ensure!(args.is_empty(), "help takes no arguments");
                json!({"help":HELP})
            }
            "status" if args.is_empty() => {
                let mut q=tx.prepare("SELECT j.id,j.state,j.phase,j.error FROM jobs j JOIN inbox i ON i.bot_id=j.bot_id AND i.update_id=j.update_id WHERE j.bot_id=?1 AND i.chat_id=?2 AND i.user_id=?3 AND j.id!=?4 ORDER BY j.rowid DESC LIMIT 10")?;
                let jobs=q.query_map(params![scope.bot_id,scope.chat_id,scope.user_id,lease.job.id],|r|Ok(json!({"job":r.get::<_,String>(0)?,"state":r.get::<_,String>(1)?,"phase":r.get::<_,String>(2)?,"error":r.get::<_,Option<String>>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
                json!({"jobs":jobs})
            }
            "status" => {
                let job = scoped_job(&tx, scope, one(&args)?)?;
                let mut q = tx.prepare(
                    "SELECT id,state,attempt,error FROM outbox WHERE job_id=?1 ORDER BY ordinal LIMIT 10",
                )?;
                let outbox=q.query_map([&job.id],|r|Ok(json!({"delivery":r.get::<_,String>(0)?,"state":r.get::<_,String>(1)?,"attempts":r.get::<_,u32>(2)?,"error":r.get::<_,Option<String>>(3)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
                let total: u64 = tx.query_row(
                    "SELECT count(*) FROM outbox WHERE job_id=?1",
                    [&job.id],
                    |r| r.get(0),
                )?;
                json!({"job":job.id,"state":job.state,"phase":job.phase,"error":job.error,"deliveries":outbox,"delivery_count":total,"delivery_details_limit":10})
            }
            "cancel" => {
                let id = one(&args)?;
                ensure!(id != lease.job.id, "cannot cancel the executing command");
                let changed = cancel_in_tx(&tx, scope, id)?;
                json!({"job":id,"cancelled":changed,"in_flight_send_may_finish":true})
            }
            "retry" if args.len() == 2 && args[0] == "delivery" => {
                retry_delivery_tx(&tx, scope, args[1], false)?;
                json!({"delivery":args[1],"retry_queued":true})
            }
            "retry" => {
                let id = one(&args)?;
                let job = scoped_job(&tx, scope, id)?;
                retry_in_tx(&tx, scope, id, job.phase, now())?;
                json!({"job":id,"phase":job.phase,"retry_queued":true})
            }
            "resend" => {
                ensure!(
                    args.len() == 2 && args[1] == "--accept-duplicate-risk",
                    "resend requires OUTBOX --accept-duplicate-risk; the prior send may already have arrived"
                );
                retry_delivery_tx(&tx, scope, args[0], true)?;
                json!({"delivery":args[0],"resend_queued":true,"duplicate_risk_accepted":true})
            }
            "get" => {
                let id = one(&args)?;
                scoped_job(&tx, scope, id)?;
                let mut q = tx.prepare(
                    "SELECT payload FROM outbox WHERE job_id=?1 AND ordinal>=0 ORDER BY ordinal",
                )?;
                let payloads = q
                    .query_map([id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                ensure!(
                    payloads == saved,
                    "saved deliveries changed during validation"
                );
                ensure!(
                    !payloads.is_empty(),
                    "no saved deliveries are available for this job"
                );
                for value in payloads {
                    deliveries.push(serde_json::from_str::<Delivery>(&value)?);
                }
                json!({"job":id,"saved_deliveries_queued":deliveries.len(),"upstream_processing_repeated":false})
            }
            "refresh" => {
                let id = one(&args)?;
                let job = scoped_job(&tx, scope, id)?;
                ensure!(
                    !matches!(job.request, Request::Command { .. }),
                    "refresh requires a source job"
                );
                let new_id = digest(format!("refresh-v1:{}:{id}", lease.job.id));
                let ordinal:u64=tx.query_row("SELECT COALESCE(MAX(ordinal),0)+1 FROM jobs WHERE bot_id=?1 AND update_id=(SELECT update_id FROM jobs WHERE id=?2)",params![scope.bot_id,lease.job.id],|r|r.get(0))?;
                let config_json = serde_json::to_string(&job.request.job_config(&lease.config))?;
                let tools = serde_json::to_string(&lease.tools)?;
                let config_id = digest(format!("{config_json}\n{tools}"));
                tx.execute(
                    "INSERT INTO config_snapshots VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                    params![config_id, config_json, tools],
                )?;
                tx.execute("INSERT INTO jobs(id,bot_id,update_id,ordinal,request,config_id,state,force_run) SELECT ?1,bot_id,update_id,?2,?3,?4,'queued',1 FROM jobs WHERE id=?5",params![new_id,ordinal,serde_json::to_string(&job.request)?,config_id,lease.job.id])?;
                json!({"previous_job":id,"new_job":new_id,"force_refresh":true})
            }
            _ => anyhow::bail!("unsupported command"),
        };
        let text = if name == "_invalid_input" {
            "Unsupported input. Send a URL, text, or a .md/.txt/.pdf file. Reply to text or a supported file with /translate, /tts, or /summarize. Use /help for commands.".into()
        } else if name == "help" {
            HELP.into()
        } else {
            serde_json::to_string_pretty(&result)?
        };
        deliveries.push(Delivery::Text { text });
        ensure!(
            deliveries.len() <= lease.config.limits.graph_nodes.saturating_add(8),
            "too many saved deliveries"
        );
        // Validate all local inputs before selecting any action or response.
        // Saved files were validated outside the write transaction and rechecked above.
        for delivery in &deliveries {
            if matches!(delivery, Delivery::Text { .. }) {
                delivery.validate(self, &lease.config)?;
            }
        }
        let ids = enqueue_tx(&tx, &lease.job.id, &deliveries)?;
        let output = json!({"command":name,"result":result,"deliveries":ids});
        finish_in_tx(&tx, lease, &output, None, false, now())?;
        tx.commit()?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        jobs::{Failure, JobState},
        telegram::Update,
    };
    use std::{fs, os::unix::fs::PermissionsExt, path::Path};
    fn config() -> Config {
        let mut c = Config::default();
        c.telegram.bot_id = 1;
        c.telegram.allowed_chats = vec![2];
        c.telegram.allowed_users = vec![3, 4];
        c
    }
    fn request(store: &Store, c: &Config, update: i64, user: i64, text: &str) -> Result<Lease> {
        let update: Update = serde_json::from_value(
            json!({"update_id":update,"message":{"message_id":update,"chat":{"id":2},"from":{"id":user},"text":text}}),
        )?;
        store.ingest(c, &update, &json!({"tools":"fixture"}))?;
        Ok(store.claim_phase(Phase::Fetch)?.unwrap())
    }
    fn fixture() -> Result<(tempfile::TempDir, Store, Lease)> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))?;
        let store = Store::open(temp.path())?;
        let l = request(&store, &config(), 1, 3, "Saved source full text")?;
        Ok((temp, store, l))
    }
    fn count(store: &Store, table: &str) -> Result<i64> {
        Ok(store
            .connection()?
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
    }
    #[test]
    fn get_resends_saved_results_without_the_admission_notice() -> Result<()> {
        let (_temp, store, source) = fixture()?;
        let conn = store.connection()?;
        let tx = conn.unchecked_transaction()?;
        crate::delivery::enqueue_progress_tx(&tx, &source.job.id)?;
        tx.commit()?;
        store.enqueue_deliveries(
            &source,
            &[Delivery::Text {
                text: "Saved result".into(),
            }],
        )?;
        store.finish_phase(&source, &json!({}), None, false)?;
        let get = request(&store, &config(), 2, 3, &format!("/get {}", source.job.id))?;
        let output = store.execute_command(&get)?;
        assert_eq!(output["result"]["saved_deliveries_queued"], 1);
        let payloads: Vec<String> = store
            .connection()?
            .prepare("SELECT payload FROM outbox WHERE job_id=?1")?
            .query_map([&get.job.id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        assert!(payloads.iter().all(|p| !p.contains("Queued.")));
        Ok(())
    }
    #[test]
    fn refresh_preserves_requested_operation_with_the_new_configuration() -> Result<()> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(temp.path())?;
        let mut c = config();
        c.model.enabled = true;
        c.translation.enabled = true;
        c.synthesis.enabled = true;
        let original = request(&store, &c, 1, 3, "/tts Original source")?;
        store.finish_phase(&original, &json!({}), None, false)?;
        c.synthesis.voice = "zh-CN-XiaoxiaoNeural".into();
        let refresh = request(&store, &c, 2, 3, &format!("/refresh {}", original.job.id))?;
        let output = store.execute_command(&refresh)?;
        let refreshed = store.claim_phase(Phase::Fetch)?.unwrap();
        assert_eq!(
            refreshed.job.id,
            output["result"]["new_job"].as_str().unwrap()
        );
        assert_eq!(refreshed.job.request, original.job.request);
        assert!(refreshed.job.force && refreshed.config.synthesis.enabled);
        assert_eq!(refreshed.config.synthesis.voice, "zh-CN-XiaoxiaoNeural");
        assert!(
            !refreshed.config.model.enabled && !refreshed.config.translation.enabled,
            "refresh must not broaden a narration-only request"
        );
        Ok(())
    }

    #[test]
    fn help_status_get_and_refresh_are_scoped_and_atomically_completed() -> Result<()> {
        let (_t, store, source) = fixture()?;
        let ids = store.enqueue_deliveries(
            &source,
            &[Delivery::Text {
                text: "Saved full source".into(),
            }],
        )?;
        store.finish_phase(&source, &json!({"saved":true}), None, false)?;
        let help = request(&store, &config(), 2, 3, "/help")?;
        store.execute_command(&help)?;
        let status = request(
            &store,
            &config(),
            3,
            3,
            &format!("/status {}", source.job.id),
        )?;
        assert_eq!(
            store.execute_command(&status)?["result"]["job"],
            source.job.id
        );
        let other = request(&store, &config(), 4, 4, &format!("/get {}", source.job.id))?;
        let before = count(&store, "outbox")?;
        assert!(store.execute_command(&other).is_err());
        assert_eq!(count(&store, "outbox")?, before);
        store.fail_phase(&other, Failure::InvalidInput)?;
        let get = request(&store, &config(), 5, 3, &format!("/get {}", source.job.id))?;
        let value = store.execute_command(&get)?;
        assert_eq!(value["result"]["saved_deliveries_queued"], 1);
        assert_eq!(value["result"]["upstream_processing_repeated"], false);
        assert!(store.execute_command(&get).is_err());
        let refresh = request(
            &store,
            &config(),
            6,
            3,
            &format!("/refresh {}", source.job.id),
        )?;
        let value = store.execute_command(&refresh)?;
        let id = value["result"]["new_job"].as_str().unwrap();
        let new = store.job_for(
            Scope {
                bot_id: 1,
                chat_id: 2,
                user_id: 3,
            },
            id,
        )?;
        assert!(new.force);
        assert_eq!(new.request, source.job.request);
        assert_ne!(id, source.job.id);
        assert_eq!(
            store
                .job_for(
                    Scope {
                        bot_id: 1,
                        chat_id: 2,
                        user_id: 3
                    },
                    &source.job.id
                )?
                .state,
            JobState::Complete
        );
        assert_eq!(
            store
                .outbox_for(
                    Scope {
                        bot_id: 1,
                        chat_id: 2,
                        user_id: 3
                    },
                    &ids[0]
                )?
                .state,
            "pending"
        );
        Ok(())
    }
    #[test]
    fn cancel_retry_and_duplicate_risk_commands_preserve_outputs_and_attempts() -> Result<()> {
        let (_t, store, source) = fixture()?;
        let ids = store.enqueue_deliveries(
            &source,
            &[Delivery::Text {
                text: "Saved full source".into(),
            }],
        )?;
        let cancel = request(
            &store,
            &config(),
            2,
            3,
            &format!("/cancel {}", source.job.id),
        )?;
        store.execute_command(&cancel)?;
        assert_eq!(
            store
                .outbox_for(
                    Scope {
                        bot_id: 1,
                        chat_id: 2,
                        user_id: 3
                    },
                    &ids[0]
                )?
                .state,
            "terminal"
        );
        let retry = request(
            &store,
            &config(),
            3,
            3,
            &format!("/retry {}", source.job.id),
        )?;
        store.execute_command(&retry)?;
        let source_again = store.claim_phase(Phase::Fetch)?.unwrap();
        assert_eq!(source_again.job.id, source.job.id);
        store.fail_phase(&source_again, Failure::InvalidInput)?;
        store.connection()?.execute(
            "UPDATE outbox SET state='unknown',error='fixture_response_lost' WHERE id=?1",
            [&ids[0]],
        )?;
        let bad = request(
            &store,
            &config(),
            4,
            3,
            &format!("/retry delivery {}", ids[0]),
        )?;
        assert!(store.execute_command(&bad).is_err());
        store.fail_phase(&bad, Failure::InvalidInput)?;
        let bad = request(&store, &config(), 5, 3, &format!("/resend {}", ids[0]))?;
        assert!(store.execute_command(&bad).is_err());
        store.fail_phase(&bad, Failure::InvalidInput)?;
        let resend = request(
            &store,
            &config(),
            6,
            3,
            &format!("/resend {} --accept-duplicate-risk", ids[0]),
        )?;
        let out = store.execute_command(&resend)?;
        assert_eq!(out["result"]["duplicate_risk_accepted"], true);
        assert_eq!(
            store
                .outbox_for(
                    Scope {
                        bot_id: 1,
                        chat_id: 2,
                        user_id: 3
                    },
                    &ids[0]
                )?
                .state,
            "pending"
        );
        Ok(())
    }
    #[test]
    fn command_effects_rollback_when_final_response_cannot_be_queued() -> Result<()> {
        let (_t, store, source) = fixture()?;
        store.finish_phase(&source, &json!({}), None, false)?;
        let refresh = request(
            &store,
            &config(),
            2,
            3,
            &format!("/refresh {}", source.job.id),
        )?;
        // Fail the queue insertion after the refresh child and snapshot were inserted.
        store.connection()?.execute_batch("CREATE TRIGGER reject_response BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'fixture queue failure'); END;")?;
        let before = count(&store, "jobs")?;
        let snapshots = count(&store, "config_snapshots")?;
        assert!(store.execute_command(&refresh).is_err());
        assert_eq!(count(&store, "jobs")?, before);
        assert_eq!(count(&store, "config_snapshots")?, snapshots);
        assert!(store.phase_outputs(&refresh.job.id)?.is_empty());
        Ok(())
    }
    #[test]
    fn swapped_existing_snapshot_request_or_force_cannot_fence_another_job() -> Result<()> {
        let (_t, store, source) = fixture()?;
        let mut config = config();
        config.model.language = "en".into();
        let other = request(&store, &config, 2, 3, "another request")?;
        let mut swapped = source.clone();
        swapped.config = other.config.clone();
        swapped.tools = other.tools.clone();
        swapped.job.config_id = other.job.config_id.clone();
        assert!(store.heartbeat(&swapped).is_err());
        let mut swapped = source.clone();
        swapped.job.request = other.job.request;
        assert!(store.heartbeat(&swapped).is_err());
        let mut swapped = source;
        swapped.job.force = true;
        assert!(store.heartbeat(&swapped).is_err());
        Ok(())
    }
}
