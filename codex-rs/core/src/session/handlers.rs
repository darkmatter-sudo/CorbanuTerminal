use crate::realtime_conversation::handle_audio as handle_realtime_conversation_audio;
use crate::realtime_conversation::handle_close as handle_realtime_conversation_close;
use crate::realtime_conversation::handle_speech as handle_realtime_conversation_speech;
use crate::realtime_conversation::handle_start as handle_realtime_conversation_start;
use crate::realtime_conversation::handle_text as handle_realtime_conversation_text;
use async_channel::Receiver;
use codex_otel::set_parent_from_w3c_trace_context;
use codex_protocol::protocol::Submission;
use tracing::Instrument;
use tracing::debug_span;
use tracing::info_span;

use crate::session::SteerInputError;
use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::session::SessionSettingsUpdate;

use crate::config::Config;
use crate::review_prompts::resolve_review_request;
use crate::session::spawn_review_thread;
use crate::tasks::CompactTask;
use crate::tasks::UserShellCommandMode;
use crate::tasks::UserShellCommandTask;
use crate::tasks::execute_user_shell_command;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::GuardianAssessmentEvent;
use codex_protocol::protocol::GuardianAssessmentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RealtimeConversationListVoicesResponseEvent;
use codex_protocol::protocol::RealtimeVoicesList;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use codex_protocol::request_user_input::RequestUserInputResponse;
use codex_protocol::user_input::UserInput;

use crate::context_manager::is_user_turn_boundary;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::mcp::RequestId as ProtocolRequestId;
use codex_protocol::protocol::AdditionalContextEntry;
use codex_rmcp_client::ElicitationAction;
use codex_rmcp_client::ElicitationResponse;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;
use tracing::debug;
use tracing::info;
use tracing::warn;

#[cfg(test)]
struct ActiveTurnNotSteerableDeferHook {
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
static ACTIVE_TURN_NOT_STEERABLE_DEFER_HOOK: OnceLock<
    Mutex<Option<ActiveTurnNotSteerableDeferHook>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) fn set_active_turn_not_steerable_defer_hook_for_test(
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    let hook = ACTIVE_TURN_NOT_STEERABLE_DEFER_HOOK.get_or_init(|| Mutex::new(None));
    *hook.lock().expect("defer hook lock") =
        Some(ActiveTurnNotSteerableDeferHook { reached, release });
}

#[cfg(test)]
async fn active_turn_not_steerable_defer_hook_for_test() {
    let hook = ACTIVE_TURN_NOT_STEERABLE_DEFER_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("defer hook lock")
        .take();
    if let Some(hook) = hook {
        let _ = hook.reached.send(());
        let _ = hook.release.await;
    }
}

pub async fn interrupt(sess: &Arc<Session>) {
    sess.interrupt_task().await;
}

pub async fn clean_background_terminals(sess: &Arc<Session>) {
    sess.close_unified_exec_processes().await;
}

pub async fn realtime_conversation_list_voices(sess: &Session, sub_id: String) {
    sess.send_event_raw(Event {
        id: sub_id,
        msg: EventMsg::RealtimeConversationListVoicesResponse(
            RealtimeConversationListVoicesResponseEvent {
                voices: RealtimeVoicesList::builtin(),
            },
        ),
    })
    .await;
}

pub async fn user_input_or_turn(
    sess: &Arc<Session>,
    sub_id: String,
    op: Op,
    client_user_message_id: Option<String>,
    parent_turn_id: Option<String>,
) {
    user_input_or_turn_inner(sess, sub_id, op, client_user_message_id, parent_turn_id).await;
}

pub async fn update_thread_settings(
    sess: &Arc<Session>,
    sub_id: String,
    thread_settings: ThreadSettingsOverrides,
) {
    let updates = thread_settings_update(sess, thread_settings).await;
    match sess.update_settings(updates).await {
        Ok(()) => {
            sess.send_event_raw_without_materializing_rollout(Event {
                id: sub_id,
                msg: thread_settings_applied_event(sess).await,
            })
            .await;
        }
        Err(err) => {
            sess.send_event_raw(Event {
                id: sub_id,
                msg: EventMsg::Error(ErrorEvent {
                    message: format!("invalid thread settings override: {err}"),
                    codex_error_info: Some(CodexErrorInfo::BadRequest),
                }),
            })
            .await;
        }
    }
}

async fn thread_settings_update(
    sess: &Session,
    thread_settings: ThreadSettingsOverrides,
) -> SessionSettingsUpdate {
    let ThreadSettingsOverrides {
        environments,
        profile_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        model,
        model_provider,
        effort,
        summary,
        service_tier,
        collaboration_mode,
        personality,
    } = thread_settings;
    let collaboration_mode = match collaboration_mode {
        Some(collaboration_mode) => collaboration_mode,
        None => {
            let state = sess.state.lock().await;
            // Model and reasoning effort live in CollaborationMode settings today, so
            // partial thread-settings updates refresh those fields on the active mode.
            state
                .session_configuration
                .collaboration_mode
                .with_updates(model, effort, /*developer_instructions*/ None)
        }
    };
    SessionSettingsUpdate {
        environments,
        profile_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        collaboration_mode: Some(collaboration_mode),
        model_provider,
        reasoning_summary: summary,
        service_tier,
        personality,
        ..Default::default()
    }
}

pub(super) async fn thread_settings_applied_event(sess: &Session) -> EventMsg {
    let snapshot = {
        let state = sess.state.lock().await;
        state.session_configuration.thread_config_snapshot()
    };
    EventMsg::ThreadSettingsApplied(ThreadSettingsAppliedEvent {
        thread_settings: snapshot.into_thread_settings_snapshot(),
    })
}

pub(super) async fn user_input_or_turn_inner(
    sess: &Arc<Session>,
    sub_id: String,
    op: Op,
    client_user_message_id: Option<String>,
    parent_turn_id: Option<String>,
) {
    let Op::UserInput {
        items,
        final_output_json_schema,
        responsesapi_client_metadata,
        additional_context,
        thread_settings,
    } = op
    else {
        unreachable!();
    };
    if !items.is_empty() {
        sess.services
            .agent_control
            .note_native_operator_input(sess.thread_id);
    }
    let emit_thread_settings_applied = thread_settings != ThreadSettingsOverrides::default();
    let mut updates = if emit_thread_settings_applied {
        thread_settings_update(sess, thread_settings).await
    } else {
        SessionSettingsUpdate::default()
    };
    updates.final_output_json_schema = Some(final_output_json_schema);

    let Ok(current_context) = sess.new_turn_with_sub_id(sub_id.clone(), updates).await else {
        // new_turn_with_sub_id already emits the error event.
        return;
    };
    if emit_thread_settings_applied {
        sess.send_event_raw_without_materializing_rollout(Event {
            id: sub_id.clone(),
            msg: thread_settings_applied_event(sess).await,
        })
        .await;
    }
    sess.maybe_emit_model_warnings_for_turn(current_context.as_ref())
        .await;
    match sess
        .steer_input(
            items.clone(),
            additional_context.clone(),
            /*expected_turn_id*/ None,
            client_user_message_id.clone(),
            responsesapi_client_metadata.clone(),
        )
        .await
    {
        Ok(_) => {
            current_context.session_telemetry.user_prompt(&items);
        }
        Err(SteerInputError::NoActiveTurn(items)) => {
            if let Some(id) = parent_turn_id {
                current_context.turn_metadata_state.set_parent_turn_id(id);
            }
            spawn_regular_turn_for_no_active_input(
                sess,
                &current_context,
                items,
                additional_context,
                client_user_message_id,
                responsesapi_client_metadata,
                /*record_user_prompt*/ true,
            )
            .await;
        }
        Err(
            err @ (SteerInputError::AuthorizationChanged(_)
            | SteerInputError::ActiveTurnNotSteerable { .. }),
        ) => {
            let items = match err {
                SteerInputError::AuthorizationChanged(items) => items,
                _ => items,
            };
            #[cfg(test)]
            active_turn_not_steerable_defer_hook_for_test().await;

            match sess
                .defer_user_input_until_active_turn_finished(
                    items.clone(),
                    additional_context.clone(),
                    client_user_message_id.clone(),
                    current_context.final_output_json_schema.clone(),
                )
                .await
            {
                Ok(_) => {
                    current_context.session_telemetry.user_prompt(&items);
                }
                Err(SteerInputError::NoActiveTurn(items)) => {
                    let Ok(current_context) = sess
                        .new_turn_with_sub_id(
                            sub_id.clone(),
                            SessionSettingsUpdate {
                                final_output_json_schema: Some(
                                    current_context.final_output_json_schema.clone(),
                                ),
                                ..Default::default()
                            },
                        )
                        .await
                    else {
                        return;
                    };
                    if let Some(id) = parent_turn_id {
                        current_context.turn_metadata_state.set_parent_turn_id(id);
                    }
                    spawn_regular_turn_for_no_active_input(
                        sess,
                        &current_context,
                        items,
                        additional_context,
                        client_user_message_id,
                        responsesapi_client_metadata,
                        /*record_user_prompt*/ true,
                    )
                    .await;
                }
                Err(err) => {
                    sess.send_event_raw(Event {
                        id: sub_id,
                        msg: EventMsg::Error(err.to_error_event()),
                    })
                    .await;
                }
            }
        }
        Err(err) => {
            sess.send_event_raw(Event {
                id: sub_id,
                msg: EventMsg::Error(err.to_error_event()),
            })
            .await;
        }
    }
}

async fn spawn_regular_turn_for_no_active_input(
    sess: &Arc<Session>,
    current_context: &Arc<crate::session::turn_context::TurnContext>,
    items: Vec<UserInput>,
    additional_context: BTreeMap<String, AdditionalContextEntry>,
    client_user_message_id: Option<String>,
    responsesapi_client_metadata: Option<HashMap<String, String>>,
    record_user_prompt: bool,
) {
    if let Some(responsesapi_client_metadata) = responsesapi_client_metadata {
        current_context
            .turn_metadata_state
            .set_responsesapi_client_metadata(responsesapi_client_metadata);
    }
    if record_user_prompt {
        current_context.session_telemetry.user_prompt(&items);
    }
    sess.refresh_mcp_if_dirty().await;
    let additional_context_input = {
        let mut state = sess.state.lock().await;
        state.additional_context.merge(additional_context)
    };
    let mut task_input = additional_context_input
        .into_iter()
        .map(ResponseItem::from)
        .map(TurnInput::ResponseItem)
        .collect::<Vec<_>>();
    // Queue-only collaboration mail must join the next admitted turn before its first model
    // request. Leaving it in the session mailbox until after the user's request has already been
    // sampled creates an unnecessary continuation and can loop when that continuation has no new
    // model output. The submission loop serializes this idle-to-active boundary, so draining here
    // also preserves mailbox order relative to the newly admitted user input.
    let (mailbox_input, mailbox_parent_turn_id) =
        sess.input_queue.drain_ready_mailbox_input_items().await;
    task_input.extend(mailbox_input);
    if let Some(parent_turn_id) = mailbox_parent_turn_id {
        current_context
            .turn_metadata_state
            .set_parent_turn_id(parent_turn_id);
    }
    if !items.is_empty() {
        task_input.push(TurnInput::UserInput {
            content: items,
            client_id: client_user_message_id,
        });
    }
    sess.spawn_task(
        Arc::clone(current_context),
        task_input,
        crate::tasks::RegularTask::new(),
    )
    .await;
}

/// Queues an inter-agent message, then lets the shared pending-work scheduler
/// decide whether an idle session should start a regular turn.
pub async fn inter_agent_communication(
    sess: &Arc<Session>,
    sub_id: String,
    communication: InterAgentCommunication,
    parent_turn_id: Option<String>,
) {
    let trigger_turn = communication.trigger_turn;
    sess.input_queue
        .enqueue_mailbox_communication_for_session(
            sess,
            communication,
            parent_turn_id.filter(|_| trigger_turn),
        )
        .await;
    crate::agent_communication::emit_agent_communication_receive(&sub_id);
    if trigger_turn || sess.has_outstanding_durable_sleep() {
        sess.maybe_start_turn_for_pending_work_with_sub_id(sub_id)
            .await;
    }
}

pub async fn run_user_shell_command(sess: &Arc<Session>, sub_id: String, command: String) {
    if let Some((turn_context, cancellation_token)) =
        sess.active_turn_context_and_cancellation_token().await
    {
        let session = Arc::clone(sess);
        tokio::spawn(async move {
            execute_user_shell_command(
                session,
                turn_context,
                command,
                cancellation_token,
                UserShellCommandMode::ActiveTurnAuxiliary,
            )
            .await;
        });
        return;
    }

    let turn_context = sess.new_default_turn_with_sub_id(sub_id).await;
    sess.spawn_task(
        Arc::clone(&turn_context),
        Vec::new(),
        UserShellCommandTask::new(command),
    )
    .await;
}

pub async fn resolve_elicitation(
    sess: &Arc<Session>,
    server_name: String,
    request_id: ProtocolRequestId,
    decision: codex_protocol::approvals::ElicitationAction,
    content: Option<Value>,
    meta: Option<Value>,
) {
    let action = match decision {
        codex_protocol::approvals::ElicitationAction::Accept => ElicitationAction::Accept,
        codex_protocol::approvals::ElicitationAction::Decline => ElicitationAction::Decline,
        codex_protocol::approvals::ElicitationAction::Cancel => ElicitationAction::Cancel,
    };
    let content = match action {
        // Preserve the legacy fallback for clients that only send an action.
        ElicitationAction::Accept => Some(content.unwrap_or_else(|| serde_json::json!({}))),
        ElicitationAction::Decline | ElicitationAction::Cancel => None,
        _ => None,
    };
    let response = ElicitationResponse {
        action,
        content,
        meta,
    };
    let request_id = match request_id {
        ProtocolRequestId::String(value) => {
            rmcp::model::NumberOrString::String(std::sync::Arc::from(value))
        }
        ProtocolRequestId::Integer(value) => rmcp::model::NumberOrString::Number(value),
    };
    if let Err(err) = sess
        .resolve_elicitation(server_name, request_id, response)
        .await
    {
        warn!(
            error = %err,
            "failed to resolve elicitation request in session"
        );
    }
}

/// Propagate a user's exec approval decision to the session.
/// Also optionally applies an execpolicy amendment.
pub async fn exec_approval(
    sess: &Arc<Session>,
    approval_id: String,
    turn_id: Option<String>,
    decision: ReviewDecision,
) {
    let event_turn_id = turn_id.unwrap_or_else(|| approval_id.clone());
    let mut stale_amendment = false;
    if let ReviewDecision::ApprovedExecpolicyAmendment {
        proposed_execpolicy_amendment,
    } = &decision
    {
        // #945: only an answer to an open prompt may save a rule. One with no
        // open prompt (its turn ended) is dropped by `notify_approval` anyway.
        let persisted = match sess.pending_approval_shown_under(&approval_id).await {
            Some(shown_under) => Some(
                sess.persist_execpolicy_amendment(proposed_execpolicy_amendment, shown_under)
                    .await,
            ),
            None => {
                tracing::warn!(
                    "not saving a \"don't ask again\" rule for {approval_id}: no prompt is open"
                );
                None
            }
        };
        match persisted {
            None => {}
            // #945: the prompt was shown before a security transition. The
            // rule is not saved; D50 (#1001): nor does the answer approve the
            // command, which is asked about again under the new level.
            Some(Ok(None)) => {
                let message = "The security level changed while this approval was open, so \
                               \"don't ask again\" was not saved and the answer was not used."
                    .to_string();
                sess.send_event_raw(Event {
                    id: event_turn_id.clone(),
                    msg: EventMsg::Warning(WarningEvent { message }),
                })
                .await;
                stale_amendment = true;
            }
            Some(Ok(Some(crate::exec_policy::AmendmentScope::Saved))) => {
                sess.record_execpolicy_amendment_message(
                    &event_turn_id,
                    proposed_execpolicy_amendment,
                )
                .await;
            }
            Some(Ok(Some(crate::exec_policy::AmendmentScope::Session))) => {
                let message = "This command contains a credential, so the approval applies to \
                               this session only and was not saved to your rules."
                    .to_string();
                let warning = EventMsg::Warning(WarningEvent { message });
                sess.send_event_raw(Event {
                    id: event_turn_id.clone(),
                    msg: warning,
                })
                .await;
            }
            Some(Err(err)) => {
                let message = format!("Failed to apply execpolicy amendment: {err}");
                tracing::warn!("{message}");
                let warning = EventMsg::Warning(WarningEvent { message });
                sess.send_event_raw(Event {
                    id: event_turn_id.clone(),
                    msg: warning,
                })
                .await;
            }
        }
    }
    let decision = if stale_amendment {
        ReviewDecision::Approved
    } else {
        decision
    };
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&approval_id, other).await,
    }
}

pub async fn patch_approval(sess: &Arc<Session>, id: String, decision: ReviewDecision) {
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&id, other).await,
    }
}

pub async fn request_user_input_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestUserInputResponse,
) {
    sess.notify_user_input_response(&id, response).await;
}

pub async fn request_permissions_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestPermissionsResponse,
) {
    sess.notify_request_permissions_response(&id, response)
        .await;
}

pub async fn dynamic_tool_response(sess: &Arc<Session>, id: String, response: DynamicToolResponse) {
    sess.notify_dynamic_tool_response(&id, response).await;
}

pub fn refresh_mcp_servers(sess: &Session) {
    sess.services.mcp_runtime.reconnect_on_next_refresh();
    sess.request_mcp_runtime_refresh();
}

pub async fn reload_user_config(sess: &Arc<Session>) {
    sess.reload_user_config_layer().await;
}

pub async fn compact(sess: &Arc<Session>, sub_id: String) {
    let turn_context = sess.new_default_turn_with_sub_id(sub_id).await;

    sess.spawn_task(Arc::clone(&turn_context), Vec::new(), CompactTask)
        .await;
}

pub async fn thread_rollback(sess: &Arc<Session>, sub_id: String, num_turns: u32) {
    if num_turns == 0 {
        sess.send_event_raw(Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: "num_turns must be >= 1".to_string(),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let has_active_turn = { sess.active_turn.lock().await.is_some() };
    if has_active_turn {
        sess.send_event_raw(Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: "Cannot rollback while a turn is in progress.".to_string(),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let turn_context = sess.new_default_turn_with_sub_id(sub_id).await;
    let live_thread = match sess.live_thread_for_persistence("rollback thread") {
        Ok(live_thread) => live_thread,
        Err(_) => {
            sess.send_event_raw(Event {
                id: turn_context.sub_id.clone(),
                msg: EventMsg::Error(ErrorEvent {
                    message: "thread rollback requires persisted thread history".to_string(),
                    codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
                }),
            })
            .await;
            return;
        }
    };
    if let Err(err) = live_thread.flush().await {
        sess.send_event_raw(Event {
            id: turn_context.sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: format!("failed to flush thread persistence for rollback replay: {err}"),
                codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
            }),
        })
        .await;
        return;
    }

    let stored_history = match live_thread.load_history(/*include_archived*/ false).await {
        Ok(history) => history,
        Err(err) => {
            sess.send_event_raw(Event {
                id: turn_context.sub_id.clone(),
                msg: EventMsg::Error(ErrorEvent {
                    message: format!("failed to load thread history for rollback replay: {err}"),
                    codex_error_info: Some(CodexErrorInfo::ThreadRollbackFailed),
                }),
            })
            .await;
            return;
        }
    };

    let rollback_event = ThreadRolledBackEvent { num_turns };
    let rollback_msg = EventMsg::ThreadRolledBack(rollback_event.clone());
    let replay_items = stored_history
        .items
        .into_iter()
        .chain(std::iter::once(RolloutItem::EventMsg(rollback_msg.clone())))
        .collect::<Vec<_>>();
    sess.apply_rollout_reconstruction(turn_context.as_ref(), replay_items.as_slice())
        .await;
    sess.services
        .agent_control
        .rollout_budget()
        .rearm_reminder(sess.thread_id());
    sess.recompute_token_usage(turn_context.as_ref()).await;

    sess.persist_rollout_items(&[RolloutItem::EventMsg(rollback_msg.clone())])
        .await;
    if let Err(err) = sess.flush_rollout().await {
        sess.send_event(
            turn_context.as_ref(),
            EventMsg::Warning(WarningEvent {
                message: format!(
                    "Rolled the thread back, but failed to save the rollback marker. Codex will continue retrying. Error: {err}"
                ),
            }),
        )
        .await;
    }

    sess.deliver_event_raw(Event {
        id: turn_context.sub_id.clone(),
        msg: rollback_msg,
    })
    .await;
}

pub(super) async fn persist_thread_memory_mode_update(
    sess: &Arc<Session>,
    mode: ThreadMemoryMode,
) -> anyhow::Result<()> {
    let live_thread = sess.live_thread_for_persistence("update thread memory mode")?;
    live_thread.persist().await?;
    live_thread.flush().await?;
    live_thread
        .update_memory_mode(mode, /*include_archived*/ false)
        .await?;
    live_thread.flush().await?;
    Ok(())
}

/// Persists thread-level memory mode metadata for the active session.
///
/// This does not involve the model and only affects whether the thread is
/// eligible for future memory generation.
pub async fn set_thread_memory_mode(sess: &Arc<Session>, sub_id: String, mode: ThreadMemoryMode) {
    if let Err(err) = persist_thread_memory_mode_update(sess, mode).await {
        warn!("Failed to persist thread memory mode update to rollout: {err}");
        let event = Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                message: err.to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }
}

async fn shutdown_session_runtime(sess: &Arc<Session>) {
    if let Some(startup_prewarm) = sess.take_session_startup_prewarm().await {
        startup_prewarm.abort().await;
    }
    let _ = sess.conversation.shutdown().await;
    sess.abort_all_tasks(TurnAbortReason::Interrupted).await;
    sess.services
        .unified_exec_manager
        .terminate_all_processes()
        .await;
    if let Err(err) = sess.services.code_mode_service.shutdown().await {
        warn!("failed to shutdown code mode session: {err}");
    }
    sess.stop_mcp_prewarm_worker().await;
    {
        let _refresh = sess.mcp_refresh.acquire().await;
        sess.mcp_refresh.close();
        sess.services.mcp_runtime.shutdown().await;
    }
    sess.guardian_review_session.shutdown().await;
    // PF-23-S03: run end revokes this session's brokered credentials.
    crate::security::transition::RevocationSink::revoke(sess.as_ref());

    crate::hook_runtime::run_session_end_hooks(sess).await;
}

async fn emit_thread_stop_lifecycle(sess: &Session) {
    for contributor in sess.services.extensions.thread_lifecycle_contributors() {
        contributor
            .on_thread_stop(codex_extension_api::ThreadStopInput {
                session_store: &sess.services.session_extension_data,
                thread_store: &sess.services.thread_extension_data,
            })
            .await;
    }
}

pub async fn shutdown(sess: &Arc<Session>, sub_id: String) -> bool {
    shutdown_session_runtime(sess).await;
    info!("Shutting down Codex instance");
    let history = sess.clone_history().await;
    let turn_count = history
        .raw_items()
        .iter()
        .filter(|item| is_user_turn_boundary(item))
        .count();
    sess.services.session_telemetry.counter(
        "codex.conversation.turn.count",
        i64::try_from(turn_count).unwrap_or(0),
        &[],
    );

    emit_thread_stop_lifecycle(sess.as_ref()).await;

    // Gracefully flush and shutdown thread persistence on session end so tests
    // that inspect durable state do not race with the background writer.
    if let Some(live_thread) = sess.live_thread()
        && let Err(e) = live_thread.shutdown().await
    {
        warn!("failed to shutdown thread persistence: {e}");
        let event = Event {
            id: sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                message: "Failed to shutdown thread persistence".to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }
    // #872: nothing writes this thread's items after its shutdown.
    crate::home_access::note_thread_ended(sess.thread_id());

    let event = Event {
        id: sub_id,
        msg: EventMsg::ShutdownComplete,
    };
    sess.services
        .rollout_thread_trace
        .record_protocol_event(&event.msg);
    sess.deliver_event_raw(event).await;
    sess.services
        .rollout_thread_trace
        .record_ended(codex_rollout_trace::RolloutStatus::Completed);
    true
}

pub async fn review(
    sess: &Arc<Session>,
    config: &Arc<Config>,
    sub_id: String,
    review_request: ReviewRequest,
) {
    let turn_context = sess.new_default_turn_with_sub_id(sub_id.clone()).await;
    sess.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
        .await;
    #[allow(deprecated)]
    match resolve_review_request(review_request, &turn_context.cwd) {
        Ok(resolved) => {
            spawn_review_thread(
                Arc::clone(sess),
                Arc::clone(config),
                turn_context.clone(),
                sub_id,
                resolved,
            )
            .await;
        }
        Err(err) => {
            let event = Event {
                id: sub_id,
                msg: EventMsg::Error(ErrorEvent {
                    message: err.to_string(),
                    codex_error_info: Some(CodexErrorInfo::Other),
                }),
            };
            sess.send_event(&turn_context, event.msg).await;
        }
    }
}

pub(super) async fn submission_loop(
    sess: Arc<Session>,
    config: Arc<Config>,
    rx_sub: Receiver<Submission>,
) {
    // To break out of this loop, send Op::Shutdown.
    let mut shutdown_received = false;
    loop {
        let sub = tokio::select! {
            sub = rx_sub.recv() => match sub {
                Ok(sub) => sub,
                Err(_) => break,
            },
            (input, final_output_json_schema) = sess.input_queue.next_interrupted_deferred_input() => {
                let (op, client_user_message_id) = match input {
                    TurnInput::UserInput { content, client_id } => (Op::UserInput {
                        items: content,
                        final_output_json_schema,
                        responsesapi_client_metadata: None,
                        additional_context: Default::default(),
                        thread_settings: Default::default(),
                    }, client_id),
                    TurnInput::ResponseItem(item) => {
                        if let Err(items) = sess.inject_extension_if_running(vec![item]).await {
                            let context = sess.new_default_turn().await;
                            sess.record_conversation_items(&context, &items).await;
                        }
                        continue;
                    }
                    TurnInput::InterAgentCommunication(communication) => (
                        Op::InterAgentCommunication { communication }, None,
                    ),
                };
                Submission {
                    id: uuid::Uuid::new_v4().to_string(),
                    op,
                    parent_turn_id: None,
                    client_user_message_id,
                    trace: None,
                }
            }
        };
        debug!(?sub, "Submission");
        let dispatch_span = submission_dispatch_span(&sub);
        let should_exit = async {
            match sub.op.clone() {
                Op::Interrupt => {
                    interrupt(&sess).await;
                    false
                }
                Op::CleanBackgroundTerminals => {
                    clean_background_terminals(&sess).await;
                    false
                }
                Op::RealtimeConversationStart(params) => {
                    if let Err(err) =
                        handle_realtime_conversation_start(&sess, sub.id.clone(), params).await
                    {
                        sess.send_event_raw(Event {
                            id: sub.id.clone(),
                            msg: EventMsg::Error(ErrorEvent {
                                message: err.to_string(),
                                codex_error_info: Some(CodexErrorInfo::Other),
                            }),
                        })
                        .await;
                    }
                    false
                }
                Op::RealtimeConversationAudio(params) => {
                    handle_realtime_conversation_audio(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationText(params) => {
                    handle_realtime_conversation_text(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationSpeech(params) => {
                    handle_realtime_conversation_speech(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationClose => {
                    handle_realtime_conversation_close(&sess, sub.id.clone()).await;
                    false
                }
                Op::RealtimeConversationListVoices => {
                    realtime_conversation_list_voices(&sess, sub.id.clone()).await;
                    false
                }
                Op::UserInput { .. } => {
                    // Boxed: a turn's future is large in debug builds, and
                    // polling it inline in this dispatch block made the
                    // block's frame overflow a 2 MiB worker stack.
                    Box::pin(user_input_or_turn(
                        &sess,
                        sub.id.clone(),
                        sub.op,
                        sub.client_user_message_id,
                        sub.parent_turn_id,
                    ))
                    .await;
                    false
                }
                Op::ThreadSettings { thread_settings } => {
                    update_thread_settings(&sess, sub.id.clone(), thread_settings).await;
                    false
                }
                Op::InterAgentCommunication { communication } => {
                    inter_agent_communication(
                        &sess,
                        sub.id.clone(),
                        communication,
                        sub.parent_turn_id,
                    )
                    .await;
                    false
                }
                Op::WakePendingWork => {
                    sess.maybe_start_turn_for_pending_work_with_sub_id(sub.id.clone())
                        .await;
                    false
                }
                Op::ExecApproval {
                    id: approval_id,
                    turn_id,
                    decision,
                } => {
                    exec_approval(&sess, approval_id, turn_id, decision).await;
                    false
                }
                Op::PatchApproval { id, decision } => {
                    patch_approval(&sess, id, decision).await;
                    false
                }
                Op::UserInputAnswer { id, response } => {
                    request_user_input_response(&sess, id, response).await;
                    false
                }
                Op::RequestPermissionsResponse { id, response } => {
                    request_permissions_response(&sess, id, response).await;
                    false
                }
                Op::DynamicToolResponse { id, response } => {
                    dynamic_tool_response(&sess, id, response).await;
                    false
                }
                Op::RefreshMcpServers => {
                    refresh_mcp_servers(&sess);
                    false
                }
                Op::ReloadUserConfig => {
                    reload_user_config(&sess).await;
                    false
                }
                Op::Compact => {
                    compact(&sess, sub.id.clone()).await;
                    false
                }
                Op::ThreadRollback { num_turns } => {
                    thread_rollback(&sess, sub.id.clone(), num_turns).await;
                    false
                }
                Op::SetThreadMemoryMode { mode } => {
                    set_thread_memory_mode(&sess, sub.id.clone(), mode).await;
                    false
                }
                Op::RunUserShellCommand { command } => {
                    run_user_shell_command(&sess, sub.id.clone(), command).await;
                    false
                }
                Op::ResolveElicitation {
                    server_name,
                    request_id,
                    decision,
                    content,
                    meta,
                } => {
                    resolve_elicitation(&sess, server_name, request_id, decision, content, meta)
                        .await;
                    false
                }
                Op::Shutdown => shutdown(&sess, sub.id.clone()).await,
                Op::Review { review_request } => {
                    review(&sess, &config, sub.id.clone(), review_request).await;
                    false
                }
                Op::ApproveGuardianDeniedAction { event } => {
                    approve_guardian_denied_action(&sess, event).await;
                    false
                }
                _ => false, // Ignore unknown ops; enum is non_exhaustive to allow extensions.
            }
        }
        .instrument(dispatch_span)
        .await;
        if should_exit {
            shutdown_received = true;
            break;
        }
    }
    // If the submission loop exits because the channel closed without an
    // explicit shutdown op, still run session teardown.
    if !shutdown_received {
        shutdown_session_runtime(&sess).await;
        emit_thread_stop_lifecycle(sess.as_ref()).await;
        if let Some(live_thread) = sess.live_thread()
            && let Err(err) = live_thread.shutdown().await
        {
            warn!("failed to shutdown thread persistence after submission channel closed: {err}");
        }
        crate::home_access::note_thread_ended(sess.thread_id());
    }
    debug!("Agent loop exited");
}

async fn approve_guardian_denied_action(sess: &Arc<Session>, event: GuardianAssessmentEvent) {
    if event.status != GuardianAssessmentStatus::Denied {
        warn!(
            review_id = event.id.as_str(),
            "ignoring approval for non-denied Guardian assessment"
        );
        return;
    }

    let approved_action = serde_json::json!({
        "action": &event.action,
        "outcome": "allowed",
    });
    let approved_action_json = match serde_json::to_string_pretty(&approved_action) {
        Ok(approved_action_json) => approved_action_json,
        Err(error) => {
            warn!(%error, review_id = event.id.as_str(), "failed to serialize approved Guardian action");
            return;
        }
    };
    let approval_prefix = crate::guardian::AUTO_REVIEW_DENIED_ACTION_APPROVAL_DEVELOPER_PREFIX;
    let text = format!(
        r#"{approval_prefix}

Treat this as approval to perform that exact action in the same context in which it was originally requested.
Do not assume this also authorizes similar operations with different payloads.

Approved action:
{approved_action_json}"#,
    );
    let items = vec![ResponseItem::from(ResponseInputItem::Message {
        role: "developer".to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
    })];

    sess.inject_no_new_turn(items, /*current_turn_context*/ None)
        .await;
}

pub(super) fn submission_dispatch_span(sub: &Submission) -> tracing::Span {
    let op_name = sub.op.kind();
    let span_name = format!("op.dispatch.{op_name}");
    let dispatch_span = match &sub.op {
        Op::RealtimeConversationAudio(_) => {
            debug_span!(
                "submission_dispatch",
                otel.name = span_name.as_str(),
                submission.id = sub.id.as_str(),
                codex.op = op_name
            )
        }
        _ => info_span!(
            "submission_dispatch",
            otel.name = span_name.as_str(),
            submission.id = sub.id.as_str(),
            codex.op = op_name
        ),
    };
    if let Some(trace) = sub.trace.as_ref()
        && !set_parent_from_w3c_trace_context(&dispatch_span, trace)
    {
        warn!(
            submission.id = sub.id.as_str(),
            "ignoring invalid submission trace carrier"
        );
    }
    dispatch_span
}
