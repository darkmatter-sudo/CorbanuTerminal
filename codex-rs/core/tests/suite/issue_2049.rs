//! #2049 (D510, reproduction): two checks are decided by the computer Corbanu
//! Terminal runs on, not by where the work runs. A Windows client with no
//! Windows sandbox refuses benign commands meant for a remote Linux machine
//! (an exec-server), though they would run under that machine's own sandbox,
//! and nothing reaches the executor:
//! - under Moderate and Aggressive, every command, by #1976's rule (D491,
//!   `security::windows_unsandboxed::applies`);
//! - under Permissive too, a command outside the known-safe list (`df -h /`),
//!   by `exec_policy::render_decision_for_unmatched_command`
//!   (`windows_managed_fs_restrictions_without_sandbox_backend`), as
//!   "blocked by policy" when approvals are off.
//! On other clients the same commands reach the executor.
//!
//! This pins today's behavior so the fix for #2049 has to flip it: once both
//! checks are decided where the work runs, `expected_refusal` below returns
//! `None` for a remote Linux machine and every command must reach it.

use std::sync::Arc;
use std::sync::Mutex;

use codex_core::config::Constrained;
use codex_features::Feature;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use codex_security_policy::SecurityLevel;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

const CALL_ID: &str = "call-2049";

/// Benign commands meant for the remote Linux machine.
const COMMANDS: [&str; 4] = [
    "uname -a",
    "cat /etc/os-release",
    "ls -la /home/me",
    "df -h /",
];

/// The refusal a Windows client without its sandbox gives today for `cmd`
/// under `level` (`name`), with approvals off; `None` where the command
/// reaches the remote machine. #2049: should be `None` everywhere here.
fn expected_refusal(level: SecurityLevel, name: &str, cmd: &str) -> Option<String> {
    if !cfg!(windows) {
        return None;
    }
    if level != SecurityLevel::Permissive {
        return Some(format!("Security level {name} refused this command"));
    }
    // `df` is not on the known-safe list; `uname`, `cat` and `ls` are.
    (cmd == "df -h /").then(|| format!("`/bin/bash -lc '{cmd}'` rejected: blocked by policy"))
}

/// A fake Linux executor: answers `initialize` and `environment/info`,
/// records every other request and answers it with an error.
async fn fake_linux_executor() -> anyhow::Result<(String, Arc<Mutex<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let methods = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&methods);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(message)) = websocket.next().await {
                    let request: Value = match message {
                        Message::Text(text) => match serde_json::from_str(text.as_ref()) {
                            Ok(request) => request,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    let method = request["method"].as_str().unwrap_or_default().to_string();
                    let Some(id) = request.get("id").cloned() else {
                        continue;
                    };
                    let reply = match method.as_str() {
                        "initialize" => json!({ "id": id, "result": { "sessionId": "fake" } }),
                        "environment/info" => json!({
                            "id": id,
                            "result": {
                                "shell": { "name": "bash", "path": "/bin/bash" },
                                "cwd": "file:///home/me",
                            },
                        }),
                        _ => {
                            seen.lock().expect("methods").push(method);
                            json!({
                                "id": id,
                                "error": { "code": -32601, "message": "not served by the fake" },
                            })
                        }
                    };
                    if websocket
                        .send(Message::Text(reply.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    Ok((url, methods))
}

struct Outcome {
    output: String,
    /// The requests other than `initialize` and `environment/info` the
    /// executor got during the turn.
    reached: Vec<String>,
}

async fn run(level: SecurityLevel, cmd: &str) -> anyhow::Result<Outcome> {
    let server = start_mock_server().await;
    let (url, methods) = fake_linux_executor().await?;
    let test = test_codex()
        .with_exec_server_url(url)
        .with_config(move |config| {
            config.security_level = level;
            config.permissions.approval_policy = Constrained::allow_any(AskForApproval::Never);
            // Moderate and Aggressive need it to send a request at all.
            config
                .features
                .enable(Feature::SourceEnvelopes)
                .expect("test config should allow feature update");
            config.use_experimental_unified_exec_tool = true;
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("test config should allow feature update");
            config
                .permissions
                .set_permission_profile(PermissionProfile::workspace_write_with(
                    &[],
                    NetworkSandboxPolicy::Restricted,
                    /*exclude_tmpdir_env_var*/ true,
                    /*exclude_slash_tmp*/ true,
                ))
                .expect("profile");
        })
        .build(&server)
        .await?;
    // Only what the tool call sends counts.
    methods.lock().expect("methods").clear();
    let arguments = json!({ "cmd": cmd, "yield_time_ms": 500 });
    let captured = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created(CALL_ID),
                ev_function_call(CALL_ID, "exec_command", &arguments.to_string()),
                ev_completed(CALL_ID),
            ]),
            sse(vec![
                ev_response_created("done"),
                ev_assistant_message("msg-done", "done"),
                ev_completed("done"),
            ]),
        ],
    )
    .await;
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "go".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    let request = captured
        .last_request()
        .ok_or_else(|| anyhow::anyhow!("no request after the tool call"))?;
    let reached = methods.lock().expect("methods").clone();
    Ok(Outcome {
        output: request
            .function_call_output_text(CALL_ID)
            .unwrap_or_default(),
        reached,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_2049_windows_client_without_its_sandbox_refuses_remote_linux_work()
-> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let mut wrong = Vec::new();
    for (level, name) in [
        (SecurityLevel::Permissive, "Permissive"),
        (SecurityLevel::Moderate, "Moderate"),
        (SecurityLevel::Aggressive, "Aggressive"),
    ] {
        for cmd in COMMANDS {
            let outcome = run(level, cmd).await?;
            let refusal = expected_refusal(level, name, cmd);
            let refused = refusal
                .as_ref()
                .is_some_and(|refusal| outcome.output.contains(refusal));
            let reached = outcome
                .reached
                .iter()
                .any(|method| method == "process/start");
            println!(
                "issue_2049 windows={} level={name} cmd={cmd:?} expected_refusal={refusal:?} \
                 refused={refused} reached_executor={reached} output={:?}",
                cfg!(windows),
                outcome.output
            );
            if refused != refusal.is_some() || reached == refusal.is_some() {
                wrong.push(format!(
                    "{name} {cmd:?}: output {}; reached {:?}",
                    outcome.output, outcome.reached
                ));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    Ok(())
}
