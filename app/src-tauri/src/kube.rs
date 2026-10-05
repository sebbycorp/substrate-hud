//! Collectors. Everything goes through `kubectl`, so the HUD sees exactly what
//! your terminal sees (same kubeconfig, same Omni OIDC token cache).
//!
//! - pods/workerpools: polled every few seconds
//! - ate postgres (`actors` + `worker_assignments`): polled; ground truth for
//!   which actors exist and which live worker holds them
//! - ate-api-server logs: streamed; every resume/suspend as it happens, with the
//!   worker the scheduler picked (joined on trace_id)

use crate::model::{agent_label, now_ms, Actor, Hub, Pool, Worker, HARNESSES};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// kubectl context; empty = current-context.
    pub context: String,
    pub ate_namespace: String,
    pub api_server_selector: String,
    pub postgres_pod: String,
    pub postgres_container: String,
    pub database: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            context: String::new(),
            ate_namespace: "ate-system".into(),
            api_server_selector: "app=ate-api-server".into(),
            postgres_pod: "postgres-0".into(),
            postgres_container: "postgres".into(),
            database: "atepg".into(),
            port: 7788,
        }
    }
}

fn home() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/Users/Shared".into())
}

pub fn config_path() -> PathBuf {
    PathBuf::from(home()).join(".config/substrate-hud/config.json")
}

pub fn load_config() -> Config {
    std::fs::read_to_string(config_path())
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// Apps launched from Finder get a bare PATH; kubectl's exec credential
/// plugin (kubectl-oidc_login for Omni) has to be findable too.
fn search_path() -> String {
    let h = home();
    let mut p = vec![
        format!("{h}/.local/bin"),
        "/opt/homebrew/bin".into(),
        "/usr/local/bin".into(),
        format!("{h}/.krew/bin"),
        format!("{h}/bin"),
        "/usr/bin".into(),
        "/bin".into(),
        "/usr/sbin".into(),
        "/sbin".into(),
    ];
    if let Ok(cur) = std::env::var("PATH") {
        p.push(cur);
    }
    p.join(":")
}

fn kubectl_bin() -> String {
    static BIN: OnceLock<String> = OnceLock::new();
    BIN.get_or_init(|| {
        for dir in search_path().split(':') {
            let c = PathBuf::from(dir).join("kubectl");
            if c.is_file() {
                return c.to_string_lossy().into_owned();
            }
        }
        "kubectl".into()
    })
    .clone()
}

fn kubectl(cfg: &Config) -> Command {
    let mut c = Command::new(kubectl_bin());
    c.env("PATH", search_path());
    if !cfg.context.is_empty() {
        c.arg("--context").arg(&cfg.context);
    }
    c.stdin(Stdio::null());
    c
}

fn run(cfg: &Config, args: &[&str]) -> Result<String, String> {
    let out = kubectl(cfg)
        .args(args)
        .output()
        .map_err(|e| format!("kubectl: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        let err = String::from_utf8_lossy(&out.stderr);
        let line = err
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("kubectl failed");
        Err(line.chars().take(200).collect())
    }
}

pub fn context_name(cfg: &Config) -> String {
    if !cfg.context.is_empty() {
        return cfg.context.clone();
    }
    run(cfg, &["config", "current-context"])
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "kubectl".into())
}

// ── pods + workerpools ─────────────────────────────────────────────────────

pub fn poll_workers(cfg: Config, hub: Arc<Hub>) {
    loop {
        let pools = run(&cfg, &["get", "workerpools.ate.dev", "-A", "-o", "json", "--request-timeout=8s"]);
        let pods = run(&cfg, &["get", "pods", "-A", "-l", "ate.dev/worker-pool", "-o", "json", "--request-timeout=8s"]);
        // Harness names let agent_label strip `-<harness>` from template names.
        if let Ok(h) = run(&cfg, &["get", "harnesses.kagent.dev", "-A", "-o", "json", "--request-timeout=8s"]) {
            let v: Value = serde_json::from_str(&h).unwrap_or(Value::Null);
            let mut names: Vec<String> = v["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|i| i["metadata"]["name"].as_str().map(String::from))
                .collect();
            // Longest first so `-high` doesn't shadow e.g. `-very-high`.
            names.sort_by_key(|n| std::cmp::Reverse(n.len()));
            *HARNESSES.lock().unwrap() = names;
        }
        match (pools, pods) {
            (Ok(pools), Ok(pods)) => {
                let pools = parse_pools(&pools);
                let workers = parse_workers(&pods);
                hub.update(|m| {
                    m.connected = true;
                    m.error = None;
                    m.pools = pools;
                    let prev = std::mem::take(&mut m.workers);
                    for mut w in workers {
                        if let Some(p) = prev.get(&w.name) {
                            w.sessions = p.sessions;
                        }
                        m.workers.insert(w.name.clone(), w);
                    }
                });
            }
            (Err(e), _) | (_, Err(e)) => hub.update(|m| {
                m.connected = false;
                m.error = Some(e);
            }),
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn parse_pools(json: &str) -> Vec<Pool> {
    let v: Value = serde_json::from_str(json).unwrap_or(Value::Null);
    v["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|p| Pool {
                    namespace: p["metadata"]["namespace"].as_str().unwrap_or("").into(),
                    name: p["metadata"]["name"].as_str().unwrap_or("").into(),
                    desired: p["spec"]["replicas"].as_i64().unwrap_or(0),
                    ready: p["status"]["readyReplicas"].as_i64().unwrap_or(0),
                    priority_class: p["spec"]["template"]["priorityClassName"].as_str().unwrap_or("").into(),
                    tier: p["metadata"]["labels"]["substrate.viper-env/tier"].as_str().unwrap_or("").into(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_workers(json: &str) -> Vec<Worker> {
    let v: Value = serde_json::from_str(json).unwrap_or(Value::Null);
    let mut out = Vec::new();
    for p in v["items"].as_array().into_iter().flatten() {
        let phase = p["status"]["phase"].as_str().unwrap_or("").to_string();
        // Completed/failed pods from old ReplicaSets linger on Talos; skip them.
        if phase != "Running" && phase != "Pending" {
            continue;
        }
        let terminating = !p["metadata"]["deletionTimestamp"].is_null();
        let ready = !terminating
            && p["status"]["containerStatuses"]
                .as_array()
                .is_some_and(|cs| !cs.is_empty() && cs.iter().all(|c| c["ready"] == true));
        out.push(Worker {
            name: p["metadata"]["name"].as_str().unwrap_or("").into(),
            uid: p["metadata"]["uid"].as_str().unwrap_or("").into(),
            pool: p["metadata"]["labels"]["ate.dev/worker-pool"].as_str().unwrap_or("").into(),
            phase: if terminating { "Terminating".into() } else { phase },
            ready,
            sessions: 0,
        });
    }
    out
}

// ── ate postgres ───────────────────────────────────────────────────────────

/// Actor protos are opaque bytes; the template name is the one string in
/// them shaped like `<name>-<12 hex>` that isn't a bare UUID chunk.
fn template_from_proto(hex: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*-[0-9a-f]{12}").unwrap());
    let bytes: Vec<u8> = (0..hex.len() / 2)
        .filter_map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok())
        .collect();
    let text = String::from_utf8_lossy(&bytes);
    for m in re.find_iter(&text) {
        let s = m.as_str();
        // Actor names are `ai-<uuid>`, so judge the part after an `ai-` prefix.
        let head = &s[..s.len() - 13];
        let head = head.strip_prefix("ai-").unwrap_or(head);
        if !s.starts_with("ate-") && head.chars().any(|c| ('g'..='z').contains(&c)) {
            return s.to_string();
        }
    }
    String::new()
}

const LOG_GRACE_MS: i64 = 45_000;

pub fn poll_db(cfg: Config, hub: Arc<Hub>) {
    let sql = "select a.name, a.uid, coalesce(wa.worker_name,''), encode(a.proto,'hex') \
               from actors a left join worker_assignments wa on wa.actor_uid = a.uid \
               where a.atespace <> 'ate-golden'"; // golden-snapshot builders aren't sessions
    loop {
        let res = run(
            &cfg,
            &[
                "exec", "-n", &cfg.ate_namespace, &cfg.postgres_pod, "-c", &cfg.postgres_container,
                "--request-timeout=10s", "--", "psql", "-U", "postgres", "-d", &cfg.database,
                "-At", "-F", "\t", "-c", sql,
            ],
        );
        if let Ok(out) = res {
            let rows: Vec<(String, String, String, String)> = out
                .lines()
                .filter_map(|l| {
                    let mut f = l.split('\t');
                    Some((
                        f.next()?.to_string(),
                        f.next()?.to_string(),
                        f.next()?.to_string(),
                        f.next().unwrap_or("").to_string(),
                    ))
                })
                .collect();
            hub.update(|m| {
                let now = now_ms();
                // worker_assignments.worker_name is the worker pod's UID.
                let pod_by_uid: HashMap<&str, &str> = m
                    .workers
                    .values()
                    .map(|w| (w.uid.as_str(), w.name.as_str()))
                    .collect();
                let mut seen = HashSet::new();
                let mut updates = Vec::new();
                for (name, uid, wuid, proto) in &rows {
                    seen.insert(name.clone());
                    let worker = pod_by_uid.get(wuid.as_str()).map(|s| s.to_string());
                    let state = if worker.is_some() { "running" } else { "suspended" };
                    updates.push((name.clone(), uid.clone(), worker, state, template_from_proto(proto)));
                }
                for (name, uid, worker, state, template) in updates {
                    let a = m.actors.entry(name.clone()).or_insert_with(|| Actor {
                        name: name.clone(),
                        uid: uid.clone(),
                        agent: agent_label(&template),
                        state: state.into(),
                        worker: worker.clone(),
                        since: now,
                        log_touched: 0,
                    });
                    // Recompute each poll: the Harness list may have loaded since the first one.
                    if !template.is_empty() {
                        a.agent = agent_label(&template);
                    }
                    if now - a.log_touched > LOG_GRACE_MS && (a.state != state || a.worker != worker) {
                        a.state = state.into();
                        a.worker = worker;
                        a.since = now;
                    }
                }
                m.actors
                    .retain(|n, a| seen.contains(n) || now - a.log_touched < LOG_GRACE_MS);
            });
        }
        thread::sleep(Duration::from_secs(15));
    }
}

// ── ate-api-server log stream ──────────────────────────────────────────────

fn ts_ms(v: &Value) -> i64 {
    v["time"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|d| d.timestamp_millis())
        .unwrap_or_else(now_ms)
}

pub fn stream_logs(cfg: Config, hub: Arc<Hub>) {
    // trace_id → worker pod chosen by "Picked worker" for that resume.
    let mut picked: HashMap<String, String> = HashMap::new();
    let pod_re = Regex::new(r#"worker_pod:"([^"]+)""#).unwrap();
    loop {
        let child = kubectl(&cfg)
            .args([
                "logs", "-f", "-n", &cfg.ate_namespace, "-l", &cfg.api_server_selector,
                "--since=5s", "--max-log-requests=20",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let Ok(mut child) = child else {
            thread::sleep(Duration::from_secs(5));
            continue;
        };
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let child: Arc<Mutex<Child>> = Arc::new(Mutex::new(child));

        // `logs -f -l` never notices api-server pods created after it started,
        // so recycle the stream periodically.
        let started = Instant::now();
        let watchdog = {
            let child = child.clone();
            thread::spawn(move || {
                while started.elapsed() < Duration::from_secs(600) {
                    thread::sleep(Duration::from_secs(2));
                    if child.lock().unwrap().try_wait().ok().flatten().is_some() {
                        return;
                    }
                }
                let _ = child.lock().unwrap().kill();
            })
        };

        hub.update(|m| m.log_stream = true);
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
            handle_line(&hub, &v, &mut picked, &pod_re);
        }
        let _ = child.lock().unwrap().kill();
        let _ = child.lock().unwrap().wait();
        let _ = watchdog.join();
        let mut err = String::new();
        let _ = stderr.read_to_string(&mut err);
        hub.update(|m| {
            m.log_stream = false;
            if let Some(l) = err.lines().rev().find(|l| !l.trim().is_empty()) {
                m.error = Some(l.chars().take(200).collect());
            }
        });
        thread::sleep(Duration::from_secs(3));
    }
}

fn handle_line(hub: &Hub, v: &Value, picked: &mut HashMap<String, String>, pod_re: &Regex) {
    let msg = v["msg"].as_str().unwrap_or("");
    let trace = v["trace_id"].as_str().unwrap_or("").to_string();
    let ts = ts_ms(v);

    if msg == "Picked worker" {
        if let Some(c) = v["worker"].as_str().and_then(|w| pod_re.captures(w)) {
            if picked.len() > 500 {
                picked.clear();
            }
            picked.insert(trace, c[1].to_string());
        }
        return;
    }

    if msg == "Actor state changed" {
        if v["ate.atespace"].as_str() == Some("ate-golden") {
            return;
        }
        let name = v["ate.actor.name"].as_str().unwrap_or("").to_string();
        let uid = v["ate.actor.uid"].as_str().unwrap_or("").to_string();
        let agent = agent_label(v["ate.template.name"].as_str().unwrap_or(""));
        let op = v["ate.actor.operation.name"].as_str().unwrap_or("");
        let state = v["ate.actor.state"].as_str().unwrap_or("").to_string();
        let pod = picked.get(&trace).cloned();
        hub.update(|m| {
            let a = m.actors.entry(name.clone()).or_insert_with(|| Actor {
                name: name.clone(),
                uid,
                agent: agent.clone(),
                state: "suspended".into(),
                worker: None,
                since: ts,
                log_touched: 0,
            });
            let prev_state = std::mem::replace(&mut a.state, state.clone());
            let prev_since = std::mem::replace(&mut a.since, ts);
            a.log_touched = now_ms();
            if !agent.is_empty() {
                a.agent = agent.clone();
            }
            let label = a.agent.clone();
            match state.as_str() {
                "resuming" => {
                    if pod.is_some() {
                        a.worker = pod;
                    }
                }
                "running" => {
                    if a.worker.is_none() {
                        a.worker = pod;
                    }
                    let worker = a.worker.clone();
                    let ms = (prev_state == "resuming").then(|| ts - prev_since);
                    m.stats.resumes += 1;
                    if let Some(ms) = ms {
                        m.record_restore(ms);
                    }
                    if let Some(w) = worker.as_ref().and_then(|w| m.workers.get_mut(w)) {
                        w.sessions += 1;
                    }
                    let lat = ms.map(|ms| format!(" · {ms} ms")).unwrap_or_default();
                    let on = worker.map(|w| format!(" → {w}")).unwrap_or_default();
                    m.push_event(ts, "restore", &label, format!("restored{on}{lat}"));
                }
                "suspending" => {}
                "suspended" => {
                    a.worker = None;
                    if op == "create" {
                        m.stats.creates += 1;
                        m.push_event(ts, "create", &label, "created · golden snapshot".into());
                    } else {
                        let ms = (prev_state == "suspending").then(|| ts - prev_since);
                        m.stats.suspends += 1;
                        if ms.is_some() {
                            m.stats.last_suspend_ms = ms;
                        }
                        let lat = ms.map(|ms| format!(" · {ms} ms")).unwrap_or_default();
                        m.push_event(ts, "suspend", &label, format!("checkpointed to storage{lat}"));
                    }
                }
                other => {
                    m.push_event(ts, "info", &label, format!("{op} → {other}"));
                }
            }
        });
        return;
    }

    let level = v["level"].as_str().unwrap_or("");
    let lower = msg.to_ascii_lowercase();
    if level == "ERROR" || lower.contains("no free worker") {
        let detail = v["err"].as_str().or(v["error"].as_str()).unwrap_or("");
        let text: String = if detail.is_empty() { msg.to_string() } else { format!("{msg}: {detail}") };
        // Name the failing agent when the error is about an actor template.
        let who = v["template"]["name"].as_str().map(agent_label).unwrap_or_else(|| "ate-api".into());
        hub.update(|m| {
            m.stats.errors += 1;
            m.push_event(ts, "error", &who, text.chars().take(160).collect());
        });
    }
}
