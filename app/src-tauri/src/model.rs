//! The HUD's view of the substrate: worker pools, worker pods, actors, and
//! rolling stats. Collectors mutate it; the publisher snapshots it to the UI.

use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Serialize)]
pub struct Pool {
    pub namespace: String,
    pub name: String,
    pub desired: i64,
    pub ready: i64,
}

#[derive(Clone, Serialize)]
pub struct Worker {
    pub name: String,
    pub uid: String,
    pub pool: String,
    pub phase: String,
    pub ready: bool,
    /// Actors this worker has hosted since the HUD started.
    pub sessions: u64,
}

#[derive(Clone, Serialize)]
pub struct Actor {
    pub name: String,
    pub uid: String,
    /// AgentTemplate-derived display name, e.g. "fortigate".
    pub agent: String,
    /// suspended | resuming | running | suspending
    pub state: String,
    /// Worker pod name while resuming/running.
    pub worker: Option<String>,
    /// When `state` last changed (ms since epoch, from the log timestamp).
    pub since: i64,
    /// Set by the log stream; DB snapshots don't override fresh log state.
    #[serde(skip)]
    pub log_touched: i64,
}

#[derive(Clone, Serialize)]
pub struct Event {
    pub id: u64,
    pub ts: i64,
    /// restore | suspend | create | error | info
    pub kind: String,
    pub agent: String,
    pub text: String,
}

#[derive(Default, Clone, Serialize)]
pub struct Stats {
    pub resumes: u64,
    pub suspends: u64,
    pub creates: u64,
    pub errors: u64,
    pub last_restore_ms: Option<i64>,
    pub p50_restore_ms: Option<i64>,
    pub last_suspend_ms: Option<i64>,
    pub events_per_min: u64,
    #[serde(skip)]
    pub restore_samples: VecDeque<i64>,
    #[serde(skip)]
    pub recent: VecDeque<i64>,
}

#[derive(Default)]
pub struct Model {
    pub context: String,
    pub connected: bool,
    pub error: Option<String>,
    pub log_stream: bool,
    pub pools: Vec<Pool>,
    pub workers: BTreeMap<String, Worker>,
    pub actors: HashMap<String, Actor>,
    pub stats: Stats,
    pub events: VecDeque<Event>,
    next_event: u64,
    pub started: i64,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    context: &'a str,
    connected: bool,
    error: &'a Option<String>,
    log_stream: bool,
    now: i64,
    started: i64,
    pools: &'a [Pool],
    workers: Vec<&'a Worker>,
    actors: Vec<&'a Actor>,
    stats: &'a Stats,
    events: Vec<&'a Event>,
}

impl Model {
    pub fn push_event(&mut self, ts: i64, kind: &str, agent: &str, text: String) {
        self.next_event += 1;
        self.events.push_back(Event {
            id: self.next_event,
            ts,
            kind: kind.to_string(),
            agent: agent.to_string(),
            text,
        });
        while self.events.len() > 40 {
            self.events.pop_front();
        }
        self.stats.recent.push_back(now_ms());
    }

    pub fn record_restore(&mut self, ms: i64) {
        let s = &mut self.stats;
        s.last_restore_ms = Some(ms);
        s.restore_samples.push_back(ms);
        while s.restore_samples.len() > 50 {
            s.restore_samples.pop_front();
        }
        let mut v: Vec<i64> = s.restore_samples.iter().copied().collect();
        v.sort_unstable();
        s.p50_restore_ms = v.get(v.len() / 2).copied();
    }

    pub fn snapshot_json(&mut self) -> String {
        let cutoff = now_ms() - 60_000;
        while self.stats.recent.front().is_some_and(|t| *t < cutoff) {
            self.stats.recent.pop_front();
        }
        self.stats.events_per_min = self.stats.recent.len() as u64;
        let mut actors: Vec<&Actor> = self.actors.values().collect();
        actors.sort_by(|a, b| a.agent.cmp(&b.agent).then(a.name.cmp(&b.name)));
        let snap = Snapshot {
            context: &self.context,
            connected: self.connected,
            error: &self.error,
            log_stream: self.log_stream,
            now: now_ms(),
            started: self.started,
            pools: &self.pools,
            workers: self.workers.values().collect(),
            actors,
            stats: &self.stats,
            events: self.events.iter().collect(),
        };
        serde_json::to_string(&snap).unwrap_or_else(|_| "{}".into())
    }
}

/// Model + subscribers. `dirty` coalesces bursts of log lines into one push.
pub struct Hub {
    pub model: Mutex<Model>,
    pub dirty: Mutex<bool>,
    pub subs: Mutex<Vec<Sender<String>>>,
}

impl Hub {
    pub fn new(context: String) -> Arc<Hub> {
        Arc::new(Hub {
            model: Mutex::new(Model {
                context,
                started: now_ms(),
                ..Default::default()
            }),
            dirty: Mutex::new(true),
            subs: Mutex::new(Vec::new()),
        })
    }

    pub fn update<F: FnOnce(&mut Model)>(&self, f: F) {
        f(&mut self.model.lock().unwrap());
        *self.dirty.lock().unwrap() = true;
    }

    pub fn snapshot(&self) -> String {
        self.model.lock().unwrap().snapshot_json()
    }

    pub fn broadcast(&self, msg: &str) {
        self.subs
            .lock()
            .unwrap()
            .retain(|tx| tx.send(msg.to_string()).is_ok());
    }
}

/// "fortigate-default-a0f8b09338b1" → "fortigate". Template names are
/// `<agenttemplate>-<harness>-<hash>`; the harness on this cluster is "default".
pub fn agent_label(template: &str) -> String {
    let mut s = template;
    if let Some((head, tail)) = s.rsplit_once('-') {
        if tail.len() == 12 && tail.chars().all(|c| c.is_ascii_hexdigit()) {
            s = head;
        }
    }
    s.strip_suffix("-default").unwrap_or(s).to_string()
}
