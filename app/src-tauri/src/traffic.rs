//! Traffic wave: one real chat to each agent through the diary's agent-desk
//! router (`/ask`), so the HUD has something to show on demand. Every chat is
//! a genuine restore → model turn → checkpoint on the cluster.
//!
//! Triggered from the app toolbar (T), the menu bar, or GET /traffic on the
//! local server (handy for a Stream Deck button).

use crate::kube::{kubectl, Config};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Local end of the port-forward to the agent-desk Service.
const LOCAL_PORT: u16 = 17781;

static CFG: OnceLock<Config> = OnceLock::new();
static FORWARD: Mutex<Option<Child>> = Mutex::new(None);
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Read-only questions, one picked per agent each wave.
const PROMPTS: &[(&str, &[&str])] = &[
    ("@k8s", &[
        "how many pods are running in the kagent namespace?",
        "list the nodes and their status",
        "which deployments are in agentgateway-system?",
    ]),
    ("@k8sp", &["is the node Ready?", "how many namespaces are there?", "how many WorkerPools are there and how big are they?"]),
    ("@code", &[
        "write a bash one-liner that prints the numbers 1 to 5, run it and show the output",
        "write a bash loop that sums 1 to 100, run it and show the result",
    ]),
    ("@forti", &["how many DHCP leases are there?", "list the interfaces that are up"]),
];

pub fn init(cfg: Config) {
    let _ = CFG.set(cfg);
}

pub fn in_flight() -> usize {
    IN_FLIGHT.load(Ordering::SeqCst)
}

/// Keep a `kubectl port-forward` to agent-desk alive and wait until it accepts.
fn ensure_forward(cfg: &Config) -> Result<(), String> {
    let mut fwd = FORWARD.lock().unwrap();
    let alive = fwd.as_mut().is_some_and(|c| c.try_wait().ok().flatten().is_none());
    if !alive {
        let child = kubectl(cfg)
            .args([
                "port-forward", "-n", &cfg.traffic_namespace, &format!("svc/{}", cfg.traffic_service),
                &format!("{LOCAL_PORT}:{}", cfg.traffic_port),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("port-forward: {e}"))?;
        *fwd = Some(child);
    }
    drop(fwd);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        if TcpStream::connect(("127.0.0.1", LOCAL_PORT)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err("agent-desk port-forward didn't come up".into())
}

/// Fire one chat per agent in the background. Returns how many were sent.
pub fn wave() -> Result<usize, String> {
    let cfg = CFG.get().ok_or("traffic not initialised")?.clone();
    if in_flight() > 0 {
        return Err(format!("a wave is still running ({} chats in flight)", in_flight()));
    }
    ensure_forward(&cfg)?;
    let seed = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as usize).unwrap_or(0);
    for (i, (tag, qs)) in PROMPTS.iter().enumerate() {
        let text = format!("{tag} {}", qs[(seed + i) % qs.len()]);
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        thread::spawn(move || {
            // stagger a little so restores land one after another on screen
            thread::sleep(Duration::from_millis(400 * i as u64));
            let body = serde_json::json!({ "text": text }).to_string();
            let _ = Command::new("/usr/bin/curl")
                .args(["-s", "-m", "200", "-X", "POST", &format!("http://127.0.0.1:{LOCAL_PORT}/ask"),
                       "-H", "content-type: application/json", "-d", &body])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        });
    }
    Ok(PROMPTS.len())
}

/// Stop the port-forward when the app quits.
pub fn shutdown() {
    if let Some(mut c) = FORWARD.lock().unwrap().take() {
        let _ = c.kill();
    }
}
