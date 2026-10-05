#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod kube;
mod model;
mod server;

use model::Hub;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tauri::{Emitter, State};

struct Ports {
    http: u16,
}

/// The OBS Browser Source URL, shown in the app's menu.
#[tauri::command]
fn obs_url(ports: State<Ports>) -> String {
    format!("http://127.0.0.1:{}/", ports.http)
}

#[tauri::command]
fn get_state(hub: State<Arc<Hub>>) -> String {
    hub.snapshot()
}

/// Coalesce model changes into at most ~8 pushes/s, plus a 2 s heartbeat so
/// ages and events/min keep ticking while the cluster is idle.
fn publisher(hub: Arc<Hub>) {
    let mut last = Instant::now();
    loop {
        thread::sleep(Duration::from_millis(120));
        let dirty = std::mem::take(&mut *hub.dirty.lock().unwrap());
        if dirty || last.elapsed() > Duration::from_secs(2) {
            let snap = hub.snapshot();
            hub.broadcast(&snap);
            last = Instant::now();
        }
    }
}

fn main() {
    let cfg = kube::load_config();
    let hub = Hub::new(kube::context_name(&cfg));
    for f in [kube::poll_workers, kube::poll_db, kube::stream_logs] {
        let (cfg, hub) = (cfg.clone(), hub.clone());
        thread::spawn(move || f(cfg, hub));
    }
    {
        let hub = hub.clone();
        thread::spawn(move || publisher(hub));
    }
    {
        let hub = hub.clone();
        let port = cfg.port;
        thread::spawn(move || server::serve(port, hub));
    }

    tauri::Builder::default()
        .manage(hub.clone())
        .manage(Ports { http: cfg.port })
        .setup(move |app| {
            let (tx, rx) = channel::<String>();
            hub.subs.lock().unwrap().push(tx);
            let handle = app.handle().clone();
            thread::spawn(move || {
                for snap in rx {
                    let _ = handle.emit("snapshot", snap);
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![obs_url, get_state])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
