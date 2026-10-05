#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod kube;
mod model;
mod server;
mod traffic;

use model::Hub;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tauri::menu::{CheckMenuItem, Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{Emitter, Manager, State, WindowEvent};
use tauri_plugin_autostart::{MacosLauncher, ManagerExt};

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

/// One wave of real chats to the agents (toolbar T / menu bar / GET /traffic).
#[tauri::command]
fn send_traffic() -> Result<usize, String> {
    traffic::wave()
}

fn show_window(app: &tauri::AppHandle) {
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.show();
        let _ = w.set_focus();
    }
}

/// Menu bar icon: the HUD keeps serving OBS while its window is hidden.
fn build_tray(app: &tauri::App) -> tauri::Result<()> {
    let login_on = app.autolaunch().is_enabled().unwrap_or(false);
    let show = MenuItem::with_id(app, "show", "Show HUD", true, None::<&str>)?;
    let wave = MenuItem::with_id(app, "traffic", "Send traffic wave", true, None::<&str>)?;
    let obs = MenuItem::with_id(app, "obs", "Copy OBS URL", true, None::<&str>)?;
    let login = CheckMenuItem::with_id(app, "login", "Launch at login", true, login_on, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit Substrate HUD", true, None::<&str>)?;
    let sep = PredefinedMenuItem::separator(app)?;
    let menu = Menu::with_items(app, &[&show, &wave, &obs, &sep, &login, &PredefinedMenuItem::separator(app)?, &quit])?;
    let login_item = login.clone();
    TrayIconBuilder::with_id("hud")
        .icon(app.default_window_icon().cloned().expect("app icon"))
        .icon_as_template(false)
        .tooltip("Substrate HUD")
        .menu(&menu)
        .show_menu_on_left_click(true)
        .on_menu_event(move |app, ev| match ev.id().as_ref() {
            "show" => show_window(app),
            "traffic" => {
                let _ = traffic::wave();
            }
            "obs" => {
                let port = app.state::<Ports>().http;
                let _ = std::process::Command::new("/bin/sh")
                    .args(["-c", &format!("printf 'http://127.0.0.1:{port}/' | pbcopy")])
                    .status();
            }
            "login" => {
                let al = app.autolaunch();
                let on = al.is_enabled().unwrap_or(false);
                let _ = if on { al.disable() } else { al.enable() };
                let _ = login_item.set_checked(al.is_enabled().unwrap_or(!on));
            }
            "quit" => {
                traffic::shutdown();
                app.exit(0);
            }
            _ => {}
        })
        .build(app)?;
    Ok(())
}

/// Turn on launch-at-login once, on first run; after that the menu item rules.
fn default_autostart(app: &tauri::App) {
    let marker = kube::config_path().with_file_name(".autostart-default-set");
    if marker.exists() {
        return;
    }
    let _ = app.autolaunch().enable();
    if let Some(dir) = marker.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(&marker, "1");
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
    traffic::init(cfg.clone());
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
        .plugin(tauri_plugin_autostart::init(MacosLauncher::LaunchAgent, None))
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
            default_autostart(app);
            build_tray(app)?;
            Ok(())
        })
        // closing the window hides it; the HUD keeps serving OBS from the menu bar
        .on_window_event(|w, ev| {
            if let WindowEvent::CloseRequested { api, .. } = ev {
                api.prevent_close();
                let _ = w.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![obs_url, get_state, send_traffic])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, ev| {
            match ev {
                // clicking the Dock icon brings the hidden window back
                tauri::RunEvent::Reopen { .. } => show_window(app),
                tauri::RunEvent::Exit => traffic::shutdown(),
                _ => {}
            }
        });
}
