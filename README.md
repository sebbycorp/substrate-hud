# Substrate HUD

A minimalist, transparent macOS overlay of your **kagent Agent Substrate**:
the worker pool, every agent checkpointed in the snapshot store, and agents
moving in and out of workers as they restore and suspend. Built for OBS.

White structure, **blue** = checkpointed / idle, **green** = running,
**red** = errors, pool short of desired, or an agent waiting on a worker.

```
kubectl (your kubeconfig, Omni OIDC)
  ├─ get workerpools / worker pods        every 3 s
  ├─ exec ate postgres: actors + worker_assignments   every 15 s
  └─ logs -f ate-api-server  ("Actor state changed", "Picked worker")   live
        │
        ▼
  Substrate HUD.app ──► app window (Tauri, transparent)
                    └─► http://127.0.0.1:7788/  (OBS Browser Source, SSE)
```

## Build & install

Prereqs: Rust, Xcode CLT, `cargo install tauri-cli --version "^2.0" --locked`.

```bash
cd app/src-tauri
CARGO_TARGET_DIR=~/.cache/substrate-hud-target cargo tauri build
cp -R ~/.cache/substrate-hud-target/release/bundle/macos/"Substrate HUD.app" /Applications/
```

The target dir lives outside Google Drive so build artifacts don't sync.
First launch of the unsigned app: right-click → **Open** → **Open**.

Icon: `python3 tools/make_icon.py app/src-tauri/icon-src.png && cargo tauri icon icon-src.png`.

## OBS

Add a **Browser Source** → URL `http://127.0.0.1:7788/`, width 1280, height 720.
The background is truly transparent (no chroma key). The app must be running.
Hover the app window → **OBS URL** copies it.

Query params for the Browser Source: `?panel=1` adds the dark panel,
`?sim=1` forces simulated traffic.

## App window

Hover the top-right for controls, or use keys:

| Key | |
| --- | --- |
| **B** | dark panel behind the HUD (on by default in the window) |
| **P** | keep on top |
| **I** | click-through (Esc to exit) |
| **S** | simulated traffic: for talks or when the cluster is idle |

Drag the window by its top edge.

## Config

Optional `~/.config/substrate-hud/config.json` (all keys optional):

```json
{
  "context": "maniak-omni-viper",
  "ate_namespace": "ate-system",
  "api_server_selector": "app=ate-api-server",
  "postgres_pod": "postgres-0",
  "postgres_container": "postgres",
  "database": "atepg",
  "port": 7788
}
```

Empty `context` means kubectl's current-context.

## Why these data sources

kagent Enterprise on omni-viper doesn't serve `/api/substrate/status` or
`SystemService/GetSubstrateStatus` (what `substrate-scope` uses), and per-actor
state isn't a CRD. So:

- **ate postgres** is ground truth for which actors exist and which worker
  holds them. `worker_assignments.worker_name` is the worker *pod UID*; rows
  pointing at dead pods are stale and ignored. Agent names come from the
  template name embedded in the actor proto (`fortigate-default-<hash>` →
  `fortigate`).
- **ate-api-server logs** give every transition live with timestamps, so
  restore latency = `resuming → running`, checkpoint = `suspending → suspended`.
  The chosen worker comes from the `Picked worker` line with the same `trace_id`.
  The stream restarts every 10 min to pick up new api-server pods.
