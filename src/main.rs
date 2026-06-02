//! DSTP Relay — tiny HTTP proxy that bridges the DST mod to a remote DSTP
//! backend. The DST Lua sandbox only allows QueryServer() to 127.0.0.1 /
//! localhost and a hardcoded list of Klei domains, so the mod can't reach a
//! public backend directly. This relay listens on 127.0.0.1 (which passes the
//! sandbox check) and forwards every request to the configured upstream.
//!
//! Ships as a single self-contained executable. No install, no deps.
//!
//! Config precedence (highest to lowest):
//!   1. env vars DSTP_UPSTREAM / DSTP_PORT / DSTP_TOKEN
//!   2. dstp-relay.config.json next to the binary
//!   3. baked-in defaults at build time
//!
//! This is a 1:1 port of the original relay.ts (Bun) to Rust, for a ~400KB
//! binary instead of 116MB.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;

// ─── Baked defaults ─────────────────────────────────────────────────────
// Edit these before `cargo build --release` to embed your production upstream.
const BAKED_UPSTREAM: &str = "https://dstp.marcosbrendon.com";
// Port 47834 chosen from IANA unassigned range to avoid conflicts with
// common dev services (Node 3000, Vite 5173, Tomcat 8080, etc).
const BAKED_PORT: u16 = 47834;

// Central config fetched from git at startup. One JSON with a `prod` and a
// `dev` entry; the relay picks one by DSTP_ENV (default: prod). Editing this
// in the repo re-points every relay on its next boot — no recompile.
const REMOTE_CONFIG_URL: &str =
    "https://raw.githubusercontent.com/MarcosBrendonDePaula/dstp-relay/main/relay-config.json";

// ─── Config ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Config {
    upstream: String,
    port: u16,
    token: Option<String>,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Overlay a JSON object's upstream/port/token onto an existing Config.
fn apply_json(cfg: &mut Config, v: &Value) {
    if let Some(u) = v.get("upstream").and_then(|x| x.as_str()) {
        if !u.is_empty() {
            cfg.upstream = u.to_string();
        }
    }
    if let Some(p) = v.get("port").and_then(|x| x.as_u64()) {
        if p > 0 && p < 65536 {
            cfg.port = p as u16;
        }
    }
    if let Some(t) = v.get("token").and_then(|x| x.as_str()) {
        if !t.is_empty() {
            cfg.token = Some(t.to_string());
        }
    }
}

// Fetch the central config from git and pick the prod/dev entry.
// Returns None on any failure — the caller falls back to local/baked.
async fn fetch_remote_config(http: &reqwest::Client, env: &str) -> Option<Value> {
    let res = http.get(REMOTE_CONFIG_URL).send().await.ok()?;
    if !res.status().is_success() {
        return None;
    }
    let body = res.text().await.ok()?;
    let root: Value = serde_json::from_str(&body).ok()?;
    root.get(env).cloned()
}

// Config precedence (highest to lowest):
//   1. env vars DSTP_UPSTREAM / DSTP_PORT / DSTP_TOKEN
//   2. dstp-relay.config.json next to the binary (or CWD)
//   3. central config from git, entry chosen by DSTP_ENV (default: prod)
//   4. baked-in defaults
async fn load_config(http: &reqwest::Client) -> Config {
    // Start from baked defaults.
    let mut cfg = Config {
        upstream: BAKED_UPSTREAM.to_string(),
        port: BAKED_PORT,
        token: None,
    };

    // (3) Remote git config. DSTP_ENV selects the entry; default "prod".
    let env = std::env::var("DSTP_ENV").unwrap_or_else(|_| "prod".to_string());
    match fetch_remote_config(http, &env).await {
        Some(entry) => {
            apply_json(&mut cfg, &entry);
            println!("[relay] Using remote config (env: {}) -> {}", env, cfg.upstream);
        }
        None => {
            eprintln!(
                "[relay] Remote config unavailable (env: {}), falling back to local/baked",
                env
            );
        }
    }

    // (2) Local config file next to the binary, then CWD.
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("dstp-relay.config.json"));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("dstp-relay.config.json"));
    }
    for path in candidates {
        if path.exists() {
            match std::fs::read_to_string(&path) {
                Ok(raw) => match serde_json::from_str::<Value>(&raw) {
                    Ok(v) => {
                        apply_json(&mut cfg, &v);
                        println!("[relay] Using local config: {}", path.display());
                        break;
                    }
                    Err(e) => eprintln!("[relay] Failed to parse {}: {}", path.display(), e),
                },
                Err(e) => eprintln!("[relay] Failed to read {}: {}", path.display(), e),
            }
        }
    }

    // (1) Env vars win over everything.
    if let Ok(u) = std::env::var("DSTP_UPSTREAM") {
        if !u.is_empty() {
            cfg.upstream = u;
        }
    }
    if let Some(p) = std::env::var("DSTP_PORT").ok().and_then(|s| s.parse::<u16>().ok()).filter(|p| *p > 0) {
        cfg.port = p;
    }
    if let Some(t) = std::env::var("DSTP_TOKEN").ok().filter(|s| !s.is_empty()) {
        cfg.token = Some(t);
    }

    cfg
}

// ─── Shared runtime state ───────────────────────────────────────────────

// Rolling latency stats for the live dashboard. Keeps the last N upstream
// round-trip times (ms) for a sparkline + avg/min/max/jitter.
const LAT_WINDOW: usize = 60;

struct Metrics {
    started_at: i64,
    latencies: std::collections::VecDeque<f64>, // last N request times (ms)
    last_req_count: u64,                          // for req/s between ticks
    last_tick_at: i64,
}

impl Metrics {
    fn new(now: i64) -> Self {
        Metrics {
            started_at: now,
            latencies: std::collections::VecDeque::with_capacity(LAT_WINDOW),
            last_req_count: 0,
            last_tick_at: now,
        }
    }
    fn record(&mut self, ms: f64) {
        if self.latencies.len() == LAT_WINDOW {
            self.latencies.pop_front();
        }
        self.latencies.push_back(ms);
    }
}

struct AppState {
    cfg: Config,
    verbose: bool,
    use_ws: bool,
    http: reqwest::Client,

    request_count: AtomicU64,
    error_count: AtomicU64,
    last_upstream_ok_at: AtomicI64,
    metrics: Mutex<Metrics>,

    // WebSocket tunnel state
    ws_ready: AtomicBool,
    ws_seq: AtomicU64,
    ws_tx: Mutex<Option<mpsc::UnboundedSender<Message>>>,
    ws_pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    // Local command buffer keyed by shard_id (pushed by backend over WS).
    pushed_commands: Mutex<HashMap<String, Vec<Value>>>,
}

// ─── WebSocket tunnel ───────────────────────────────────────────────────
// Each DST sync HTTP request from the mod is tunneled as one WS message
// (request/response correlated by `id`). One persistent TLS connection.

fn ws_url(upstream: &str) -> Option<String> {
    let mut u = url::Url::parse(upstream).ok()?;
    let scheme = if u.scheme() == "https" { "wss" } else { "ws" };
    u.set_scheme(scheme).ok()?;
    u.set_path("/api/dst/relay");
    u.set_query(None);
    Some(u.to_string())
}

async fn connect_ws(state: Arc<AppState>) {
    if !state.use_ws {
        return;
    }
    let url = match ws_url(&state.cfg.upstream) {
        Some(u) => u,
        None => return,
    };
    let mut reconnect_delay = 1000u64;

    loop {
        if state.verbose {
            println!("[relay-ws] connecting to {}", url);
        }
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _)) => {
                reconnect_delay = 1000;
                println!("[relay-ws] connected to {}", url);
                state.ws_ready.store(true, Ordering::SeqCst);

                let (mut write, mut read) = ws_stream.split();
                let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
                {
                    *state.ws_tx.lock().await = Some(tx);
                }

                // Writer task: drains outbound messages onto the socket.
                let writer = tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        if write.send(msg).await.is_err() {
                            break;
                        }
                    }
                });

                // Reader loop: dispatch incoming messages.
                while let Some(item) = read.next().await {
                    match item {
                        Ok(Message::Text(txt)) => handle_ws_message(&state, &txt).await,
                        Ok(Message::Binary(bin)) => {
                            if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                                handle_ws_message(&state, &txt).await;
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }

                // Disconnected: tear down.
                state.ws_ready.store(false, Ordering::SeqCst);
                *state.ws_tx.lock().await = None;
                writer.abort();
                // Reject all pending requests.
                {
                    let mut pending = state.ws_pending.lock().await;
                    pending.clear(); // dropping senders rejects the receivers
                }
                println!(
                    "[relay-ws] disconnected, reconnecting in {}ms",
                    reconnect_delay
                );
            }
            Err(e) => {
                if state.verbose {
                    eprintln!("[relay-ws] error: {}", e);
                }
                println!(
                    "[relay-ws] disconnected, reconnecting in {}ms",
                    reconnect_delay
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(reconnect_delay)).await;
        reconnect_delay = (reconnect_delay * 2).min(30_000);
    }
}

async fn handle_ws_message(state: &Arc<AppState>, txt: &str) {
    let msg: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(e) => {
            if state.verbose {
                eprintln!("[relay-ws] bad message: {}", e);
            }
            return;
        }
    };

    // Server push: a command was enqueued for this shard. Buffer it locally
    // so the mod's next poll gets it instantly without a round-trip.
    if msg.get("type").and_then(|t| t.as_str()) == Some("command") {
        if let (Some(shard_id), Some(command)) = (
            msg.get("shard_id").and_then(|s| s.as_str()),
            msg.get("command"),
        ) {
            let mut map = state.pushed_commands.lock().await;
            map.entry(shard_id.to_string())
                .or_default()
                .push(command.clone());
            if state.verbose {
                let ctype = command
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("?");
                println!("[relay-ws] push received: {} -> {}", ctype, shard_id);
            }
            return;
        }
    }

    // Response to a sync request we sent.
    if let Some(id) = msg.get("id").and_then(|i| i.as_u64()) {
        let mut pending = state.ws_pending.lock().await;
        if let Some(sender) = pending.remove(&id) {
            let data = msg.get("data").cloned().unwrap_or(Value::Null);
            let _ = sender.send(data);
        }
    }
}

fn is_ws_ready(state: &AppState) -> bool {
    state.ws_ready.load(Ordering::SeqCst)
}

async fn send_sync_via_ws(state: &Arc<AppState>, sync_data: Value) -> Result<Value, String> {
    if !is_ws_ready(state) {
        return Err("ws_not_ready".into());
    }
    let id = state.ws_seq.fetch_add(1, Ordering::SeqCst) + 1;
    let (tx, rx) = oneshot::channel::<Value>();
    {
        state.ws_pending.lock().await.insert(id, tx);
    }
    let payload = json!({ "id": id, "type": "sync", "data": sync_data });
    {
        let guard = state.ws_tx.lock().await;
        match guard.as_ref() {
            Some(sender) => {
                if sender.send(Message::Text(payload.to_string())).is_err() {
                    state.ws_pending.lock().await.remove(&id);
                    return Err("ws_send_failed".into());
                }
            }
            None => {
                state.ws_pending.lock().await.remove(&id);
                return Err("ws_not_ready".into());
            }
        }
    }
    match tokio::time::timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_)) => Err("websocket_closed".into()),
        Err(_) => {
            state.ws_pending.lock().await.remove(&id);
            Err("ws_timeout".into())
        }
    }
}

// Dedupe key matching relay.ts: `${type}|${queued_at}|${JSON(data)}`.
fn command_key(c: &Value) -> String {
    let ctype = c.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let queued = match c.get("queued_at") {
        Some(Value::Null) | None => String::new(),
        Some(v) => v.to_string().trim_matches('"').to_string(),
    };
    let data = c
        .get("data")
        .cloned()
        .unwrap_or_else(|| json!({}))
        .to_string();
    format!("{}|{}|{}", ctype, queued, data)
}

// ─── HTTP handler ───────────────────────────────────────────────────────

fn full(body: impl Into<Bytes>) -> Full<Bytes> {
    Full::new(body.into())
}

async fn handle(
    state: Arc<AppState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    state.request_count.fetch_add(1, Ordering::SeqCst);

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let path_and_query = match &query {
        Some(q) => format!("{}?{}", path, q),
        None => path.clone(),
    };

    // Status endpoints — handy for diagnostics without touching upstream.
    if path == "/" || path == "/relay-status" {
        let body = json!({
            "relay": "DSTP",
            "listening": format!("http://127.0.0.1:{}", state.cfg.port),
            "upstream": state.cfg.upstream,
            "requests": state.request_count.load(Ordering::SeqCst),
            "errors": state.error_count.load(Ordering::SeqCst),
            "lastUpstreamOkAt": state.last_upstream_ok_at.load(Ordering::SeqCst),
        });
        let s = serde_json::to_string_pretty(&body).unwrap_or_default();
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(full(s))
            .unwrap());
    }

    // Read request body up front (needed for both WS fast-path and HTTP).
    let req_headers = req.headers().clone();
    let body_bytes = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => Bytes::new(),
    };

    if state.verbose {
        println!("[relay] -> {} {}", method, path_and_query);
    }

    // Fast path: tunnel DST sync through the persistent WebSocket if it's up.
    if method == hyper::Method::POST && path == "/api/dst/sync" && is_ws_ready(&state) {
        if let Some(resp) = try_ws_sync(&state, &body_bytes).await {
            return Ok(resp);
        }
        // else fall through to HTTP path
    }

    // Build upstream target URL preserving path + query.
    let target = format!(
        "{}{}",
        state.cfg.upstream.trim_end_matches('/'),
        path_and_query
    );

    // Clone headers, strip hop-by-hop, fix Host.
    let upstream_host = url::Url::parse(&state.cfg.upstream)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default();

    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in req_headers.iter() {
        let n = name.as_str().to_ascii_lowercase();
        if n == "host" || n == "connection" || n == "content-length" {
            continue;
        }
        if let (Ok(hn), Ok(hv)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            headers.insert(hn, hv);
        }
    }
    if let Ok(hv) = reqwest::header::HeaderValue::from_str(&upstream_host) {
        headers.insert(reqwest::header::HOST, hv);
    }
    let xff = req_headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1");
    if let Ok(hv) = reqwest::header::HeaderValue::from_str(xff) {
        headers.insert("x-forwarded-for", hv);
    }
    headers.insert("x-dstp-relay", reqwest::header::HeaderValue::from_static("1"));
    if let Some(token) = &state.cfg.token {
        if let Ok(hv) = reqwest::header::HeaderValue::from_str(token) {
            headers.insert("x-dstp-relay-token", hv);
        }
    }

    let send_body = !(method == hyper::Method::GET || method == hyper::Method::HEAD);
    let rmethod = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);

    let mut builder = state.http.request(rmethod, &target).headers(headers);
    if send_body {
        builder = builder.body(body_bytes.to_vec());
    }

    // Timeout (10s) is configured on the client; map errors to 502/504.
    let started = std::time::Instant::now();
    match builder.send().await {
        Ok(res) => {
            state.last_upstream_ok_at.store(now_ms(), Ordering::SeqCst);
            state.metrics.lock().await.record(started.elapsed().as_secs_f64() * 1000.0);
            let status = res.status();
            let ct = res
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            // reqwest auto-decompresses; we rebuild headers cleanly so DST's
            // libcurl doesn't try to re-decompress plain bytes.
            let bytes = res.bytes().await.unwrap_or_default();

            if state.verbose {
                println!(
                    "[relay] <- {} {} {} ({}b)",
                    status.as_u16(),
                    method,
                    path,
                    bytes.len()
                );
            }

            let mut out = Response::builder().status(status.as_u16());
            if let Some(ct) = ct {
                out = out.header("content-type", ct);
            }
            out = out.header("content-length", bytes.len().to_string());
            Ok(out.body(full(bytes)).unwrap())
        }
        Err(e) => {
            state.error_count.fetch_add(1, Ordering::SeqCst);
            let is_timeout = e.is_timeout();
            eprintln!(
                "[relay] {} {} -> {}",
                method,
                path,
                if is_timeout {
                    "TIMEOUT after 10s".to_string()
                } else {
                    e.to_string()
                }
            );
            let err = json!({
                "error": if is_timeout { "relay_upstream_timeout" } else { "relay_upstream_unreachable" },
                "message": e.to_string(),
            });
            Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header("content-type", "application/json")
                .body(full(err.to_string()))
                .unwrap())
        }
    }
}

async fn try_ws_sync(state: &Arc<AppState>, body_bytes: &Bytes) -> Option<Response<Full<Bytes>>> {
    // DST's json.encode emits \' for single quotes (invalid JSON). Fix it.
    let raw = String::from_utf8_lossy(body_bytes);
    let fixed = raw.replace("\\'", "'");
    let sync_payload: Value = if fixed.is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&fixed) {
            Ok(v) => v,
            Err(_) => return None, // fall through to HTTP
        }
    };

    let shard_id = sync_payload
        .get("shard_id")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());

    // Drain locally-buffered commands for this shard.
    let local_commands: Vec<Value> = match &shard_id {
        Some(sid) => {
            let mut map = state.pushed_commands.lock().await;
            map.remove(sid).unwrap_or_default()
        }
        None => Vec::new(),
    };

    let started = std::time::Instant::now();
    let response = match send_sync_via_ws(state, sync_payload).await {
        Ok(v) => v,
        Err(e) => {
            if state.verbose {
                println!("[relay] WS sync failed ({}), falling back to HTTP", e);
            }
            return None;
        }
    };
    state.metrics.lock().await.record(started.elapsed().as_secs_f64() * 1000.0);

    // Merge + dedupe local (pushed) and remote (drained) commands.
    let mut by_key: HashMap<String, Value> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for c in &local_commands {
        let k = command_key(c);
        if !by_key.contains_key(&k) {
            order.push(k.clone());
        }
        by_key.insert(k, c.clone());
    }
    if let Some(cmds) = response.get("commands").and_then(|c| c.as_array()) {
        for c in cmds {
            let k = command_key(c);
            if !by_key.contains_key(&k) {
                order.push(k.clone());
            }
            by_key.insert(k, c.clone());
        }
    }
    let merged: Vec<Value> = order.into_iter().filter_map(|k| by_key.remove(&k)).collect();

    let mut merged_response = response.clone();
    if let Value::Object(ref mut map) = merged_response {
        map.insert("commands".to_string(), Value::Array(merged));
    }

    let body_str = merged_response.to_string();
    state.last_upstream_ok_at.store(now_ms(), Ordering::SeqCst);
    if state.verbose {
        let src = if !local_commands.is_empty() {
            format!(
                "{} pushed + {} drained",
                local_commands.len(),
                response
                    .get("commands")
                    .and_then(|c| c.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0)
            )
        } else {
            "backend".to_string()
        };
        println!(
            "[relay] <- WS 200 POST /api/dst/sync ({}b, {})",
            body_str.len(),
            src
        );
    }

    Some(
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .header("content-length", body_str.len().to_string())
            .body(full(body_str))
            .unwrap(),
    )
}

// ─── Live dashboard ─────────────────────────────────────────────────────

// Unicode bar levels for a compact sparkline.
const SPARK: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

// Render a list of values as a sparkline scaled to its own min/max.
fn sparkline(data: &[f64], width: usize) -> String {
    if data.is_empty() {
        return " ".repeat(width);
    }
    // Take the last `width` samples.
    let slice: Vec<f64> = data.iter().rev().take(width).rev().cloned().collect();
    let min = slice.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = (max - min).max(1e-9);
    let mut out = String::new();
    for v in &slice {
        let lvl = (((v - min) / range) * (SPARK.len() - 1) as f64).round() as usize;
        out.push(SPARK[lvl.min(SPARK.len() - 1)]);
    }
    // Left-pad if fewer samples than width.
    if slice.len() < width {
        let mut padded = " ".repeat(width - slice.len());
        padded.push_str(&out);
        return padded;
    }
    out
}

struct LatStats {
    avg: f64,
    min: f64,
    max: f64,
    last: f64,
    jitter: f64, // mean absolute deviation from avg
    n: usize,
}

fn lat_stats(data: &std::collections::VecDeque<f64>) -> LatStats {
    if data.is_empty() {
        return LatStats { avg: 0.0, min: 0.0, max: 0.0, last: 0.0, jitter: 0.0, n: 0 };
    }
    let n = data.len();
    let sum: f64 = data.iter().sum();
    let avg = sum / n as f64;
    let min = data.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let last = *data.back().unwrap();
    let jitter = data.iter().map(|v| (v - avg).abs()).sum::<f64>() / n as f64;
    LatStats { avg, min, max, last, jitter, n }
}

fn fmt_uptime(secs: i64) -> String {
    let s = secs.max(0);
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    if h > 0 { format!("{}h{:02}m{:02}s", h, m, sec) }
    else if m > 0 { format!("{}m{:02}s", m, sec) }
    else { format!("{}s", sec) }
}

// A health label for latency variation (jitter).
fn jitter_label(jitter: f64) -> &'static str {
    if jitter < 5.0 { "estavel" }
    else if jitter < 20.0 { "ok" }
    else if jitter < 50.0 { "instavel" }
    else { "ruim" }
}

// Inner content width of the box (between the ║ borders, minus the 2 leading
// spaces of padding used by every line).
const BOX_W: usize = 60;

// Display width of a string, treating each char as width 1. The box-drawing,
// arrows and dot glyphs we use are all single-column, so char count == columns.
fn disp_width(s: &str) -> usize {
    s.chars().count()
}

// Emit one dashboard line, padded to the box width by visual columns.
fn line(content: &str) {
    let w = disp_width(content);
    let pad = BOX_W.saturating_sub(w);
    println!("║ {}{} ║", content, " ".repeat(pad));
}

// Redraw the whole dashboard in place (clears screen, moves cursor home).
fn render_dashboard(cfg: &Config, m: &Metrics, reqs: u64, errs: u64, rps: f64, ws_up: bool, last_ok: i64, now: i64) {
    let st = lat_stats(&m.latencies);
    let spark = sparkline(&m.latencies.iter().cloned().collect::<Vec<_>>(), BOX_W - 2);
    let uptime = fmt_uptime((now - m.started_at) / 1000);
    let last_sync = if last_ok == 0 { "nunca".to_string() } else { format!("{}s atras", (now - last_ok) / 1000) };
    let ws = if ws_up { "● UP" } else { "○ DOWN" };

    let bar = "═".repeat(BOX_W + 2);
    print!("\x1b[2J\x1b[H"); // clear screen + cursor home
    println!("╔{}╗", bar);
    line(&format!("DSTP Relay · escutando 127.0.0.1:{}", cfg.port));
    line(&format!("→ {}", cfg.upstream));
    println!("╠{}╣", bar);
    line(&format!("Túnel WS  : {}", ws));
    line(&format!("Uptime    : {}", uptime));
    line(&format!("Último sync: {}", last_sync));
    line(&format!("Requests  : {} total · {:.1} req/s · {} err", reqs, rps, errs));
    println!("╠{}╣", bar);
    line("Latência upstream (ms)");
    line(&spark);
    line(&format!("atual {:.0}  ·  média {:.0}  ·  min {:.0}  ·  max {:.0}", st.last, st.avg, st.min, st.max));
    line(&format!("variação ±{:.0}ms  [{}]", st.jitter, jitter_label(st.jitter)));
    println!("╠{}╣", bar);
    line(&format!("Ctrl+C para sair · {} amostras", st.n));
    println!("╚{}╝", bar);
}

// ─── Banner & heartbeat ─────────────────────────────────────────────────

fn pad(s: &str, w: usize) -> String {
    let mut out = s.to_string();
    if out.len() > w {
        out.truncate(w);
    } else {
        while out.len() < w {
            out.push(' ');
        }
    }
    out
}

fn banner(cfg: &Config) {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                    DSTP Relay (running)                       ║");
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!("║  Listening:  http://127.0.0.1:{}║", pad(&cfg.port.to_string(), 31));
    println!("║  Upstream:   {}║", pad(&cfg.upstream, 49));
    println!("║                                                               ║");
    println!("║  In the DST mod config, set BACKEND_URL to:                   ║");
    println!("║    http://127.0.0.1:{}║", pad(&cfg.port.to_string(), 41));
    println!("║                                                               ║");
    println!("║  Keep this window open. Close it to stop the relay.           ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
}

// ─── main ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let verbose = std::env::var("DSTP_VERBOSE").ok().as_deref() == Some("1");
    let use_ws = std::env::var("DSTP_USE_WS").ok().as_deref() != Some("0");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build http client");

    // Resolve config (env > local file > remote git > baked). Uses `http` to
    // fetch the central config from the dstp-relay repo.
    let cfg = load_config(&http).await;

    let state = Arc::new(AppState {
        cfg: cfg.clone(),
        verbose,
        use_ws,
        http,
        request_count: AtomicU64::new(0),
        error_count: AtomicU64::new(0),
        last_upstream_ok_at: AtomicI64::new(0),
        metrics: Mutex::new(Metrics::new(now_ms())),
        ws_ready: AtomicBool::new(false),
        ws_seq: AtomicU64::new(0),
        ws_tx: Mutex::new(None),
        ws_pending: Mutex::new(HashMap::new()),
        pushed_commands: Mutex::new(HashMap::new()),
    });

    // Start the WS tunnel.
    {
        let st = state.clone();
        tokio::spawn(async move { connect_ws(st).await });
    }

    banner(&cfg);

    // Live dashboard, redrawn in place every second. In verbose mode we keep
    // the scrolling logs instead (the dashboard would fight with them).
    if !verbose {
        let st = state.clone();
        let dcfg = cfg.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.tick().await; // first tick is immediate; skip it
            // Give the banner a moment to be read before taking over the screen.
            tokio::time::sleep(Duration::from_secs(2)).await;
            loop {
                ticker.tick().await;
                let now = now_ms();
                let reqs = st.request_count.load(Ordering::SeqCst);
                let errs = st.error_count.load(Ordering::SeqCst);
                let last_ok = st.last_upstream_ok_at.load(Ordering::SeqCst);
                let ws_up = is_ws_ready(&st);

                let mut m = st.metrics.lock().await;
                let dt = ((now - m.last_tick_at) as f64 / 1000.0).max(0.001);
                let rps = (reqs.saturating_sub(m.last_req_count)) as f64 / dt;
                m.last_req_count = reqs;
                m.last_tick_at = now;
                render_dashboard(&dcfg, &m, reqs, errs, rps, ws_up, last_ok, now);
            }
        });
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], cfg.port));
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!();
            eprintln!("[relay] ERRO: a porta {} ja esta em uso.", cfg.port);
            eprintln!("[relay] Outro relay provavelmente ja esta rodando — feche-o,");
            eprintln!("[relay] ou rode com outra porta:  DSTP_PORT=47835 dstp-relay");
            eprintln!();
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("[relay] ERRO ao abrir a porta {}: {}", cfg.port, e);
            std::process::exit(1);
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("[relay] accept error: {}", e);
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let st = state.clone();
        let verbose = st.verbose;
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let st = st.clone();
                async move { handle(st, req).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                // Mirrors relay.ts error() handler: log and keep serving.
                if verbose {
                    eprintln!("[relay] server error: {}", e);
                }
            }
        });
    }
}
