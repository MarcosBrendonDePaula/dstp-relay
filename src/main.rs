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
const BAKED_UPSTREAM: &str = "https://local.marcosbrendon.com";
// Port 47834 chosen from IANA unassigned range to avoid conflicts with
// common dev services (Node 3000, Vite 5173, Tomcat 8080, etc).
const BAKED_PORT: u16 = 47834;

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

fn load_config() -> Config {
    let mut cfg = Config {
        upstream: std::env::var("DSTP_UPSTREAM").unwrap_or_else(|_| BAKED_UPSTREAM.to_string()),
        port: std::env::var("DSTP_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .filter(|p| *p > 0)
            .unwrap_or(BAKED_PORT),
        token: std::env::var("DSTP_TOKEN").ok().filter(|s| !s.is_empty()),
    };

    // Look next to the executable first, then CWD as fallback (dev mode).
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
                        println!("[relay] Using config: {}", path.display());
                        return cfg;
                    }
                    Err(e) => eprintln!("[relay] Failed to read {}: {}", path.display(), e),
                },
                Err(e) => eprintln!("[relay] Failed to read {}: {}", path.display(), e),
            }
        }
    }
    cfg
}

// ─── Shared runtime state ───────────────────────────────────────────────

struct AppState {
    cfg: Config,
    verbose: bool,
    use_ws: bool,
    http: reqwest::Client,

    request_count: AtomicU64,
    error_count: AtomicU64,
    last_upstream_ok_at: AtomicI64,

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
    match builder.send().await {
        Ok(res) => {
            state.last_upstream_ok_at.store(now_ms(), Ordering::SeqCst);
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

    let response = match send_sync_via_ws(state, sync_payload).await {
        Ok(v) => v,
        Err(e) => {
            if state.verbose {
                println!("[relay] WS sync failed ({}), falling back to HTTP", e);
            }
            return None;
        }
    };

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
    let cfg = load_config();
    let verbose = std::env::var("DSTP_VERBOSE").ok().as_deref() == Some("1");
    let use_ws = std::env::var("DSTP_USE_WS").ok().as_deref() != Some("0");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build http client");

    let state = Arc::new(AppState {
        cfg: cfg.clone(),
        verbose,
        use_ws,
        http,
        request_count: AtomicU64::new(0),
        error_count: AtomicU64::new(0),
        last_upstream_ok_at: AtomicI64::new(0),
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

    // Heartbeat status line every 30s.
    {
        let st = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.tick().await; // skip immediate first tick
            loop {
                ticker.tick().await;
                let last = st.last_upstream_ok_at.load(Ordering::SeqCst);
                let status = if last == 0 {
                    "never connected".to_string()
                } else {
                    format!("last success {}s ago", (now_ms() - last) / 1000)
                };
                let ws_status = if is_ws_ready(&st) { "WS up" } else { "WS down" };
                println!(
                    "[relay] {} req, {} err, {}, upstream: {}",
                    st.request_count.load(Ordering::SeqCst),
                    st.error_count.load(Ordering::SeqCst),
                    ws_status,
                    status
                );
            }
        });
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], cfg.port));
    let listener = TcpListener::bind(addr).await.expect("failed to bind");

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
