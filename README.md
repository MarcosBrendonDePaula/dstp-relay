# DSTP Relay

Tiny HTTP forwarder that makes the DSTP mod work with a **remote** DSTP backend.

## Why this exists

Don't Starve Together's Lua sandbox only allows HTTP calls to `127.0.0.1` / `localhost`. This is a hardcoded whitelist in the game binary — you cannot bypass it from Lua.

If you want to host DSTP centrally (one backend, many DST servers connecting to it), each DST server host has to run this relay on their machine. The relay listens on `127.0.0.1:3000`, passes the DST sandbox check, and forwards every request to your public DSTP backend.

```
DST Server (user's machine)
   │
   ▼  HTTP 127.0.0.1:3000  (passes DST sandbox)
dstp-relay.exe
   │
   ▼  HTTPS
https://your-backend.example.com  (your central DSTP)
```

## Install & run

1. Download the binary for your OS from the latest release:
   - Windows: `dstp-relay.exe`
   - Linux:   `dstp-relay-linux`
   - macOS:   `dstp-relay-mac` (Apple Silicon)
2. Put it in any folder. (Optional) drop a `dstp-relay.config.json` next to it
   to override the upstream panel — see Configuration below. Without a config
   file it uses the baked-in defaults.
3. Run it: double-click on Windows, or `./dstp-relay-linux` on Linux/macOS
   (you may need `chmod +x` first). A window opens showing the relay status.
   Keep it open.
4. Install the DSTP mod in DST. It's already configured to use
   `http://127.0.0.1:47834` (the relay's port) — no configuration needed.

Done. The mod talks to the relay on localhost; the relay forwards to your
chosen panel backend.

The relay is a single self-contained ~2MB native binary (Rust) with no
runtime dependencies — nothing to install.

## Configuration

Config file `dstp-relay.config.json` next to the binary:

| Option | Env var | Default | Description |
|---|---|---|---|
| `upstream` | `DSTP_UPSTREAM` | `https://local.marcosbrendon.com` | Central backend URL |
| `port` | `DSTP_PORT` | `47834` | Local port to listen on. **Must match the mod's BACKEND_URL port.** |
| `token` | `DSTP_TOKEN` | `null` | Optional shared secret sent as `X-DSTP-Relay-Token` header |

Env vars override the config file. The config file overrides the baked defaults.
Fields starting with `_` (like `_comment`, `_docs`) are ignored, so you can keep
notes inline.

## Build from source

Requires the Rust toolchain (`rustup`).

```bash
cd relay
cargo build --release        # produces target/release/dstp-relay(.exe)
```

Cross-compile to another platform with `cargo build --release --target <triple>`
(e.g. `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc`, `aarch64-apple-darwin`).
The release CI builds all three on native runners.

## Security notes

- The relay binds to `127.0.0.1` only — never exposed to LAN/WAN.
- All traffic is TLS terminated at the upstream if it's HTTPS.
- If your backend requires auth, use `DSTP_TOKEN` so the backend can verify requests come from a legitimate relay, not a stray script.
