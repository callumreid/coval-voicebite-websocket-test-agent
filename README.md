# Coval VoiceBite WebSocket Test Agent

Always-on protocol harness for the Checkmate / VoiceBite WebSocket shape shared
with Coval. It lets Coval's `MODEL_TYPE_WEBSOCKET` simulator validate the
generic JSON-audio path without depending on Checkmate staging uptime.

This is intentionally a protocol test agent, not a faithful copy of Checkmate's
business logic. It mirrors the AsyncAPI message shapes that Checkmate confirmed
on 2026-05-07 and keeps non-audio behavior deterministic.

## Customer-confirmed protocol summary (2026-05-07)

- Canonical endpoint: `wss://voicebite-websocket-app-staging.azurewebsites.net/ws`
  (the `voicebite-websocket-app-coval.azuresites.net/ws` host is documentation
  metadata only).
- Authentication: Bearer token, header
  `Authorization: Bearer <ACCESS_TOKEN>` or query string
  `?token=<ACCESS_TOKEN>`.
- No init / no ready / no ping / no end-of-call message. Connection is active
  immediately after upgrade. Standard close frames terminate.
- Audio is JSON only: base64 PCM in `audio_bytes`. Linear PCM, 16000 Hz, 16-bit
  signed little-endian, mono. Preferred chunk duration 20-100 ms.
- `audio_message` is bidirectional. Coval (client) sends `sender: "USER"`.
  Agent (server) sends `sender: "AI"`.
- `system_notify` / `ocb:cart-updated` events are server-originated. Coval
  receives them and may assert against them; Coval does not emit them.

## What It Simulates

- `wss://.../ws` direct WebSocket connection.
- Optional Bearer auth via header or `?token=` query param when
  `VOICEBITE_REQUIRE_AUTH=true`.
- No initialization payload by default.
- No `session_ready` handshake by default.
- Inbound JSON audio frames from Coval:

```json
{
  "action": "audio_message",
  "payload": {},
  "audio_bytes": "<base64 pcm_s16le audio>",
  "sender": "USER"
}
```

- Outbound JSON audio frames from the harness:

```json
{
  "action": "audio_message",
  "payload": {},
  "audio_bytes": "<base64 pcm_s16le audio>",
  "sender": "AI"
}
```

- `system_notify` / `ocb:cart-updated` frames:

```json
{
  "action": "system_notify",
  "payload": {},
  "event": "ocb:cart-updated",
  "data": {
    "cart": "1x latte, 1x blueberry muffin",
    "json_cart_ocb": [],
    "total": 8.52,
    "subtotal": 7.75,
    "tax": 0.77,
    "summary": "Cart contains 1 latte and 1 blueberry muffin."
  }
}
```

By default, the local server echoes received PCM back in the same VoiceBite-style
envelope. The deployed Fly app runs in `canned_speech` mode, sending a short
spoken PCM response so Coval simulations produce audible agent audio and
transcription.

## Endpoints

### `GET /health`

Liveness probe.

### `GET /config`

Returns runtime settings and a ready-to-copy Coval `MODEL_TYPE_WEBSOCKET`
metadata payload.

### `GET /sessions`

Returns recent WebSocket sessions, including frame counts, byte counts, and
non-audio/error events. Raw audio bytes are not stored.

### `POST /sessions/reset`

Clears in-memory session history.

### `WS /ws`

Default WebSocket endpoint.

### `WS /ws/{agent_id}`

Same protocol, with an arbitrary agent id captured in session debug output.

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn server:app --port 8000 --reload
```

Smoke test:

```bash
python smoke_client.py ws://127.0.0.1:8000/ws
curl -sS http://127.0.0.1:8000/sessions
```

Unit tests:

```bash
pytest -q
```

## Deploy

The Fly app is `coval-voicebite-websocket-test-agent`.

```bash
fly apps create coval-voicebite-websocket-test-agent
fly deploy --ha=false
```

Verify:

```bash
curl -sS https://coval-voicebite-websocket-test-agent.fly.dev/health
python smoke_client.py wss://coval-voicebite-websocket-test-agent.fly.dev/ws
```

## Coval Agent Metadata

Use the `/config` endpoint for the current canonical payload. The default shape
is:

```json
{
  "model_type": "MODEL_TYPE_WEBSOCKET",
  "endpoint": "wss://coval-voicebite-websocket-test-agent.fly.dev/ws",
  "connection_mode": "direct",
  "initialization_json": "",
  "send_sample_rate_hertz": 16000,
  "receive_sample_rate_hertz": 16000,
  "handshake_ready_message_type": "",
  "handshake_requires_session_id": false,
  "send_audio_template": "{\"action\":\"audio_message\",\"payload\":{},\"audio_bytes\":\"{{audio_data}}\",\"sender\":\"USER\"}",
  "message_type_path": "action",
  "audio_message_type_value": "audio_message",
  "audio_data_path": "audio_bytes",
  "audio_encoding": "pcm",
  "receive_audio_channels": 1,
  "non_audio_event_message_types": ["system_notify"]
}
```

When dialing the real Checkmate staging endpoint, also set
`authorization_header` to `"Bearer <ACCESS_TOKEN>"`.

## Runtime Knobs

All knobs are optional environment variables:

- `VOICEBITE_SAMPLE_RATE_HZ`: default `16000`.
- `VOICEBITE_CHANNELS`: default `1`.
- `VOICEBITE_ECHO_THRESHOLD_BYTES`: minimum inbound turn audio before responding; default `3200`.
- `VOICEBITE_RESPONSE_MODE`: `echo` or `canned_speech`; local default `echo`.
- `VOICEBITE_CANNED_AUDIO_PATH`: raw PCM16LE mono 16 kHz response file.
- `VOICEBITE_CANNED_RESPONSE_LIMIT`: default `2`.
- `VOICEBITE_CANNED_RESPONSE_CHUNK_MS`: default `100`.
- `VOICEBITE_CANNED_TURN_SILENCE_MS`: silence gap after inbound audio before a canned response; default `800`.
- `VOICEBITE_CANNED_SILENCE_RMS_THRESHOLD`: PCM16 RMS value below which inbound audio counts as silence; default `250`.
- `VOICEBITE_CANNED_FORCE_RESPONSE_BYTES`: optional inbound byte count that forces a canned response even if no silence is detected; default `0`.
- `VOICEBITE_SEND_CART_ON_CONNECT`: default `true`.
- `VOICEBITE_SEND_CART_AFTER_FIRST_ECHO`: default `false`.
- `VOICEBITE_SEND_SESSION_READY`: default `false`.
- `VOICEBITE_SEND_TONE_ON_CONNECT`: default `false`.
- `VOICEBITE_REQUIRE_AUTH`: default `false`.
- `VOICEBITE_AUTH_TOKEN`: bearer/query token when auth is enabled.
- `VOICEBITE_CLOSE_AFTER_ECHOS`: default `0`, meaning keep connection open.

## Remaining Caveats

The harness now matches every protocol detail Checkmate confirmed, but it is
still a deterministic protocol harness rather than a faithful copy of their
business behavior. It does not simulate realistic Checkmate turn-taking, full
menu/cart logic, or upstream orchestration. For end-to-end confidence we still
need a single live run against `wss://voicebite-websocket-app-staging.azurewebsites.net/ws`
with a Coval-scoped Bearer token from Checkmate.
