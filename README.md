# Coval VoiceBite WebSocket Test Agent

Always-on protocol harness for the Checkmate / VoiceBite WebSocket shape shared
with Coval. It lets Coval's `MODEL_TYPE_WEBSOCKET` simulator validate the
generic JSON-audio path without depending on Checkmate staging uptime.

This is intentionally a protocol test agent, not a faithful copy of Checkmate's
business logic. The current customer spec only defines message schemas, so the
server mirrors those schemas and keeps behavior deterministic until Checkmate
confirms the missing details.

## What It Simulates

- `wss://.../ws` direct WebSocket connection.
- No initialization payload by default.
- No `session_ready` handshake by default.
- Inbound and outbound JSON audio frames:

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
  "send_audio_template": "{\"action\":\"audio_message\",\"payload\":{},\"audio_bytes\":\"{{audio_data}}\",\"sender\":\"AI\"}",
  "message_type_path": "action",
  "audio_message_type_value": "audio_message",
  "audio_data_path": "audio_bytes",
  "audio_encoding": "pcm",
  "receive_audio_channels": 1
}
```

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
- `VOICEBITE_SEND_CART_ON_CONNECT`: default `true`.
- `VOICEBITE_SEND_CART_AFTER_FIRST_ECHO`: default `false`.
- `VOICEBITE_SEND_SESSION_READY`: default `false`.
- `VOICEBITE_SEND_TONE_ON_CONNECT`: default `false`.
- `VOICEBITE_REQUIRE_AUTH`: default `false`.
- `VOICEBITE_AUTH_TOKEN`: bearer/query token when auth is enabled.
- `VOICEBITE_CLOSE_AFTER_ECHOS`: default `0`, meaning keep connection open.

## Known Gaps Pending Checkmate Answers

The harness assumes base64-encoded raw PCM16LE mono audio at 16 kHz because that
is the configuration Coval currently needs for the generic simulator work. It
does not yet prove:

- exact Checkmate codec/sample rate/channel count;
- whether `audio_bytes` is definitely base64 PCM or another binary string form;
- which side should send `sender=AI`;
- auth/query/header/session lifecycle;
- whether cart events are agent-to-client only, client-to-agent only, or
  bidirectional;
- realistic Checkmate turn-taking/business behavior.
