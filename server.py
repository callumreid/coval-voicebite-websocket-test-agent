"""VoiceBite/Checkmate-style WebSocket test agent.

This is a protocol harness for Coval's generic voice WebSocket simulator. It
implements the message shapes currently known from Checkmate's AsyncAPI spec:
`audio_message` frames with base64 `audio_bytes` and `system_notify` cart
updates. It intentionally keeps business behavior simple until Checkmate
confirms the remaining protocol details.
"""

from __future__ import annotations

import base64
import binascii
from collections import deque
from dataclasses import dataclass
from dataclasses import field
import json
import logging
import math
import os
import time
from typing import Any
from uuid import uuid4

from fastapi import FastAPI
from fastapi import WebSocket
from fastapi import status

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SERVICE_NAME = "voicebite-websocket-test-agent"
VERSION = "0.1.0"
DEFAULT_PUBLIC_BASE_URL = "https://coval-voicebite-websocket-test-agent.fly.dev"
VOICEBITE_AUDIO_TEMPLATE = '{"action":"audio_message","payload":{},"audio_bytes":"{{audio_data}}","sender":"AI"}'


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    sample_rate_hz: int = field(default_factory=lambda: _env_int("VOICEBITE_SAMPLE_RATE_HZ", 16000))
    channels: int = field(default_factory=lambda: _env_int("VOICEBITE_CHANNELS", 1))
    bytes_per_sample: int = 2
    echo_threshold_bytes: int = field(default_factory=lambda: _env_int("VOICEBITE_ECHO_THRESHOLD_BYTES", 3200))
    close_after_echoes: int = field(default_factory=lambda: _env_int("VOICEBITE_CLOSE_AFTER_ECHOS", 0))
    send_cart_on_connect: bool = field(default_factory=lambda: _env_bool("VOICEBITE_SEND_CART_ON_CONNECT", True))
    send_cart_after_first_echo: bool = field(
        default_factory=lambda: _env_bool("VOICEBITE_SEND_CART_AFTER_FIRST_ECHO", False)
    )
    send_session_ready: bool = field(default_factory=lambda: _env_bool("VOICEBITE_SEND_SESSION_READY", False))
    send_tone_on_connect: bool = field(default_factory=lambda: _env_bool("VOICEBITE_SEND_TONE_ON_CONNECT", False))
    require_auth: bool = field(default_factory=lambda: _env_bool("VOICEBITE_REQUIRE_AUTH", False))
    auth_token: str = field(default_factory=lambda: os.environ.get("VOICEBITE_AUTH_TOKEN", ""))
    public_base_url: str = field(default_factory=lambda: os.environ.get("PUBLIC_BASE_URL", DEFAULT_PUBLIC_BASE_URL))
    max_sessions: int = field(default_factory=lambda: _env_int("VOICEBITE_MAX_SESSIONS", 50))
    max_events_per_session: int = field(default_factory=lambda: _env_int("VOICEBITE_MAX_EVENTS_PER_SESSION", 200))

    @property
    def frame_bytes_per_second(self) -> int:
        return self.sample_rate_hz * self.channels * self.bytes_per_sample


@dataclass
class SessionEvent:
    at: float
    direction: str
    kind: str
    size_bytes: int = 0
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "at": self.at,
            "direction": self.direction,
            "kind": self.kind,
            "size_bytes": self.size_bytes,
            "detail": self.detail,
        }


@dataclass
class SessionState:
    session_id: str
    agent_id: str
    connected_at: float
    client: str
    max_events: int
    disconnected_at: float | None = None
    received_audio_messages: int = 0
    received_audio_bytes: int = 0
    sent_audio_messages: int = 0
    sent_audio_bytes: int = 0
    non_audio_messages: int = 0
    errors: list[str] = field(default_factory=list)
    events: deque[SessionEvent] = field(default_factory=deque)

    def add_event(self, event: SessionEvent) -> None:
        self.events.append(event)
        while len(self.events) > self.max_events:
            self.events.popleft()

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "agent_id": self.agent_id,
            "connected_at": self.connected_at,
            "disconnected_at": self.disconnected_at,
            "client": self.client,
            "received_audio_messages": self.received_audio_messages,
            "received_audio_bytes": self.received_audio_bytes,
            "sent_audio_messages": self.sent_audio_messages,
            "sent_audio_bytes": self.sent_audio_bytes,
            "non_audio_messages": self.non_audio_messages,
            "errors": self.errors[-20:],
            "events": [event.as_dict() for event in self.events],
        }


app = FastAPI(title="Coval VoiceBite WebSocket Test Agent", version=VERSION)
_sessions: dict[str, SessionState] = {}
_session_order: deque[str] = deque()


def _get_settings() -> Settings:
    return Settings()


def reset_sessions() -> None:
    _sessions.clear()
    _session_order.clear()


def _store_session(session: SessionState, settings: Settings) -> None:
    _sessions[session.session_id] = session
    _session_order.append(session.session_id)
    while len(_session_order) > settings.max_sessions:
        old_session_id = _session_order.popleft()
        _sessions.pop(old_session_id, None)


def _redacted_settings(settings: Settings) -> dict[str, Any]:
    return {
        "sample_rate_hz": settings.sample_rate_hz,
        "channels": settings.channels,
        "bytes_per_sample": settings.bytes_per_sample,
        "echo_threshold_bytes": settings.echo_threshold_bytes,
        "close_after_echoes": settings.close_after_echoes,
        "send_cart_on_connect": settings.send_cart_on_connect,
        "send_cart_after_first_echo": settings.send_cart_after_first_echo,
        "send_session_ready": settings.send_session_ready,
        "send_tone_on_connect": settings.send_tone_on_connect,
        "require_auth": settings.require_auth,
        "auth_token_configured": bool(settings.auth_token),
        "public_base_url": settings.public_base_url,
        "max_sessions": settings.max_sessions,
        "max_events_per_session": settings.max_events_per_session,
    }


def _recommended_coval_metadata(settings: Settings) -> dict[str, Any]:
    websocket_url = settings.public_base_url.rstrip("/").replace("https://", "wss://").replace("http://", "ws://")
    return {
        "model_type": "MODEL_TYPE_WEBSOCKET",
        "endpoint": f"{websocket_url}/ws",
        "connection_mode": "direct",
        "initialization_json": "",
        "send_sample_rate_hertz": settings.sample_rate_hz,
        "receive_sample_rate_hertz": settings.sample_rate_hz,
        "handshake_ready_message_type": "",
        "handshake_requires_session_id": False,
        "send_audio_template": VOICEBITE_AUDIO_TEMPLATE,
        "message_type_path": "action",
        "audio_message_type_value": "audio_message",
        "audio_data_path": "audio_bytes",
        "audio_encoding": "pcm",
        "receive_audio_channels": settings.channels,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME, "version": VERSION}


@app.get("/config")
def config() -> dict[str, Any]:
    settings = _get_settings()
    return {
        "service": SERVICE_NAME,
        "version": VERSION,
        "settings": _redacted_settings(settings),
        "recommended_coval_metadata": _recommended_coval_metadata(settings),
    }


@app.get("/sessions")
def sessions() -> dict[str, Any]:
    return {
        "count": len(_session_order),
        "sessions": [_sessions[session_id].as_dict() for session_id in reversed(_session_order) if session_id in _sessions],
    }


@app.get("/sessions/{session_id}")
def session(session_id: str) -> dict[str, Any]:
    current = _sessions.get(session_id)
    if current is None:
        return {"error": "session_not_found", "session_id": session_id}
    return current.as_dict()


@app.post("/sessions/reset")
def reset() -> dict[str, str]:
    reset_sessions()
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_default(websocket: WebSocket) -> None:
    await _websocket_handler(websocket, agent_id="default")


@app.websocket("/ws/{agent_id}")
async def websocket_with_agent_id(websocket: WebSocket, agent_id: str) -> None:
    await _websocket_handler(websocket, agent_id=agent_id)


async def _websocket_handler(websocket: WebSocket, agent_id: str) -> None:
    settings = _get_settings()
    if settings.require_auth and not _is_authorized(websocket, settings):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    session = SessionState(
        session_id=f"vb-{uuid4().hex[:12]}",
        agent_id=agent_id,
        connected_at=time.time(),
        client=f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown",
        max_events=settings.max_events_per_session,
    )
    _store_session(session, settings)
    logger.info("VoiceBite test WebSocket connected session=%s agent_id=%s", session.session_id, agent_id)

    audio_buffer = bytearray()
    echo_count = 0
    cart_sent_after_first_echo = False

    try:
        if settings.send_session_ready:
            await _send_json(
                websocket,
                session,
                {"type": "session_ready", "session_id": session.session_id},
                kind="session_ready",
            )
        if settings.send_cart_on_connect:
            await _send_json(websocket, session, _cart_updated_message(), kind="cart_updated")
        if settings.send_tone_on_connect:
            tone = _sine_pcm(duration_ms=250, settings=settings)
            await _send_audio(websocket, session, tone, settings=settings)
            echo_count += 1

        while True:
            incoming = await websocket.receive()
            if incoming["type"] == "websocket.disconnect":
                break

            audio_bytes = _extract_audio_bytes(incoming, session)
            if audio_bytes is None:
                continue

            session.received_audio_messages += 1
            session.received_audio_bytes += len(audio_bytes)
            session.add_event(
                SessionEvent(
                    at=time.time(),
                    direction="in",
                    kind="audio_message",
                    size_bytes=len(audio_bytes),
                    detail={"sample_rate_hz": settings.sample_rate_hz, "channels": settings.channels},
                )
            )

            audio_buffer.extend(audio_bytes)
            threshold = max(settings.echo_threshold_bytes, 1)
            while len(audio_buffer) >= threshold:
                chunk = bytes(audio_buffer[:threshold])
                del audio_buffer[:threshold]
                await _send_audio(websocket, session, chunk, settings=settings)
                echo_count += 1
                if settings.send_cart_after_first_echo and not cart_sent_after_first_echo:
                    await _send_json(websocket, session, _cart_updated_message(), kind="cart_updated")
                    cart_sent_after_first_echo = True
                if settings.close_after_echoes and echo_count >= settings.close_after_echoes:
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
                    return
    finally:
        session.disconnected_at = time.time()
        logger.info(
            "VoiceBite test WebSocket disconnected session=%s received_audio=%s sent_audio=%s errors=%s",
            session.session_id,
            session.received_audio_messages,
            session.sent_audio_messages,
            len(session.errors),
        )


def _is_authorized(websocket: WebSocket, settings: Settings) -> bool:
    if not settings.auth_token:
        return False
    authorization = websocket.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip() == settings.auth_token
    token = websocket.query_params.get("token") or websocket.query_params.get("api_key")
    return token == settings.auth_token


def _extract_audio_bytes(incoming: dict[str, Any], session: SessionState) -> bytes | None:
    if incoming.get("bytes") is not None:
        audio_bytes = incoming["bytes"]
        session.add_event(
            SessionEvent(at=time.time(), direction="in", kind="binary_audio", size_bytes=len(audio_bytes))
        )
        return audio_bytes

    text = incoming.get("text")
    if text is None:
        return None

    try:
        message = json.loads(text)
    except json.JSONDecodeError as exc:
        _record_error(session, f"invalid_json: {exc}")
        return None

    action = message.get("action")
    if action != "audio_message":
        session.non_audio_messages += 1
        session.add_event(
            SessionEvent(
                at=time.time(),
                direction="in",
                kind=str(action or message.get("type") or "non_audio"),
                detail=_safe_message_detail(message),
            )
        )
        return None

    audio_data = message.get("audio_bytes")
    if not isinstance(audio_data, str) or not audio_data:
        _record_error(session, "audio_message missing non-empty audio_bytes string")
        return None

    try:
        return base64.b64decode(audio_data, validate=True)
    except (binascii.Error, ValueError) as exc:
        _record_error(session, f"invalid audio_bytes base64: {exc}")
        return None


async def _send_audio(websocket: WebSocket, session: SessionState, audio_bytes: bytes, *, settings: Settings) -> None:
    message = _audio_message(audio_bytes)
    await _send_json(websocket, session, message, kind="audio_message", size_bytes=len(audio_bytes))
    session.sent_audio_messages += 1
    session.sent_audio_bytes += len(audio_bytes)
    session.add_event(
        SessionEvent(
            at=time.time(),
            direction="out",
            kind="audio_message",
            size_bytes=len(audio_bytes),
            detail={"sample_rate_hz": settings.sample_rate_hz, "channels": settings.channels},
        )
    )


async def _send_json(
    websocket: WebSocket,
    session: SessionState,
    message: dict[str, Any],
    *,
    kind: str,
    size_bytes: int = 0,
) -> None:
    await websocket.send_text(json.dumps(message, separators=(",", ":")))
    if kind != "audio_message":
        session.add_event(SessionEvent(at=time.time(), direction="out", kind=kind, size_bytes=size_bytes))


def _audio_message(audio_bytes: bytes) -> dict[str, Any]:
    return {
        "action": "audio_message",
        "payload": {},
        "audio_bytes": base64.b64encode(audio_bytes).decode("ascii"),
        "sender": "AI",
    }


def _cart_updated_message() -> dict[str, Any]:
    return {
        "action": "system_notify",
        "payload": {},
        "event": "ocb:cart-updated",
        "data": {
            "cart": "1x latte, 1x blueberry muffin",
            "json_cart_ocb": [
                {"name": "latte", "quantity": 1, "price": 4.50},
                {"name": "blueberry muffin", "quantity": 1, "price": 3.25},
            ],
            "total": 8.52,
            "subtotal": 7.75,
            "tax": 0.77,
            "summary": "Cart contains 1 latte and 1 blueberry muffin.",
        },
    }


def _sine_pcm(*, duration_ms: int, settings: Settings) -> bytes:
    sample_count = int(settings.sample_rate_hz * duration_ms / 1000)
    frames = bytearray()
    for index in range(sample_count):
        value = int(0.18 * 32767 * math.sin(2 * math.pi * 440 * index / settings.sample_rate_hz))
        sample = value.to_bytes(2, byteorder="little", signed=True)
        frames.extend(sample * settings.channels)
    return bytes(frames)


def _safe_message_detail(message: dict[str, Any]) -> dict[str, Any]:
    detail = {
        "keys": sorted(message.keys()),
        "action": message.get("action"),
        "event": message.get("event"),
        "type": message.get("type"),
    }
    if "data" in message and isinstance(message["data"], dict):
        detail["data_keys"] = sorted(message["data"].keys())
    return detail


def _record_error(session: SessionState, message: str) -> None:
    session.errors.append(message)
    session.add_event(SessionEvent(at=time.time(), direction="in", kind="error", detail={"message": message}))
    logger.warning("VoiceBite test session=%s error=%s", session.session_id, message)
