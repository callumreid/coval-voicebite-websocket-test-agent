import base64
from pathlib import Path

from fastapi.testclient import TestClient

from server import app
from server import reset_sessions


client = TestClient(app)


def setup_function():
    reset_sessions()


def _audio_message(audio_bytes: bytes) -> dict:
    return {
        "action": "audio_message",
        "payload": {},
        "audio_bytes": base64.b64encode(audio_bytes).decode("ascii"),
        "sender": "customer",
    }


def test_health_and_config():
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    config = client.get("/config").json()
    metadata = config["recommended_coval_metadata"]
    assert metadata["model_type"] == "MODEL_TYPE_WEBSOCKET"
    assert metadata["message_type_path"] == "action"
    assert metadata["audio_data_path"] == "audio_bytes"
    assert metadata["receive_audio_channels"] == 1


def test_websocket_emits_cart_and_echoes_voicebite_audio():
    pcm = b"\x01\x00" * 1600

    with client.websocket_connect("/ws") as websocket:
        cart = websocket.receive_json()
        assert cart["action"] == "system_notify"
        assert cart["event"] == "ocb:cart-updated"

        websocket.send_json(_audio_message(pcm))
        response = websocket.receive_json()

    assert response["action"] == "audio_message"
    assert response["sender"] == "AI"
    assert base64.b64decode(response["audio_bytes"]) == pcm

    sessions = client.get("/sessions").json()["sessions"]
    assert len(sessions) == 1
    assert sessions[0]["received_audio_messages"] == 1
    assert sessions[0]["sent_audio_messages"] == 1


def test_websocket_can_send_canned_speech(monkeypatch):
    pcm = b"\x01\x00" * 1600
    canned_path = Path(__file__).with_name("assets") / "voicebite-agent-reply.pcm"
    monkeypatch.setenv("VOICEBITE_RESPONSE_MODE", "canned_speech")
    monkeypatch.setenv("VOICEBITE_CANNED_AUDIO_PATH", str(canned_path))
    monkeypatch.setenv("VOICEBITE_CANNED_RESPONSE_CHUNK_MS", "1000")

    with client.websocket_connect("/ws") as websocket:
        websocket.receive_json()
        websocket.send_json(_audio_message(pcm))
        response = websocket.receive_json()

    assert response["action"] == "audio_message"
    assert response["sender"] == "AI"
    assert base64.b64decode(response["audio_bytes"]) != pcm

    session = client.get("/sessions").json()["sessions"][0]
    assert session["received_audio_messages"] == 1
    assert session["sent_audio_messages"] >= 1


def test_websocket_records_non_audio_messages():
    with client.websocket_connect("/ws/test-agent") as websocket:
        websocket.receive_json()
        websocket.send_json({"action": "system_notify", "payload": {}, "event": "ocb:cart-updated", "data": {}})

    session = client.get("/sessions").json()["sessions"][0]
    assert session["agent_id"] == "test-agent"
    assert session["non_audio_messages"] == 1


def test_invalid_audio_base64_is_recorded():
    with client.websocket_connect("/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"action": "audio_message", "payload": {}, "audio_bytes": "not-base64", "sender": "x"})

    session = client.get("/sessions").json()["sessions"][0]
    assert session["errors"]
    assert "invalid audio_bytes base64" in session["errors"][0]
