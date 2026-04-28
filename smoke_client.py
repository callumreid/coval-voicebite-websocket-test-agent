"""Smoke client for the VoiceBite WebSocket test agent."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import websockets


def _audio_message(audio_bytes: bytes) -> str:
    return json.dumps(
        {
            "action": "audio_message",
            "payload": {},
            "audio_bytes": base64.b64encode(audio_bytes).decode("ascii"),
            "sender": "customer",
        },
        separators=(",", ":"),
    )


def _sine_pcm(sample_rate_hz: int, duration_ms: int, channels: int) -> bytes:
    sample_count = int(sample_rate_hz * duration_ms / 1000)
    frames = bytearray()
    for index in range(sample_count):
        value = int(0.18 * 32767 * math.sin(2 * math.pi * 440 * index / sample_rate_hz))
        sample = value.to_bytes(2, byteorder="little", signed=True)
        frames.extend(sample * channels)
    return bytes(frames)


def _url_with_token(url: str, token: str | None) -> str:
    if not token:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["token"] = token
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


async def run(args: argparse.Namespace) -> None:
    audio = _sine_pcm(args.sample_rate_hz, args.duration_ms, args.channels)
    url = _url_with_token(args.url, args.token)
    print(f"connecting url={url}")

    async with websockets.connect(url) as websocket:
        while True:
            try:
                initial = await asyncio.wait_for(websocket.recv(), timeout=0.5)
            except TimeoutError:
                break
            print(f"initial frame: {initial[:300]}")

        await websocket.send(_audio_message(audio))
        print(f"sent audio bytes={len(audio)}")

        deadline = asyncio.get_running_loop().time() + args.timeout_seconds
        while True:
            timeout = max(0.1, deadline - asyncio.get_running_loop().time())
            message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            parsed = json.loads(message)
            print(f"received action={parsed.get('action')} event={parsed.get('event')}")
            if parsed.get("action") == "audio_message":
                echoed = base64.b64decode(parsed["audio_bytes"])
                print(f"received audio bytes={len(echoed)} sender={parsed.get('sender')}")
                return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?", default="ws://127.0.0.1:8000/ws")
    parser.add_argument("--token", default=None)
    parser.add_argument("--sample-rate-hz", type=int, default=16000)
    parser.add_argument("--channels", type=int, default=1)
    parser.add_argument("--duration-ms", type=int, default=120)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
