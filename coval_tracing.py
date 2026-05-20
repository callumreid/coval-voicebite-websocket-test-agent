"""Per-session Coval OTel tracing for the VoiceBite WebSocket harness.

Each WebSocket session gets its own ``TracerProvider`` whose OTLP exporter
carries ``X-Simulation-Id`` for the simulation output ID Coval sends in the
initialization payload. Spans started before the ID arrives are buffered and
re-emitted against the per-session provider once the ID is known.

This module is intentionally self-contained so it works in the Fly container
image without pulling Django or other backend dependencies.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import time
from typing import Any
from typing import Iterator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.trace import SpanKind
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode

logger = logging.getLogger(__name__)

SERVICE_NAME = "voicebite-websocket-test-agent"
COVAL_TRACE_ENDPOINT = os.environ.get("COVAL_TRACE_ENDPOINT", "https://api.coval.dev/v1/traces")
COVAL_API_KEY_ENV = "COVAL_API_KEY"
DEFAULT_BUFFER_LIMIT = 512
DEFAULT_EXPORT_TIMEOUT_S = 30


@dataclass
class _BufferedSpan:
    span: ReadableSpan


class _BufferingExporter(SpanExporter):
    """Holds spans in memory until the real exporter is wired in."""

    def __init__(self, limit: int) -> None:
        self._buffer: deque[ReadableSpan] = deque(maxlen=limit)
        self._shutdown = False

    def export(self, spans):  # type: ignore[override]
        if self._shutdown:
            return SpanExportResult.FAILURE
        for span in spans:
            self._buffer.append(span)
        return SpanExportResult.SUCCESS

    def drain(self) -> list[ReadableSpan]:
        drained = list(self._buffer)
        self._buffer.clear()
        return drained

    def shutdown(self) -> None:  # type: ignore[override]
        self._shutdown = True
        self._buffer.clear()


class _SwitchableExporter(SpanExporter):
    """Routes spans to the buffering exporter, then to OTLP once activated."""

    def __init__(self, buffering: _BufferingExporter) -> None:
        self._active: SpanExporter = buffering
        self._buffering = buffering

    def activate(self, real_exporter: SpanExporter) -> int:
        drained = self._buffering.drain()
        if drained:
            real_exporter.export(drained)
        self._active = real_exporter
        return len(drained)

    def export(self, spans):  # type: ignore[override]
        return self._active.export(spans)

    def shutdown(self) -> None:  # type: ignore[override]
        self._active.shutdown()


class SessionTracing:
    """One Coval-correlated trace tree per WebSocket session."""

    def __init__(
        self,
        *,
        session_id: str,
        agent_id: str,
        service_name: str = SERVICE_NAME,
        buffer_limit: int = DEFAULT_BUFFER_LIMIT,
    ) -> None:
        self.session_id = session_id
        self.agent_id = agent_id
        self.simulation_id: str | None = None
        self._activated = False
        self._buffer = _BufferingExporter(limit=buffer_limit)
        self._switch = _SwitchableExporter(self._buffer)
        resource = Resource.create(
            {
                "service.name": service_name,
                "coval.session_id": session_id,
                "coval.agent_id": agent_id,
            }
        )
        self._provider = TracerProvider(resource=resource)
        self._processor = BatchSpanProcessor(
            self._switch,
            schedule_delay_millis=1000,
            max_export_batch_size=128,
        )
        self._provider.add_span_processor(self._processor)
        self._tracer = self._provider.get_tracer("coval.voicebite-websocket")

    @property
    def tracer(self) -> trace.Tracer:
        return self._tracer

    def activate_with_simulation_id(self, simulation_id: str) -> None:
        if self._activated:
            return
        api_key = os.environ.get(COVAL_API_KEY_ENV, "")
        if not api_key:
            logger.warning(
                "coval_tracing: %s not set; spans will buffer until shutdown for session=%s",
                COVAL_API_KEY_ENV,
                self.session_id,
            )
            return
        exporter = OTLPSpanExporter(
            endpoint=COVAL_TRACE_ENDPOINT,
            headers={
                "x-api-key": api_key,
                "X-Simulation-Id": simulation_id,
            },
            timeout=DEFAULT_EXPORT_TIMEOUT_S,
        )
        drained = self._switch.activate(exporter)
        self.simulation_id = simulation_id
        self._activated = True
        logger.info(
            "coval_tracing: activated OTLP export session=%s simulation_id=%s flushed=%d",
            self.session_id,
            simulation_id,
            drained,
        )

    def shutdown(self) -> None:
        try:
            self._provider.force_flush(timeout_millis=int(DEFAULT_EXPORT_TIMEOUT_S * 1000))
        except Exception as exc:  # noqa: BLE001
            logger.warning("coval_tracing: force_flush failed session=%s err=%s", self.session_id, exc)
        try:
            self._provider.shutdown()
        except Exception as exc:  # noqa: BLE001
            logger.warning("coval_tracing: shutdown failed session=%s err=%s", self.session_id, exc)

    @contextmanager
    def span(
        self,
        name: str,
        *,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: dict[str, Any] | None = None,
        parent: trace.Span | None = None,
    ) -> Iterator[trace.Span]:
        ctx = trace.set_span_in_context(parent) if parent is not None else None
        with self._tracer.start_as_current_span(
            name, context=ctx, kind=kind, attributes=attributes or {}
        ) as span:
            try:
                yield span
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise


def now_ms() -> int:
    return int(time.time() * 1000)
