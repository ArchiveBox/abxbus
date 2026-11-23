"""Exercise the native exporter with the real OpenTelemetry SDK."""

import json
from io import StringIO
from pathlib import Path

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

from abxbus import BaseEvent, EventBus
from abxbus.middlewares import OtelTracingMiddleware


@pytest.mark.asyncio
async def test_exports_children_before_closing_their_parents(tmp_path: Path) -> None:
    class WriteFile(BaseEvent[None]):
        text: str

    class CopyFile(BaseEvent[None]):
        pass

    output = StringIO()
    provider = TracerProvider()
    provider.add_span_processor(
        SimpleSpanProcessor(ConsoleSpanExporter(out=output, formatter=lambda span: span.to_json(indent=None) + '\n'))
    )
    bus = EventBus(name='FileBus', middlewares=[OtelTracingMiddleware(tracer=provider.get_tracer('test'))])

    async def write_file(event: WriteFile) -> None:
        (tmp_path / 'result.txt').write_text(event.text)

    async def copy_file(event: CopyFile) -> None:
        await event.emit(WriteFile(text=(tmp_path / 'source.txt').read_text()))

    bus.on(CopyFile, copy_file)
    bus.on(WriteFile, write_file)
    (tmp_path / 'source.txt').write_text('real nested file operation')
    try:
        await bus.emit(CopyFile())
        await bus.wait_until_idle()
        assert (tmp_path / 'result.txt').read_text() == 'real nested file operation'
        spans = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(spans) == 4
        assert len({span['context']['trace_id'] for span in spans}) == 1
        positions = {span['context']['span_id']: index for index, span in enumerate(spans)}
        for index, span in enumerate(spans):
            if span['parent_id']:
                assert index < positions[span['parent_id']], 'A parent must remain open until its descendants are exported'
        assert spans[-1]['name'] == 'FileBus.emit(CopyFile)'
    finally:
        await bus.destroy()
        provider.shutdown()
