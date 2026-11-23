"""Exercise the native exporter with the real OpenTelemetry SDK."""

import json
from io import StringIO
from pathlib import Path

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import get_current_span

from abxbus import BaseEvent, EventBus
from abxbus.middlewares import OtelTracingMiddleware


@pytest.mark.asyncio
@pytest.mark.parametrize('expire_child', [False, True])
async def test_exports_children_before_closing_their_parents(tmp_path: Path, expire_child: bool) -> None:
    class WriteFile(BaseEvent[None]):
        text: str

    class CopyFile(BaseEvent[None]):
        pass

    class ReadFile(BaseEvent[str]):
        pass

    output = StringIO()
    provider = TracerProvider()
    provider.add_span_processor(
        SimpleSpanProcessor(ConsoleSpanExporter(out=output, formatter=lambda span: span.to_json(indent=None) + '\n'))
    )
    tracer = provider.get_tracer('test')
    bus = EventBus(name='FileBus', middlewares=[OtelTracingMiddleware(tracer=tracer)])

    async def write_file(event: WriteFile) -> None:
        with tracer.start_as_current_span('file.write'):
            (tmp_path / 'result.txt').write_text(event.text)

    async def copy_file(event: CopyFile) -> None:
        child = event.emit(WriteFile(text=(tmp_path / 'source.txt').read_text()))
        await child
        if expire_child:
            child.event_ttl = 0
        read = await event.emit(ReadFile())
        assert await read.event_result() == child.text
        if expire_child:
            assert await bus.find(WriteFile, past=True, future=False) is None

    async def read_file(event: ReadFile) -> str:
        return (tmp_path / 'result.txt').read_text()

    bus.on(CopyFile, copy_file)
    bus.on(WriteFile, write_file)
    bus.on(ReadFile, read_file)
    (tmp_path / 'source.txt').write_text('real nested file operation')
    try:
        root = await bus.emit(CopyFile())
        await root.event_results_list()
        await bus.wait_until_idle()
        assert (tmp_path / 'result.txt').read_text() == 'real nested file operation'
        spans = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(spans) == 7
        assert len({span['context']['trace_id'] for span in spans}) == 1
        positions = {span['context']['span_id']: index for index, span in enumerate(spans)}
        for index, span in enumerate(spans):
            if span['parent_id']:
                assert index < positions[span['parent_id']], 'A parent must remain open until its descendants are exported'
        assert spans[-1]['name'] == 'FileBus.emit(CopyFile)'
        assert not get_current_span().get_span_context().is_valid
    finally:
        await bus.destroy()
        provider.shutdown()


@pytest.mark.asyncio
async def test_file_failure_records_errors_without_leaking_context(tmp_path: Path) -> None:
    class ReadFile(BaseEvent[str]):
        path: str

    output = StringIO()
    provider = TracerProvider()
    provider.add_span_processor(
        SimpleSpanProcessor(ConsoleSpanExporter(out=output, formatter=lambda span: span.to_json(indent=None) + '\n'))
    )
    tracer = provider.get_tracer('test')
    bus = EventBus(name='ReadBus', middlewares=[OtelTracingMiddleware(tracer=tracer)])

    def read_file(event: ReadFile) -> str:
        with tracer.start_as_current_span('file.read'):
            return Path(event.path).read_text()

    bus.on(ReadFile, read_file)
    try:
        missing = await bus.emit(ReadFile(path=str(tmp_path / 'missing.txt')))
        with pytest.raises(FileNotFoundError):
            await missing.event_result()
        (tmp_path / 'present.txt').write_text('read after failure')
        present = await bus.emit(ReadFile(path=str(tmp_path / 'present.txt')))
        assert await present.event_result() == 'read after failure'
        await bus.wait_until_idle()
        spans = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(spans) == 6
        assert len({span['context']['trace_id'] for span in spans}) == 2
        failed = [span for span in spans if span['status']['status_code'] == 'ERROR']
        assert len(failed) == 3
        assert all(span['events'][0]['attributes']['exception.type'] == 'FileNotFoundError' for span in failed)
        assert not get_current_span().get_span_context().is_valid
    finally:
        await bus.destroy()
        provider.shutdown()
