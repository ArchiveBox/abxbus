from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

try:
    from .performance_scenarios import PERF_SCENARIO_IDS, PerfInput, run_all_perf_scenarios, run_perf_scenario_by_id
except ImportError:  # pragma: no cover - direct script execution path
    from tests.performance_scenarios import PERF_SCENARIO_IDS, PerfInput, run_all_perf_scenarios, run_perf_scenario_by_id

TABLE_MATRIX = [
    ('50k-events', '1 bus x 50k events x 1 handler'),
    ('500-buses-x-100-events', '500 buses x 100 events x 1 handler'),
    ('1-event-x-50k-parallel-handlers', '1 bus x 1 event x 50k parallel handlers'),
    ('50k-one-off-handlers', '1 bus x 50k events x 50k one-off handlers'),
    ('worst-case-forwarding-timeouts', 'Worst case (N buses x N events x N handlers)'),
]


def _format_cell(result: dict[str, Any]) -> str:
    if result.get('ok') is False:
        error = str(result.get('error') or 'failed')
        compact = error.replace('\n', ' ').strip()
        if len(compact) > 42:
            compact = compact[:39] + '...'
        return f'`failed: {compact}`'

    ms_per_event = float(result['ms_per_event'])
    unit = str(result.get('ms_per_event_unit', 'event'))
    latency = f'{ms_per_event:.3f}ms/{unit}'

    peak_rss_kb_per_event = result.get('peak_rss_kb_per_event')
    if isinstance(peak_rss_kb_per_event, (int, float)):
        peak_unit = str(result.get('peak_rss_unit', 'event'))
        return f'`{latency}`, `{float(peak_rss_kb_per_event):.3f}kb/{peak_unit}`'
    return f'`{latency}`'


def _print_markdown_matrix(runtime_name: str, results: list[dict[str, Any]]) -> None:
    by_scenario = {str(result['scenario_id']): result for result in results}

    header_cols = ['Runtime'] + [label for _, label in TABLE_MATRIX]
    print('| ' + ' | '.join(header_cols) + ' |')
    print('|' + '|'.join([' ------------------ ' for _ in header_cols]) + '|')

    row_cells = [runtime_name]
    for scenario_id, _ in TABLE_MATRIX:
        result = by_scenario.get(scenario_id)
        if result is None:
            row_cells.append('`n/a`')
            continue
        row_cells.append(_format_cell(result))

    print('| ' + ' | '.join(row_cells) + ' |')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run Python runtime performance scenarios for abxbus')
    parser.add_argument('--scenario', type=str, default=None, help=f'One scenario id: {", ".join(PERF_SCENARIO_IDS)}')
    parser.add_argument(
        '--no-json',
        action='store_false',
        dest='json',
        help='Disable full JSON output (enabled by default).',
    )
    parser.set_defaults(json=True)
    parser.add_argument(
        '--in-process',
        action='store_true',
        help='Run all scenarios in one process (default runs each scenario in an isolated subprocess).',
    )
    parser.add_argument('--child-json', action='store_true', help=argparse.SUPPRESS)
    return parser


async def _run_scenario_in_subprocess(scenario_id: str) -> dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'tests.performance_runtime',
        '--scenario',
        scenario_id,
        '--child-json',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f'Perf child process failed for scenario={scenario_id!r} exit={proc.returncode} stderr={stderr.decode().strip()}'
        )
    payload = stdout.decode().strip()
    if not payload:
        raise RuntimeError(f'Perf child process produced no output for scenario={scenario_id!r}')
    return json.loads(payload)


async def _main_async() -> int:
    args = _build_parser().parse_args()
    logging.getLogger('abxbus').setLevel(logging.CRITICAL)

    perf_input = PerfInput(runtime_name='python', log=(lambda _: None) if args.child_json else print)

    if not args.child_json:
        print('[python] runtime perf harness starting')

    results: list[dict[str, Any]]

    if args.scenario:
        if args.scenario not in PERF_SCENARIO_IDS:
            raise ValueError(f'Unknown --scenario value {args.scenario!r}. Expected one of: {", ".join(PERF_SCENARIO_IDS)}')
        result = await run_perf_scenario_by_id(perf_input, args.scenario)
        result['scenario_id'] = args.scenario
        results = [result]
    elif args.in_process:
        raw_results: list[dict[str, Any]] = await run_all_perf_scenarios(perf_input)
        results = []
        for scenario_id, result in zip(PERF_SCENARIO_IDS, raw_results, strict=True):
            result_copy = dict(result)
            result_copy['scenario_id'] = scenario_id
            results.append(result_copy)
    else:
        results = []
        for scenario_id in PERF_SCENARIO_IDS:
            results.append(await _run_scenario_in_subprocess(scenario_id))

    if args.child_json:
        print(json.dumps(results[0], default=str))
        return 0

    print('[python] runtime perf harness complete')
    print('')
    print('Markdown matrix row (copy into README):')
    _print_markdown_matrix('Python', results)

    if args.json:
        print('')
        print(json.dumps(results, indent=2, default=str))

    return 0


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == '__main__':
    raise SystemExit(main())
