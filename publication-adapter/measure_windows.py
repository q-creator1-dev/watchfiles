"""Finite publication-to-fixture-admission measurement on the selected backend.

This does not invoke a provider or alter original/result publication roots. Keep
its fixture admission distinct from the receiving lane's deployed measurements.
"""
import argparse
import ctypes
import datetime
import hashlib
import json
import math
import os
import platform
import shutil
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

from publication_wake import PublicationWake, monotonic_ns


def system_cpu():
    values = (ctypes.c_ulonglong(), ctypes.c_ulonglong(), ctypes.c_ulonglong())
    if not ctypes.windll.kernel32.GetSystemTimes(*(ctypes.byref(v) for v in values)):
        raise ctypes.WinError()
    return tuple(v.value for v in values)


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {'n': 0, 'p50_ms': None, 'p95_ms': None, 'p99_ms': None, 'max_ms': None}
    result = {'n': len(ordered), 'min_ms': min(ordered), 'max_ms': max(ordered),
              'over_100ms': sum(x > 100 for x in ordered), 'negative': sum(x < 0 for x in ordered)}
    for label, fraction in (('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99)):
        result[label] = ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]
    return result


def measure(base, name, count, step, debounce, *, load=False, busy_seconds=0):
    with tempfile.TemporaryDirectory(prefix=f'{name}-', dir=base) as temp:
        root = Path(temp)
        # A growing receiving directory: every reconciliation reads complete JSON
        # including200 already admitted records, rather than timing only a callback.
        for index in range(200):
            (root / f'seed-{index}.json').write_text(json.dumps({'id': f'seed-{index}', 'payload': 'x' * 2048}))
        accepted = {f'seed-{i}' for i in range(200)}
        admissions, published, pending, admitted_cycles = {}, {}, {}, {}
        logs, reasons = [], Counter()
        cycles, native_calls = [], []
        finished, stop_load = threading.Event(), threading.Event()
        duplicate_admissions = 0
        cpu_start = system_cpu()
        process_start = time.process_time_ns()
        started = monotonic_ns()
        wall_start = time.time_ns()
        capacity_at = started + int(busy_seconds * 1e9)
        payload = 'x' * 2048

        def producer():
            try:
                for index in range(count):
                    identity = f'publication-{index}'
                    temporary = root / f'{identity}.tmp'
                    final = root / f'{identity}.json'
                    temporary.write_text(json.dumps({'id': identity, 'payload': payload}), encoding='utf-8')
                    before = monotonic_ns()
                    os.replace(temporary, final)
                    after = monotonic_ns()
                    published[identity] = {'id': identity, 'publication_begin_ns': before, 'publication_end_ns': after}
                    finished.wait(.004 if load else .015)
            finally:
                finished.set()

        def background_load():
            block = b'x' * (1024 * 1024)
            index = 0
            while not stop_load.is_set():
                hashlib.sha256(block).digest()
                if index % 4 == 0:
                    (root / f'noise-{index}.tmp').write_bytes(b'notification load')
                index += 1

        def existing_handler_fixture(wake=None):
            nonlocal duplicate_admissions
            cycle = {'cycle': len(cycles), 'native_return_ns': native_calls[-1]['returned_ns'] if wake else None,
                     'adapter_return_ns': wake.observed_monotonic_ns if wake else None,
                     'handler_start_ns': monotonic_ns(), 'parse_total_ns': 0, 'files_parsed': 0}
            parsed_at = {}
            for path in root.glob('*.json'):
                parse_started = monotonic_ns()
                try:
                    value = json.loads(path.read_text(encoding='utf-8'))
                except (OSError, ValueError):
                    continue
                parsed = monotonic_ns()
                cycle['parse_total_ns'] += parsed - parse_started
                cycle['files_parsed'] += 1
                identity = value['id']
                parsed_at[identity] = parsed
                if identity not in accepted:
                    pending[identity] = path
            cycle['scan_end_ns'] = monotonic_ns()
            if monotonic_ns() >= capacity_at:
                for identity in list(pending):
                    if identity in admissions:
                        duplicate_admissions += 1
                    # This is the fixture's admission attempt itself, after the
                    # full scan, not an enqueue or notification timestamp.
                    admissions[identity] = monotonic_ns()
                    admitted_cycles[identity] = (cycle['cycle'], parsed_at.get(identity))
                    accepted.add(identity)
                    del pending[identity]
            cycle['handler_end_ns'] = monotonic_ns()
            cycles.append(cycle)

        class TimedNative:
            """Measure the real C-extension return; kernel callback time is unknown."""
            def __init__(self, native):
                self.native = native

            def watch(self, *args):
                begin = monotonic_ns()
                result = self.native.watch(*args)
                returned = monotonic_ns()
                native_calls.append({'started_ns': begin, 'returned_ns': returned,
                                     'reason': 'changes' if isinstance(result, set) else result,
                                     'change_count': len(result) if isinstance(result, set) else 0})
                return result

            def close(self):
                self.native.close()

        with PublicationWake([root], fallback_seconds=.25, log=logs.append, step_ms=step, debounce_ms=debounce) as watcher:
            if not watcher.registered:
                raise RuntimeError(logs)
            registered = watcher.identity
            watcher._watcher = TimedNative(watcher._watcher)
            existing_handler_fixture()  # Startup scan only after registration.
            writer = threading.Thread(target=producer, name='fixture-publication')
            worker = threading.Thread(target=background_load, name='fixture-load') if load else None
            if worker:
                worker.start()
            writer.start()
            deadline = started + 30_000_000_000
            while (not finished.is_set() or len(admissions) < count) and monotonic_ns() < deadline:
                wake = watcher.wait()
                reasons[wake.reason] += 1
                existing_handler_fixture(wake)
            writer.join()
            stop_load.set()
            if worker:
                worker.join()
            existing_handler_fixture()
        ended = monotonic_ns()
        cpu_end = system_cpu()
        cpu_total = (cpu_end[1] - cpu_start[1]) + (cpu_end[2] - cpu_start[2])
        rows = []
        for identity, row in published.items():
            dispatch = admissions.get(identity)
            cycle_index, parsed_at = admitted_cycles.get(identity, (None, None))
            cycle = cycles[cycle_index] if cycle_index is not None else None
            row.update({'fixture_dispatch_ns': dispatch,
                        'capacity_available_at_publication': row['publication_end_ns'] >= capacity_at,
                        'latency_from_publication_end_ms': None if dispatch is None else (dispatch-row['publication_end_ns'])/1e6,
                        'latency_from_publication_begin_ms': None if dispatch is None else (dispatch-row['publication_begin_ns'])/1e6,
                        'cycle': cycle_index, 'parsed_ns': parsed_at})
            if cycle is not None and cycle['native_return_ns'] is not None:
                row['decomposition_ms'] = {
                    'publication_complete_to_native_batch_return': (cycle['native_return_ns']-row['publication_end_ns'])/1e6,
                    'native_batch_return_to_adapter_return': (cycle['adapter_return_ns']-cycle['native_return_ns'])/1e6,
                    'adapter_return_to_handler_start': (cycle['handler_start_ns']-cycle['adapter_return_ns'])/1e6,
                    'handler_scan_parse': (cycle['scan_end_ns']-cycle['handler_start_ns'])/1e6,
                    'handler_parse_io_sum': cycle['parse_total_ns']/1e6,
                    'scan_end_to_admission_attempt': (dispatch-cycle['scan_end_ns'])/1e6,
                }
                row['publication_after_native_batch_return'] = row['publication_end_ns'] > cycle['native_return_ns']
            rows.append(row)
        available = [r['latency_from_publication_end_ms'] for r in rows
                     if r['capacity_available_at_publication'] and r['fixture_dispatch_ns'] is not None]
        waiting = [r['latency_from_publication_end_ms'] for r in rows
                   if not r['capacity_available_at_publication'] and r['fixture_dispatch_ns'] is not None]
        return {'name': name, 'step_ms': step, 'debounce_ms': debounce, 'publication_count': len(published),
                'fixture_admission_count': len(admissions), 'missing': sorted(set(published)-set(admissions)),
                'duplicate_admissions': duplicate_admissions, 'pending_at_end': sorted(pending),
                'capacity_available_distribution': distribution(available), 'capacity_waiting_distribution': distribution(waiting),
                'wall_duration_s': (ended-started)/1e9, 'process_cpu_s': (time.process_time_ns()-process_start)/1e9,
                'system_cpu_percent_over_interval': 100*(1-(cpu_end[0]-cpu_start[0])/cpu_total) if cpu_total else None,
                'seed_files': 200, 'payload_bytes': 2048, 'load': load, 'busy_seconds': busy_seconds,
                'clock_mapping': {'wall_time_ns': wall_start, 'perf_counter_ns': started,
                                  'cross_host_offset': None},
                'registration': registered, 'wake_reasons': dict(reasons), 'logs': logs, 'publications': rows,
                'native_calls': native_calls, 'handler_cycles': cycles,
                'native_timestamp_meaning': 'C-extension watch return with GIL; kernel callback arrival is not exposed',
                'worst_samples': sorted(rows, key=lambda row: row['latency_from_publication_end_ms'] or -1, reverse=True)[:10]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', choices=['upstream-default', 'candidate-available', 'candidate-load', 'candidate-busy'])
    args = parser.parse_args()
    if os.name != 'nt':
        raise SystemExit('This qualification requires the actual Windows backend')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {'measured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'python': platform.python_version(), 'platform': platform.platform(),
              'clock_info': {k: vars(time.get_clock_info(k)) for k in ('time', 'monotonic', 'perf_counter')},
              'disk_free_bytes': shutil.disk_usage(args.output.parent).free,
              'scope': 'Finite actual-Windows fixture admission; provider latency and deployed lane consumption are not measured',
              'runs': []}
    for settings in [('upstream-default', 120, 50, 1600, False, 0),
                     ('candidate-available', 300, 5, 20, False, 0),
                     ('candidate-load', 300, 5, 20, True, 0),
                     ('candidate-busy', 300, 5, 20, True, .5)]:
        name, count, step, debounce, load, busy = settings
        if args.case and args.case != name:
            continue
        run = measure(args.output.parent, name, count, step, debounce, load=load, busy_seconds=busy)
        report['runs'].append(run)
        args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({k: v for k, v in run.items() if k not in
                          ('publications', 'logs', 'registration', 'native_calls', 'handler_cycles', 'worst_samples')}), flush=True)
    if any(r['missing'] or r['duplicate_admissions'] or r['pending_at_end'] for r in report['runs']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
