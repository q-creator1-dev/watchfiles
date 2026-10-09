"""Finite actual-backend fixtures; no provider invocation or live publication writes."""

import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from publication_wake import PublicationWake, monotonic_ns


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='publication-wake-')
        self.root = Path(self.temp.name)
        self.logs = []
        self.watchers = []

    def tearDown(self):
        for watcher in self.watchers:
            watcher.close()
        self.temp.cleanup()

    def watcher(self, root=None, **kwargs):
        watcher = PublicationWake([root or self.root], fallback_seconds=0.08, log=self.logs.append, **kwargs)
        self.watchers.append(watcher)
        return watcher

    def assert_change(self, watcher, path, event=None):
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            wake = watcher.wait()
            if any(os.path.normcase(p) == os.path.normcase(str(path)) and (event is None or c == event)
                   for c, p in wake.changes):
                return wake
        self.fail(f'No event for {path}; logs={self.logs}')

    def test_registration_precedes_first_scan_and_overlap_deduplicates(self):
        watcher = self.watcher()
        self.assertTrue(watcher.registered, self.logs)
        registered = watcher.registered_monotonic_ns
        request = self.root / 'request.json'
        request.write_text('{"race_id":"fixture"}', encoding='utf-8')
        published = monotonic_ns()
        seen, admitted = set(), []

        def same_handler():
            for p in self.root.glob('*.json'):
                rid = json.loads(p.read_text())['race_id']
                if rid not in seen:
                    admitted.append(rid)
                    seen.add(rid)

        same_handler()  # Startup reconciliation overlaps the already registered watcher.
        self.assert_change(watcher, request)
        same_handler()
        watcher.drain()
        same_handler()
        self.assertLess(registered, published)
        self.assertEqual(admitted, ['fixture'])

    def test_partial_write_does_not_imply_complete_json(self):
        watcher = self.watcher()
        request = self.root / 'request.json'
        with request.open('w', encoding='utf-8') as stream:
            stream.write('{"race_id":')
            stream.flush()
            os.fsync(stream.fileno())
            self.assert_change(watcher, request)
            with self.assertRaises(json.JSONDecodeError):
                json.loads(request.read_text())
            stream.write('"completed"}')
            stream.flush()
            os.fsync(stream.fileno())
        self.assert_change(watcher, request)
        self.assertEqual(json.loads(request.read_text())['race_id'], 'completed')

    def test_atomic_rename_and_delete(self):
        watcher = self.watcher()
        temporary, final = self.root / 'result.tmp', self.root / 'result.json'
        temporary.write_text('{"status":"OK"}')
        os.replace(temporary, final)
        self.assert_change(watcher, final, 1)
        self.assertEqual(json.loads(final.read_text())['status'], 'OK')
        final.unlink()
        self.assert_change(watcher, final, 3)

    def test_nested_original_publication(self):
        watcher = self.watcher()
        nested = self.root / 'race'
        nested.mkdir()
        request = nested / 'request.json'
        request.write_text('{"boats":[1,2,3,4,5,6]}')
        self.assert_change(watcher, nested)
        self.assertEqual(len(json.loads(request.read_text())['boats']), 6)

    def test_busy_capacity_reconciles_later_without_duplicate(self):
        watcher = self.watcher()
        request = self.root / 'pending.json'
        request.write_text('{"race_id":"pending"}')
        busy, pending, accepted = True, {}, set()

        def handler():
            for path in self.root.glob('*.json'):
                rid = json.loads(path.read_text())['race_id']
                if rid not in accepted:
                    pending[rid] = path
            if not busy:
                accepted.update(pending)
                pending.clear()

        self.assert_change(watcher, request)
        handler()
        self.assertEqual(set(pending), {'pending'})
        self.assertFalse(accepted)
        busy = False
        self.assertEqual(watcher.wait().reason, 'timeout')
        handler()
        handler()
        self.assertEqual(accepted, {'pending'})
        self.assertFalse(pending)

    def test_result_before_boundary_remains_reachable(self):
        watcher = self.watcher()
        path = self.root / 'result.json'
        path.write_text('{"status":"OK"}')
        admitted = []
        boundary = monotonic_ns() + 120_000_000
        self.assert_change(watcher, path)
        self.assertLess(monotonic_ns(), boundary)
        while not admitted:
            remaining = max(0, (boundary - monotonic_ns()) / 1e9)
            watcher.wait(timeout_seconds=remaining)
            if monotonic_ns() >= boundary and path.exists():
                admitted.append('decision')
        self.assertEqual(admitted, ['decision'])

    def test_timeout_does_not_wait_default_rust_five_seconds(self):
        watcher = self.watcher()
        start = time.monotonic()
        self.assertEqual(watcher.wait(timeout_seconds=0.02).reason, 'timeout')
        self.assertLess(time.monotonic() - start, 0.25)

    def test_missing_root_failure_is_visible_periodic_then_recovers(self):
        missing = self.root / 'later'
        watcher = self.watcher(missing)
        self.assertFalse(watcher.registered)
        start = time.monotonic()
        self.assertEqual(watcher.wait().reason, 'error')
        self.assertGreaterEqual(time.monotonic() - start, 0.06)
        self.assertTrue(any('PUBLICATION_WATCH_ERROR' in line for line in self.logs))
        missing.mkdir()
        watcher.wait()
        self.assertTrue(watcher.registered, self.logs)
        request = missing / 'request.json'
        request.write_text('{}')
        self.assert_change(watcher, request)

    def test_native_error_closes_and_preserves_fallback(self):
        watcher = self.watcher()
        original = watcher._watcher

        class FailedBackend:
            closed = False

            def watch(self, *_args):
                raise OSError('injected backend failure')

            def close(self):
                self.closed = True

        failed = FailedBackend()
        watcher._watcher = failed
        original.close()
        start = time.monotonic()
        wake = watcher.wait()
        self.assertEqual(wake.reason, 'error')
        self.assertIn('injected backend failure', wake.error)
        self.assertTrue(failed.closed)
        self.assertGreaterEqual(time.monotonic() - start, 0.06)
        watcher.wait(timeout_seconds=0.02)
        self.assertTrue(watcher.registered)

    def test_pinned_version_mismatch_visible_without_admission_gate(self):
        with patch('watchfiles._rust_notify.__version__', '9.9.9'):
            watcher = self.watcher()
        self.assertFalse(watcher.registered)
        self.assertIn('required 1.3.0', self.logs[-1])
        self.assertEqual(watcher.wait(timeout_seconds=0.01).reason, 'error')

    def test_actual_closed_native_watcher_is_visible_and_recovers(self):
        watcher = self.watcher()
        watcher._watcher.close()
        wake = watcher.wait()
        self.assertEqual(wake.reason, 'error')
        self.assertIn('RustNotify watcher closed', wake.error)
        watcher.wait()
        self.assertTrue(watcher.registered, self.logs)

    def test_deleted_publication_root_re_registers_after_recreation(self):
        root = self.root / 'publications'
        root.mkdir()
        watcher = self.watcher(root)
        root.rmdir()
        wake = watcher.wait()
        self.assertEqual(wake.reason, 'error')
        self.assertFalse(watcher.registered)
        root.mkdir()
        watcher.wait()
        self.assertTrue(watcher.registered, self.logs)
        request = root / 'result.json'
        request.write_text('{}')
        self.assert_change(watcher, request)

    def test_drain_returns_queued_changes(self):
        watcher = self.watcher()
        request = self.root / 'request.json'
        request.write_text('{}')
        time.sleep(0.025)  # Allow the OS notification, without advancing a generator.
        wake = watcher.drain()
        self.assertEqual(wake.reason, 'changes')
        self.assertTrue(any(p == str(request) for _, p in wake.changes))

    def test_stop_close_and_context_manager(self):
        stop = threading.Event()
        watcher = self.watcher(stop_event=stop)
        stop.set()
        self.assertEqual(watcher.wait().reason, 'stop')
        watcher.close()
        watcher.close()
        self.assertFalse(watcher.registered)
        self.assertEqual(watcher.drain().reason, 'stop')
        with self.watcher() as second:
            self.assertTrue(second.registered)
        self.assertFalse(second.registered)

    def test_noisy_changes_do_not_hide_deadline(self):
        watcher = self.watcher()
        stop = threading.Event()

        def noise():
            index = 0
            while not stop.is_set():
                (self.root / f'noise-{index}.tmp').write_text('noise')
                index += 1
                stop.wait(0.001)

        writer = threading.Thread(target=noise)
        writer.start()
        start = time.monotonic()
        try:
            watcher.wait(timeout_seconds=0.01)
            self.assertLess(time.monotonic() - start, 0.2)
        finally:
            stop.set()
            writer.join()

    @unittest.skipUnless(os.name == 'nt', 'actual Windows backend assertion')
    def test_actual_windows_backend_and_source_identity(self):
        watcher = self.watcher()
        self.assertIn('Recommended', watcher.backend)
        self.assertIn('ReadDirectoryChangesWatcher', watcher.backend)
        self.assertEqual(watcher.identity['watchfiles_version'], '1.3.0')
        self.assertEqual(len(watcher.identity['native_module_sha256']), 64)
        self.assertEqual(len(watcher.identity['module_sha256']), 64)


if __name__ == '__main__':
    unittest.main(verbosity=2)
