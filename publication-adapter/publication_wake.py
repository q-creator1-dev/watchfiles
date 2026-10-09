"""Wake an existing publication consumer without owning its admission or file parsing.

Construct before startup reconciliation, then use every wake (including errors and
timeouts) to run the same existing handler. Changes are hints, not complete files.
The consumer retains its deduplication, capacity checks and decision clock.
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

UPSTREAM_COMMIT = 'e74b8fc7d8d9e35c3b6bb88af0aef5ea82ee815e'
WATCHFILES_VERSION = '1.3.0'
_MODULE_PATH = str(Path(__file__).resolve())
_MODULE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
# Python3.12 on this Windows host uses GetTickCount64 for time.monotonic;
# perf_counter is the monotonic high-resolution QueryPerformanceCounter clock.
monotonic_ns = time.perf_counter_ns


@dataclass(frozen=True, slots=True)
class Wake:
    reason: str
    changes: frozenset[tuple[int, str]]
    observed_monotonic_ns: int
    error: str | None = None


class _Deadline:
    def __init__(self, owner: PublicationWake, deadline_ns: int):
        self.owner = owner
        self.deadline_ns = deadline_ns

    def is_set(self) -> bool:
        return self.owner._stopped() or monotonic_ns() >= self.deadline_ns


class PublicationWake:
    """One watcher owned by one existing consumer thread; no handler or supervisor.

    ``RustNotify`` registers synchronously in this constructor. Failed registration
    is visible and retried at the existing fallback cadence. ``wait`` observes the
    shorter of that cadence and a supplied decision-boundary budget. Rust checks
    the deadline every step; scheduler delay can still exceed the supplied budget.
    ``drain`` retrieves one queued batch, taking up to debounce + two steps plus
    scheduling delay. It is not a zero-time operation or a file-completeness test.
    Close from the owning thread, after its wait returns.
    """

    def __init__(
        self,
        roots: Iterable[str | os.PathLike[str]],
        *,
        fallback_seconds: float,
        log: Callable[[str], object],
        step_ms: int = 5,
        debounce_ms: int = 20,
        stop_event: threading.Event | None = None,
    ):
        if not math.isfinite(fallback_seconds) or fallback_seconds <= 0:
            raise ValueError('fallback_seconds must be finite and positive')
        if not isinstance(step_ms, int) or step_ms < 1:
            raise ValueError('step_ms must be a positive integer')
        if not isinstance(debounce_ms, int) or debounce_ms < step_ms:
            raise ValueError('debounce_ms must be an integer at least step_ms')
        self.roots = tuple(dict.fromkeys(os.path.abspath(os.fspath(p)) for p in roots))
        if not self.roots:
            raise ValueError('at least one publication root is required')
        self.fallback_seconds = fallback_seconds
        self.step_ms = step_ms
        self.debounce_ms = debounce_ms
        self._log = log
        self._stop_event = stop_event
        self._closed = threading.Event()
        self._watcher = None
        self._root_identities = None
        self._retry_at_ns = 0
        self._error = None
        self.backend = None
        self.dependency_version = None
        self.native_module_path = None
        self.native_module_sha256 = None
        self.registered_monotonic_ns = None
        self._register()

    @property
    def registered(self) -> bool:
        return self._watcher is not None

    @property
    def identity(self) -> dict:
        return {
            'module_path': _MODULE_PATH,
            'module_sha256': _MODULE_SHA256,
            'watchfiles_version': self.dependency_version,
            'required_watchfiles_version': WATCHFILES_VERSION,
            'native_module_path': self.native_module_path,
            'native_module_sha256': self.native_module_sha256,
            'upstream_commit': UPSTREAM_COMMIT,
            'roots': self.roots,
            'registered': self.registered,
            'registered_monotonic_ns': self.registered_monotonic_ns,
            'backend': self.backend,
            'force_polling': False,
            'step_ms': self.step_ms,
            'debounce_ms': self.debounce_ms,
            'fallback_seconds': self.fallback_seconds,
        }

    def _emit(self, message: str) -> None:
        try:
            self._log(message)
        except Exception:
            logging.getLogger(__name__).exception('PublicationWake log callback failed: %s', message)

    def _stopped(self) -> bool:
        return self._closed.is_set() or (self._stop_event is not None and self._stop_event.is_set())

    def _register(self) -> None:
        if self._stopped():
            return
        try:
            from watchfiles import _rust_notify
            from watchfiles._rust_notify import RustNotify, __version__

            self.dependency_version = __version__
            if __version__ != WATCHFILES_VERSION:
                raise RuntimeError(f'watchfiles version {__version__}; required {WATCHFILES_VERSION}')
            self.native_module_path = _rust_notify.__file__
            self.native_module_sha256 = hashlib.sha256(Path(self.native_module_path).read_bytes()).hexdigest()
            root_identities = self._stat_roots()
            # Explicit native selection, narrow roots, recursive for request.json
            # descendants; do not silently ignore denied paths or apply filters.
            watcher = RustNotify(list(self.roots), False, False, 300, True, False)
            self._watcher = watcher
            self._root_identities = root_identities
            self._check_roots()
            self.backend = ' '.join(repr(watcher).split())
            self.registered_monotonic_ns = monotonic_ns()
            self._error = None
            self._emit(f'PUBLICATION_WATCH_REGISTERED backend={self.backend} roots={self.roots}')
        except Exception as exc:
            self._fail('register', exc)

    def _stat_roots(self) -> tuple:
        return tuple((value.st_dev, value.st_ino) for value in (os.stat(root) for root in self.roots))

    def _check_roots(self) -> None:
        # Watching the old directory object after replacement cannot wake a
        # consumer of the new path. Retain its periodic reconciliation/retry.
        if self._stat_roots() != self._root_identities:
            raise OSError('publication root identity changed; watcher must register again')

    def _fail(self, operation: str, exc: Exception) -> None:
        watcher, self._watcher = self._watcher, None
        if watcher is not None:
            try:
                watcher.close()
            except Exception as close_exc:
                self._emit(f'PUBLICATION_WATCH_ERROR close {type(close_exc).__name__}: {close_exc}')
        error = f'{operation} {type(exc).__name__}: {exc}'
        if error != self._error:
            self._emit(f'PUBLICATION_WATCH_ERROR {error}; periodic reconciliation continues')
        self._error = error
        self._retry_at_ns = monotonic_ns() + int(self.fallback_seconds * 1e9)

    def _wake(self, reason: str, changes=()) -> Wake:
        return Wake(reason, frozenset(changes), monotonic_ns(), self._error if reason == 'error' else None)

    def _pause(self, seconds: float) -> None:
        if seconds <= 0 or self._stopped():
            return
        if self._stop_event is not None:
            self._stop_event.wait(seconds)
        else:
            self._closed.wait(seconds)

    def wait(self, timeout_seconds: float | None = None) -> Wake:
        if timeout_seconds is not None and (not math.isfinite(timeout_seconds) or timeout_seconds < 0):
            raise ValueError('timeout_seconds must be finite and nonnegative')
        if self._stopped():
            return self._wake('stop')
        if timeout_seconds == 0:
            return self.drain()
        budget = min(self.fallback_seconds, timeout_seconds) if timeout_seconds is not None else self.fallback_seconds
        deadline_ns = monotonic_ns() + int(budget * 1e9)
        if self._watcher is None:
            # Reach the retry boundary within this existing wait, rather than
            # delaying recovery by another full fallback interval.
            self._pause(max(0, (min(deadline_ns, self._retry_at_ns) - monotonic_ns()) / 1e9))
            if not self._stopped() and monotonic_ns() >= self._retry_at_ns:
                self._register()
        if self._watcher is not None:
            try:
                self._check_roots()
                result = self._watcher.watch(
                    self.debounce_ms, self.step_ms,
                    max(1, math.ceil((deadline_ns - monotonic_ns()) / 1e6)), _Deadline(self, deadline_ns)
                )
                if isinstance(result, set):
                    return self._wake('changes', result)
                if self._stopped() or result == 'signal':
                    return self._wake('stop')
                return self._wake('timeout')
            except Exception as exc:
                self._fail('watch', exc)
        # A failed watch must not turn the existing loop into a hot retry loop.
        self._pause(max(0, (deadline_ns - monotonic_ns()) / 1e9))
        return self._wake('stop' if self._stopped() else 'error')

    def drain(self) -> Wake:
        if self._stopped():
            return self._wake('stop')
        if self._watcher is None:
            return self._wake('error')
        try:
            self._check_roots()
            result = self._watcher.watch(self.debounce_ms, self.step_ms, 1, self._stop_event)
            if isinstance(result, set):
                return self._wake('changes', result)
            return self._wake('stop' if result in ('stop', 'signal') else 'timeout')
        except Exception as exc:
            self._fail('watch', exc)
            return self._wake('error')

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        watcher, self._watcher = self._watcher, None
        if watcher is not None:
            try:
                watcher.close()
            except Exception as exc:
                self._emit(f'PUBLICATION_WATCH_ERROR close {type(exc).__name__}: {exc}')

    def __enter__(self) -> PublicationWake:
        return self

    def __exit__(self, *_args) -> None:
        self.close()
