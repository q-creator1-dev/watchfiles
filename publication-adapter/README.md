# Publication wake adapter

This standalone module wakes an existing durable-file consumer. The consumer still owns parsing, admission,
deduplication, capacity, accounts, pending calls and the decision clock. A filesystem notification is a hint to run
that same handler. It does not establish complete JSON, six-lane data, provider acceptance or an order.

The selected base is [watchfiles v1.3.0, e74b8fc](https://github.com/samuelcolvin/watchfiles/tree/e74b8fc7d8d9e35c3b6bb88af0aef5ea82ee815e).
The fork's `publication-v1.3.0` branch retains that exact Rust/Python runtime and its MIT license. Upstream authorship
and Samuel Colvin's copyright remain intact. Child1 implements this adapter and its qualification; Child2 owns the
WIN lane integration. The fork's later upstream `main` branch is not the selected dependency boundary.

The Windows native dependency is the official `watchfiles==1.3.0` CPython abi3 AMD64 wheel, verified against its
primary PyPI digest. The standalone module is installed byte-for-byte from the merged fork source. No rebuilt
Rust binary or changed upstream runtime is claimed. The selected wheel and dependencies are in
[dependency-lock.json](dependency-lock.json) and [requirements-windows-py312.lock](requirements-windows-py312.lock).

## Consumer interface

Final agreed WIN path: `C:/Users/hatti/win_lanes/publication_wake.py`.

```python
from publication_wake import PublicationWake

with PublicationWake([publication_root], fallback_seconds=10, log=log) as wake:
    reconcile_with_existing_handler()  # The native constructor already registered.
    while running:
        result = wake.wait(timeout_seconds=seconds_to_existing_decision_boundary)
        reconcile_with_existing_handler()  # Also for timeout/error, not only changes.
```

`PublicationWake(roots, *, fallback_seconds, log, step_ms=5, debounce_ms=20, stop_event=None)` registers synchronously
through the pinned `RustNotify` constructor. A Python `watch()` generator would not register until advanced.
The pinned [native source](https://github.com/samuelcolvin/watchfiles/blob/e74b8fc7d8d9e35c3b6bb88af0aef5ea82ee815e/src/lib.rs)
and [API stub](https://github.com/samuelcolvin/watchfiles/blob/e74b8fc7d8d9e35c3b6bb88af0aef5ea82ee815e/watchfiles/_rust_notify.pyi)
are the implementation references.

`wait(timeout_seconds=None)` waits for changes or the shorter of the existing fallback cadence and a positive
decision-boundary budget. It returns immutable `Wake(reason, changes, observed_monotonic_ns, error)`:

| Field | Meaning |
| --- | --- |
| `reason` | `changes`, `timeout`, `error` or `stop` |
| `changes` | Frozen set of `(event_type, absolute_path)` hints; 1 added, 2 modified, 3 deleted |
| `observed_monotonic_ns` | QPC/perf_counter timestamp when the adapter returns, not consumer admission time |
| `error` | Visible watcher failure details for an error wake |

`drain()` retrieves one queued native batch. It may take debounce plus two steps and scheduling delay.
`wait(timeout_seconds=0)` has the same drain semantics; it is not an immediate return. The WIN consumer uses
positive boundary budgets. A monotonic deadline stops native batching even under continuous noise. Native stop
can discard queued hints at that deadline, so the timeout must run the same full reconciliation. The durable
files remain authoritative. Use `drain` for bounded inspection, not a retry loop.

`close()` is idempotent; call it from the owning consumer thread after its wait returns. Context exit closes it.
No Python worker thread, subprocess, supervisor or scheduler is created by the adapter. The native dependency
owns its existing notification thread. `stop_event` may be an existing `threading.Event`.

`registered`, `backend`, `registered_monotonic_ns` and `identity` expose actual registration and loaded source.
Identity captures the module hash at import and records the native version, binary hash/path, backend, roots and
settings. Native selection is explicit (`force_polling=False`), recursive for original request descendants,
with no ignored permission errors or post-batch filters. Actual backend selection is recorded from Rust's repr.

Registration/watch failures log `PUBLICATION_WATCH_ERROR`, close the failed watcher and continue the consumer's
periodic cadence. Registration retries occur within that existing wait at the fallback boundary. The adapter
checks root identities so deleting or replacing a publication directory does not leave a watcher on the old
object. Missing, denied or replaced roots remain visible while full reconciliation can continue. A logging
callback failure is also emitted through Python logging. The adapter never changes admission behavior.

The WIN runner watches original publications with the retained 10s fallback. Its decider watches the current
day's completed results with the retained 3s fallback, recreating its watcher on rollover. A result arriving
before an existing decision window must remain reachable at that window; notification alone cannot replace
the decision-boundary wake.

## Clocks and acceptance

On the actual Python3.12 Windows host, `time.monotonic` uses `GetTickCount64` with 15.625ms resolution.
`time.perf_counter` uses monotonic `QueryPerformanceCounter` with 100ns reported resolution. This module exports
`monotonic_ns = time.perf_counter_ns`; all its monotonic fields use that clock. Consumer dispatch receipts must
use that same clock, never subtract `time.monotonic_ns()` from an adapter timestamp. Wall mapping is retained
separately, with its 15.625ms reported resolution; cross-host clock offset remains unknown.

`test_publication_wake.py` exercises actual native notification, registration/startup overlap and same-handler
deduplication, partial JSON, atomic rename, file/root deletion, native failure and recovery, occupied capacity,
early results and deadline wake, bounded drain, noise, stop/close and source identity. Provider submission is
not invoked. `measure_windows.py` measures a finite existing-handler fixture with 200 seeded JSON records and
growing publication population, including all reads and the fixture admission call. It retains raw publication
begin/end and dispatch stamps, missing/duplicate counts, percentiles, maxima, over 100ms counts, process CPU and
concurrent whole-machine CPU. Busy-capacity samples are reported separately.

Measurements and exact console-free command receipts live in `evidence/`. The available-capacity objective
does not promise 100ms, a provider start time or economic improvement. Actual lane dispatch, provider acceptance,
deployed consumption and ordinary outputs require Child2's integration receipts in
[Data450](https://github.com/q-creator1-dev/boatrace-data/issues/450).

## Validation and recovery

Run the finite fixtures from this directory with the selected dependency installed:

```text
python -B -m unittest -v test_publication_wake
python -B measure_windows.py --output PATH_TO_WINDOWS_MEASUREMENT.json
```

The fork's focused CI verifies unchanged upstream runtime/license and runs native fixtures on Windows and
Linux. Its large upstream CI matrix is retained for other branches and excluded only for the qualified
`publication-v1.3.0` target, which uses this focused workflow.

Install the hash-locked artifacts into the existing owned environment, without upgrading its existing packages,
then copy the authenticated merged module bytes to the agreed final path and verify the installed hash/import.
Record dependency additions and unchanged prior package versions. Runtime deployment remains Child2's boundary.
If import/registration fails, the optional consumer import and existing periodic reconciliation continue visibly.
Removing the standalone module restores that consumer's retained import-fallback path. Existing accounts,
original/results, F1/N1 and executor configuration are outside this module's writes.
