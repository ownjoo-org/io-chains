"""Ensure each test starts with a clean oj-persistence _BACKENDS registry.

Manager instances share a process-scoped backend registry. When tests create
different Managers that register different specs, the registry normally
reference-counts correctly — but if a prior test somehow didn't fully close
(background exception, file-lock race), the stale backend sticks around and
poisons the next test.
"""
import asyncio
import logging
import sys

import pytest

from oj_persistence import manager as manager_module


@pytest.fixture(autouse=True)
def _reset_backend_registry():
    """Clear the registry and close any stragglers after each test."""
    yield
    with manager_module._BACKENDS._lock:
        backends = list(manager_module._BACKENDS._backends.values())
        manager_module._BACKENDS._backends.clear()
        manager_module._BACKENDS._refcounts.clear()
    for backend in backends:
        try:
            asyncio.new_event_loop().run_until_complete(backend.aclose())
        except Exception as e:
            print(f'teardown close failed: {e!r}', file=sys.stderr)


@pytest.fixture(autouse=True)
def _capture_unhandled_exceptions(caplog):
    """Surface any exception that escaped an async task — the default asyncio
    handler logs them at ERROR, which pytest captures into caplog. Asserting
    there are none gives tests a chance to fail loudly instead of silently.
    """
    caplog.set_level(logging.WARNING)
    yield
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    if errors:
        msgs = '\n'.join(f'  {r.levelname} {r.name}: {r.getMessage()}' for r in errors)
        raise AssertionError(f'unhandled error(s) during test:\n{msgs}')
