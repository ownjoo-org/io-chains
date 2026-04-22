"""Top-level pytest config — strict visibility for the whole io-chains suite."""

import pytest

from oj_toolkit.diagnostics import strict_visibility


@pytest.fixture(autouse=True)
def _strict_visibility(request):
    """Fail tests that silently emit ERROR logs, lose async tasks, or die
    in background threads.

    Opt out per test with ``@pytest.mark.expect_error``.
    """
    if request.node.get_closest_marker('expect_error') is not None:
        yield
        return
    with strict_visibility():
        yield


def pytest_configure(config):
    config.addinivalue_line('markers', 'expect_error: suppress strict_visibility for this test')
