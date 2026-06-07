"""Shared test fixtures for hermes-kchat.

`pusher.py` tests need only aiohttp. `adapter.py`/standalone tests need the
hermes-agent host packages (`gateway`, `hermes_cli`); those modules call
`pytest.importorskip("gateway...")` at their top so they skip cleanly when
hermes-agent is not installed. This conftest also pre-registers the "kchat"
platform name so `Platform("kchat")` resolves during adapter construction
(at runtime the plugin loader does this; in tests we must do it ourselves).
"""

import pytest


@pytest.fixture(scope="session", autouse=True)
def _register_kchat_platform():
    """Make Platform('kchat') resolvable in tests (no-op if gateway absent)."""
    try:
        from gateway.platform_registry import platform_registry, PlatformEntry
    except Exception:
        yield  # hermes-agent not installed; adapter tests will importorskip
    else:
        if not platform_registry.is_registered("kchat"):
            platform_registry.register(
                PlatformEntry(
                    name="kchat",
                    label="kChat",
                    adapter_factory=lambda config: None,
                    check_fn=lambda: True,
                    source="plugin",
                )
            )
        yield
