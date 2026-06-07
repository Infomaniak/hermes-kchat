"""Infomaniak kChat platform adapter for Hermes Agent."""

__all__ = ["register"]


def __getattr__(name):
    # PEP 562: defer importing adapter (and gateway.*) until `register` is
    # actually accessed by the plugin loader. Keeps `hermes_kchat.pusher`
    # importable with only aiohttp installed.
    if name == "register":
        from .adapter import register
        return register
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
