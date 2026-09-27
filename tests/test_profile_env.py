"""Settings must come from the profile being served, not the process env."""

import pytest

pytest.importorskip("agent.secret_scope")  # needs hermes-agent host

from agent.secret_scope import (
    reset_secret_scope,
    set_multiplex_active,
    set_secret_scope,
)
from gateway.config import PlatformConfig


def test_adapter_reads_token_and_url_from_active_profile(monkeypatch):
    from hermes_kchat.adapter import KChatAdapter

    monkeypatch.setenv("KCHAT_TOKEN", "default-profile-token")
    monkeypatch.setenv("KCHAT_URL", "https://default.kchat.infomaniak.com")
    scope = set_secret_scope({
        "KCHAT_TOKEN": "pro-profile-token",
        "KCHAT_URL": "https://pro.kchat.infomaniak.com",
    })
    try:
        adapter = KChatAdapter(PlatformConfig(enabled=True))
    finally:
        reset_secret_scope(scope)

    assert adapter._token == "pro-profile-token"
    assert adapter._base_url == "https://pro.kchat.infomaniak.com"


def test_adapter_falls_back_to_process_env_without_scope(monkeypatch):
    from hermes_kchat.adapter import KChatAdapter

    monkeypatch.setenv("KCHAT_TOKEN", "env-token")
    monkeypatch.setenv("KCHAT_URL", "https://env.kchat.infomaniak.com")
    adapter = KChatAdapter(PlatformConfig(enabled=True))

    assert adapter._token == "env-token"
    assert adapter._base_url == "https://env.kchat.infomaniak.com"


def test_unscoped_read_when_multiplexed_uses_process_env(monkeypatch):
    """Hermes builds the default profile's adapter outside any scope."""
    from hermes_kchat.adapter import KChatAdapter

    monkeypatch.setenv("KCHAT_TOKEN", "default-profile-token")
    monkeypatch.setenv("KCHAT_URL", "https://default.kchat.infomaniak.com")
    set_multiplex_active(True)
    try:
        adapter = KChatAdapter(PlatformConfig(enabled=True))
    finally:
        set_multiplex_active(False)

    assert adapter._token == "default-profile-token"


def test_scoped_read_when_multiplexed_ignores_process_env(monkeypatch):
    """A secondary profile never sees the default profile's values."""
    from hermes_kchat.adapter import KChatAdapter

    monkeypatch.setenv("KCHAT_TOKEN", "default-profile-token")
    monkeypatch.setenv("KCHAT_URL", "https://default.kchat.infomaniak.com")
    set_multiplex_active(True)
    scope = set_secret_scope({"KCHAT_TOKEN": "pro-profile-token"})
    try:
        adapter = KChatAdapter(PlatformConfig(enabled=True))
    finally:
        reset_secret_scope(scope)
        set_multiplex_active(False)

    assert adapter._token == "pro-profile-token"
    assert adapter._base_url == "", "missing URL must not come from the default profile"
