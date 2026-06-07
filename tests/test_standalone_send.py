import pytest

pytest.importorskip("gateway.platforms.base")

from unittest.mock import AsyncMock, MagicMock, patch


class _PConfig:
    def __init__(self):
        self.token = "tok"
        self.extra = {"url": "https://org.kchat.infomaniak.com"}


@pytest.mark.asyncio
async def test_standalone_send_posts_message():
    from hermes_kchat.adapter import _standalone_send

    resp = AsyncMock()
    resp.status = 201
    resp.json = AsyncMock(return_value={"id": "post_abc"})
    resp.text = AsyncMock(return_value="")
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)

    session = AsyncMock()
    session.post = MagicMock(return_value=resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)

    with patch("hermes_kchat.adapter.aiohttp.ClientSession", return_value=session):
        out = await _standalone_send(_PConfig(), "chan_1", "hi there")
    assert out["success"] is True
    assert out["message_id"] == "post_abc"
    assert out["platform"] == "kchat"


def test_register_smoke():
    """register(ctx) calls ctx.register_platform with name='kchat'."""
    from hermes_kchat.adapter import register
    ctx = MagicMock()
    register(ctx)
    kwargs = ctx.register_platform.call_args.kwargs
    assert kwargs["name"] == "kchat"
    assert kwargs["label"] == "kChat"
    assert kwargs["cron_deliver_env_var"] == "KCHAT_HOME_CHANNEL"
    assert kwargs["standalone_sender_fn"] is not None
    assert kwargs["max_message_length"] == 4000


@pytest.mark.asyncio
async def test_standalone_send_resolves_thread_root():
    """thread_id is resolved to its root before posting (kChat rejects non-root)."""
    from hermes_kchat.adapter import _standalone_send

    def _resp(status, body):
        r = AsyncMock()
        r.status = status
        r.json = AsyncMock(return_value=body)
        r.text = AsyncMock(return_value="")
        r.__aenter__ = AsyncMock(return_value=r)
        r.__aexit__ = AsyncMock(return_value=False)
        return r

    get_resp = _resp(200, {"id": "T", "root_id": "ROOT"})  # thread_id is itself a reply
    post_resp = _resp(201, {"id": "newpost"})

    session = AsyncMock()
    session.get = MagicMock(return_value=get_resp)
    session.post = MagicMock(return_value=post_resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)

    with patch("hermes_kchat.adapter.aiohttp.ClientSession", return_value=session):
        out = await _standalone_send(_PConfig(), "chan_1", "hi", thread_id="T")

    assert out["success"] is True
    assert "/api/v4/posts/T" in session.get.call_args[0][0]
    assert session.post.call_args[1]["json"]["root_id"] == "ROOT"
