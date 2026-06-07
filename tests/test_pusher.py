import pytest


def test_pusher_module_imports_without_hermes():
    """pusher.py must import with only aiohttp present (no gateway import)."""
    from hermes_kchat.pusher import PusherClient, PusherPermanentError
    assert PusherClient is not None
    assert issubclass(PusherPermanentError, Exception)


def test_build_url_mirrors_pysher_handshake():
    from hermes_kchat.pusher import PusherClient
    client = PusherClient(
        session=None,
        websocket_host="websocket.kchat.infomaniak.com",
        auth_url="https://org.kchat.infomaniak.com/broadcasting/auth",
        token="tok",
        channel="presence-teamUser.u1",
        on_channel_event=None,
    )
    assert client.build_url() == (
        "wss://websocket.kchat.infomaniak.com:443/app/kchat-key"
        "?client=Pysher&version=1.0.7&protocol=6"
    )


def test_build_url_strips_scheme_and_trailing_slash():
    from hermes_kchat.pusher import PusherClient
    client = PusherClient(
        session=None,
        websocket_host="wss://websocket.kchat.infomaniak.com/",
        auth_url="x",
        token="t",
        channel="c",
        on_channel_event=None,
    )
    assert client.build_url().startswith("wss://websocket.kchat.infomaniak.com:443/app/kchat-key")


import json
import pytest
from unittest.mock import AsyncMock, MagicMock


def _fake_response(status=200, json_body=None, text_body=""):
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_body or {})
    resp.text = AsyncMock(return_value=text_body)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _client_with_session(resp):
    from hermes_kchat.pusher import PusherClient
    session = MagicMock()
    session.post = MagicMock(return_value=resp)   # sync call returns async-cm
    return PusherClient(
        session=session,
        websocket_host="ws.example",
        auth_url="https://org.kchat.infomaniak.com/broadcasting/auth",
        token="tok123",
        channel="presence-teamUser.u1",
        on_channel_event=None,
    ), session


@pytest.mark.asyncio
async def test_authorize_posts_form_body_with_bearer():
    resp = _fake_response(200, {"auth": "kchat-key:sig", "channel_data": "{}"})
    client, session = _client_with_session(resp)
    out = await client._authorize("123.456")
    assert out == {"auth": "kchat-key:sig", "channel_data": "{}"}
    _, kwargs = session.post.call_args
    assert kwargs["data"] == {"channel_name": "presence-teamUser.u1", "socket_id": "123.456"}
    assert "json" not in kwargs
    assert kwargs["headers"]["Authorization"] == "Bearer tok123"


@pytest.mark.asyncio
async def test_authorize_defaults_channel_data_when_missing():
    resp = _fake_response(200, {"auth": "kchat-key:sig"})  # no channel_data
    client, _ = _client_with_session(resp)
    out = await client._authorize("s1")
    assert out["channel_data"] == "{}"


@pytest.mark.asyncio
async def test_authorize_401_raises_permanent():
    from hermes_kchat.pusher import PusherPermanentError
    resp = _fake_response(403, text_body="forbidden")
    client, _ = _client_with_session(resp)
    with pytest.raises(PusherPermanentError):
        await client._authorize("s1")


@pytest.mark.asyncio
async def test_established_frame_authorizes_and_subscribes():
    resp = _fake_response(200, {"auth": "kchat-key:sig", "channel_data": "{\"u\":1}"})
    client, _ = _client_with_session(resp)
    ws = MagicMock()
    ws.send_str = AsyncMock()
    established = {
        "event": "pusher:connection_established",
        "data": json.dumps({"socket_id": "99.1", "activity_timeout": 77}),
    }
    activity = await client._handle_frame(ws, established)
    assert activity == 77.0
    sent = json.loads(ws.send_str.call_args[0][0])
    assert sent["event"] == "pusher:subscribe"
    assert sent["data"]["channel"] == "presence-teamUser.u1"
    assert sent["data"]["auth"] == "kchat-key:sig"
    assert sent["data"]["channel_data"] == "{\"u\":1}"


@pytest.mark.asyncio
async def test_server_ping_is_ponged():
    client, _ = _client_with_session(_fake_response())
    ws = MagicMock(); ws.send_str = AsyncMock()
    out = await client._handle_frame(ws, {"event": "pusher:ping", "data": "{}"})
    assert out is None
    assert json.loads(ws.send_str.call_args[0][0]) == {"event": "pusher:pong", "data": ""}


@pytest.mark.asyncio
async def test_posted_frame_decodes_string_and_forwards_inner():
    captured = {}
    async def on_event(name, data):
        captured["name"] = name
        captured["data"] = data
    from hermes_kchat.pusher import PusherClient
    client = PusherClient(None, "h", "a", "t", "presence-teamUser.u1", on_event)
    ws = MagicMock(); ws.send_str = AsyncMock()
    inner = {"post": json.dumps({"id": "p1", "message": "hi"}), "channel_type": "O"}
    frame = {"event": "posted", "channel": "presence-teamUser.u1", "data": json.dumps(inner)}
    await client._handle_frame(ws, frame)
    assert captured["name"] == "posted"
    assert captured["data"] == inner


@pytest.mark.asyncio
async def test_pusher_error_4001_is_permanent():
    from hermes_kchat.pusher import PusherPermanentError
    client, _ = _client_with_session(_fake_response())
    ws = MagicMock(); ws.send_str = AsyncMock()
    with pytest.raises(PusherPermanentError):
        await client._handle_frame(ws, {"event": "pusher:error", "data": {"code": 4001}})


@pytest.mark.asyncio
async def test_pusher_error_4200_is_transient():
    client, _ = _client_with_session(_fake_response())
    ws = MagicMock(); ws.send_str = AsyncMock()
    with pytest.raises(RuntimeError):
        await client._handle_frame(ws, {"event": "pusher:error", "data": {"code": 4200}})


@pytest.mark.asyncio
async def test_subscription_succeeded_is_ignored():
    client, _ = _client_with_session(_fake_response())
    ws = MagicMock(); ws.send_str = AsyncMock()
    out = await client._handle_frame(
        ws, {"event": "pusher_internal:subscription_succeeded",
             "channel": "presence-teamUser.u1", "data": "{}"}
    )
    assert out is None


# --- connect_and_listen coverage -------------------------------------------
import aiohttp


class _FakeMsg:
    def __init__(self, msg_type, data=None):
        self.type = msg_type
        self.data = data


class _FakeWS:
    """Minimal async-iterable stand-in for an aiohttp websocket."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.send_str = AsyncMock()
        self.close = AsyncMock()

    def __aiter__(self):
        self._it = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


def _client_with_ws(ws, auth_body=None):
    from hermes_kchat.pusher import PusherClient
    session = MagicMock()
    session.ws_connect = AsyncMock(return_value=ws)
    session.post = MagicMock(
        return_value=_fake_response(200, auth_body or {"auth": "kchat-key:sig", "channel_data": "{}"})
    )
    captured = {}

    async def on_event(name, data):
        captured["name"] = name
        captured["data"] = data

    client = PusherClient(
        session=session,
        websocket_host="websocket.kchat.infomaniak.com",
        auth_url="https://org.kchat.infomaniak.com/broadcasting/auth",
        token="tok",
        channel="presence-teamUser.u1",
        on_channel_event=on_event,
    )
    return client, captured


@pytest.mark.asyncio
async def test_connect_and_listen_propagates_permanent_error_and_closes_ws():
    """A 4000-range error frame must raise out of the loop, ws still closed."""
    from hermes_kchat.pusher import PusherPermanentError
    ws = _FakeWS([
        _FakeMsg(aiohttp.WSMsgType.TEXT,
                 json.dumps({"event": "pusher:error", "data": {"code": 4001}})),
    ])
    client, _ = _client_with_ws(ws)
    with pytest.raises(PusherPermanentError):
        await client.connect_and_listen()
    ws.close.assert_awaited()


@pytest.mark.asyncio
async def test_connect_and_listen_subscribes_then_breaks_on_close():
    """Established frame subscribes; a CLOSE frame breaks the loop cleanly."""
    established = _FakeMsg(
        aiohttp.WSMsgType.TEXT,
        json.dumps({"event": "pusher:connection_established",
                    "data": json.dumps({"socket_id": "s1", "activity_timeout": 120})}),
    )
    ws = _FakeWS([established, _FakeMsg(aiohttp.WSMsgType.CLOSE)])
    client, _ = _client_with_ws(ws)
    await client.connect_and_listen()          # returns normally (no raise)
    ws.close.assert_awaited()
    # a subscribe frame was sent during the established handler
    sent_events = [json.loads(c.args[0])["event"] for c in ws.send_str.call_args_list]
    assert "pusher:subscribe" in sent_events


@pytest.mark.asyncio
async def test_connect_and_listen_forwards_posted_event():
    """A posted frame after subscribe reaches the on_channel_event callback."""
    established = _FakeMsg(
        aiohttp.WSMsgType.TEXT,
        json.dumps({"event": "pusher:connection_established",
                    "data": json.dumps({"socket_id": "s1", "activity_timeout": 120})}),
    )
    inner = {"post": json.dumps({"id": "p1", "message": "hi"}), "channel_type": "O"}
    posted = _FakeMsg(
        aiohttp.WSMsgType.TEXT,
        json.dumps({"event": "posted", "channel": "presence-teamUser.u1",
                    "data": json.dumps(inner)}),
    )
    ws = _FakeWS([established, posted, _FakeMsg(aiohttp.WSMsgType.CLOSE)])
    client, captured = _client_with_ws(ws)
    await client.connect_and_listen()
    assert captured["name"] == "posted"
    assert captured["data"] == inner
