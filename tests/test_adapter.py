"""Tests for the Infomaniak kChat platform adapter."""

import pytest

pytest.importorskip("gateway.platforms.base")  # needs hermes-agent host

from gateway.config import Platform, PlatformConfig


def _make_adapter(**extra):
    from hermes_kchat.adapter import KChatAdapter
    config = PlatformConfig(enabled=True, token="test-token",
                            extra={"url": "https://org.kchat.infomaniak.com", **extra})
    return KChatAdapter(config)


class TestKChatInit:
    def test_reads_url_token_and_default_ws_host(self):
        a = _make_adapter()
        assert a._base_url == "https://org.kchat.infomaniak.com"
        assert a._token == "test-token"
        assert a._websocket_url == "websocket.kchat.infomaniak.com"
        assert a.platform == Platform("kchat")

    def test_strips_trailing_slash_on_url(self):
        a = _make_adapter(url="https://org.kchat.infomaniak.com/")
        assert a._base_url == "https://org.kchat.infomaniak.com"

    def test_reply_mode_defaults_off(self):
        assert _make_adapter()._reply_mode == "off"

    def test_headers_have_bearer(self):
        a = _make_adapter()
        h = a._headers()
        assert h["Authorization"] == "Bearer test-token"
        assert h["Content-Type"] == "application/json"


class TestKChatRequirements:
    def test_check_requires_url_and_token(self, monkeypatch):
        from hermes_kchat.adapter import check_kchat_requirements
        monkeypatch.setenv("KCHAT_TOKEN", "t")
        monkeypatch.setenv("KCHAT_URL", "https://x")
        assert check_kchat_requirements() is True
        monkeypatch.delenv("KCHAT_URL")
        assert check_kchat_requirements() is False


import asyncio
from unittest.mock import AsyncMock, MagicMock, patch


class TestKChatWSLoop:
    def test_permanent_error_stops_reconnect(self):
        from hermes_kchat.adapter import KChatAdapter
        from hermes_kchat.pusher import PusherPermanentError
        a = KChatAdapter.__new__(KChatAdapter)
        a._closing = False
        calls = 0
        async def fake_connect():
            nonlocal calls
            calls += 1
            raise PusherPermanentError("bad token")
        a._ws_connect_and_listen = fake_connect
        asyncio.run(a._ws_loop())
        assert calls == 1

    def test_transient_error_retries(self):
        from hermes_kchat.adapter import KChatAdapter
        a = KChatAdapter.__new__(KChatAdapter)
        a._closing = False
        calls = 0
        async def fake_connect():
            nonlocal calls
            calls += 1
            if calls >= 2:
                a._closing = True
                return
            raise ConnectionError("reset")
        a._ws_connect_and_listen = fake_connect
        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await a._ws_loop()
        asyncio.run(run())
        assert calls >= 2


class TestKChatChannelEvent:
    @pytest.mark.asyncio
    async def test_posted_event_forwarded_to_handler(self):
        from hermes_kchat.adapter import KChatAdapter
        a = KChatAdapter.__new__(KChatAdapter)
        a._handle_ws_event = AsyncMock()
        inner = {"post": "{}", "channel_type": "O"}
        await a._on_channel_event("posted", inner)
        a._handle_ws_event.assert_awaited_once_with({"event": "posted", "data": inner})

    @pytest.mark.asyncio
    async def test_status_change_ignored(self):
        from hermes_kchat.adapter import KChatAdapter
        a = KChatAdapter.__new__(KChatAdapter)
        a._handle_ws_event = AsyncMock()
        await a._on_channel_event("status_change", {"status": "online"})
        a._handle_ws_event.assert_not_called()


import json


class TestKChatHandleEvent:
    def setup_method(self):
        self.a = _make_adapter()
        self.a._bot_user_id = "bot_id"
        self.a._bot_username = "hermes-bot"
        self.a.handle_message = AsyncMock()

    def _posted(self, post, channel_type="O", sender_name="@alice"):
        return {"event": "posted",
                "data": {"post": json.dumps(post), "channel_type": channel_type,
                         "sender_name": sender_name}}

    @pytest.mark.asyncio
    async def test_dm_message_dispatched(self):
        post = {"id": "p1", "user_id": "u2", "channel_id": "c_dm", "message": "hi"}
        await self.a._handle_ws_event(self._posted(post, channel_type="D"))
        self.a.handle_message.assert_called_once()
        ev = self.a.handle_message.call_args[0][0]
        assert ev.text == "hi"
        assert ev.message_id == "p1"
        assert ev.source.chat_type == "dm"
        assert ev.source.user_id == "u2"
        assert ev.source.platform == Platform("kchat")

    @pytest.mark.asyncio
    async def test_post_as_object_is_decoded(self):
        # Real kChat: the Pusher 'posted' payload delivers `post` as a nested
        # OBJECT, not a JSON string. json.loads-ing it threw TypeError and
        # silently dropped every inbound message — this is the regression guard.
        post = {"id": "p_obj", "user_id": "u2", "channel_id": "c_dm", "message": "hola"}
        event = {"event": "posted",
                 "data": {"post": post, "channel_type": "D", "sender_name": "@bob"}}
        await self.a._handle_ws_event(event)
        self.a.handle_message.assert_called_once()
        ev = self.a.handle_message.call_args[0][0]
        assert ev.text == "hola"
        assert ev.message_id == "p_obj"
        assert ev.source.chat_type == "dm"

    @pytest.mark.asyncio
    async def test_own_message_ignored(self):
        post = {"id": "p2", "user_id": "bot_id", "channel_id": "c", "message": "echo"}
        await self.a._handle_ws_event(self._posted(post))
        assert not self.a.handle_message.called

    @pytest.mark.asyncio
    async def test_system_post_ignored(self):
        post = {"id": "p3", "user_id": "u2", "channel_id": "c", "message": "x",
                "type": "system_join_channel"}
        await self.a._handle_ws_event(self._posted(post))
        assert not self.a.handle_message.called

    @pytest.mark.asyncio
    async def test_channel_requires_mention(self, monkeypatch):
        monkeypatch.setenv("KCHAT_REQUIRE_MENTION", "true")
        post = {"id": "p4", "user_id": "u2", "channel_id": "c_chan", "message": "no mention"}
        await self.a._handle_ws_event(self._posted(post, channel_type="O"))
        assert not self.a.handle_message.called

    @pytest.mark.asyncio
    async def test_channel_mention_stripped(self, monkeypatch):
        monkeypatch.setenv("KCHAT_REQUIRE_MENTION", "true")
        post = {"id": "p5", "user_id": "u2", "channel_id": "c_chan",
                "message": "@hermes-bot hello there"}
        await self.a._handle_ws_event(self._posted(post, channel_type="O"))
        self.a.handle_message.assert_called_once()
        assert self.a.handle_message.call_args[0][0].text == "hello there"

    @pytest.mark.asyncio
    async def test_duplicate_ignored(self):
        post = {"id": "dup", "user_id": "u2", "channel_id": "c_dm", "message": "hi"}
        await self.a._handle_ws_event(self._posted(post, channel_type="D"))
        await self.a._handle_ws_event(self._posted(post, channel_type="D"))
        assert self.a.handle_message.call_count == 1


class TestKChatSend:
    def setup_method(self):
        self.a = _make_adapter()

    def _mock_post(self, json_body, status=200):
        resp = AsyncMock()
        resp.status = status
        resp.json = AsyncMock(return_value=json_body)
        resp.text = AsyncMock(return_value="")
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        self.a._session = MagicMock()
        self.a._session.post = MagicMock(return_value=resp)
        return resp

    @pytest.mark.asyncio
    async def test_send_posts_message(self):
        self._mock_post({"id": "post123"})
        result = await self.a.send("chan_1", "Hello!")
        assert result.success is True
        assert result.message_id == "post123"
        call = self.a._session.post.call_args
        assert "/api/v4/posts" in call[0][0]
        assert call[1]["json"]["channel_id"] == "chan_1"
        assert call[1]["json"]["message"] == "Hello!"

    @pytest.mark.asyncio
    async def test_send_empty_is_noop_success(self):
        result = await self.a.send("chan_1", "")
        assert result.success is True

    @pytest.mark.asyncio
    async def test_get_chat_info_maps_type(self):
        resp = AsyncMock()
        resp.status = 200
        resp.json = AsyncMock(return_value={"type": "D", "display_name": "DM"})
        resp.text = AsyncMock(return_value="")
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        self.a._session = MagicMock()
        self.a._session.get = MagicMock(return_value=resp)
        info = await self.a.get_chat_info("chan_x")
        assert info["type"] == "dm"
        assert info["name"] == "DM"

    @pytest.mark.asyncio
    async def test_format_message_strips_image_markdown(self):
        out = self.a.format_message("see ![alt](http://x/y.png) now")
        assert out == "see http://x/y.png now"


class TestKChatFiles:
    def setup_method(self):
        self.a = _make_adapter()

    @pytest.mark.asyncio
    async def test_send_local_file_uploads_then_posts(self, tmp_path):
        f = tmp_path / "doc.txt"
        f.write_text("hello")
        self.a._upload_file = AsyncMock(return_value="file_99")
        self.a._api_post = AsyncMock(return_value={"id": "post_with_file"})
        result = await self.a.send_document("chan_1", str(f))
        assert result.success is True
        assert result.message_id == "post_with_file"
        payload = self.a._api_post.call_args[0][1]
        assert payload["file_ids"] == ["file_99"]
        assert payload["channel_id"] == "chan_1"

    @pytest.mark.asyncio
    async def test_send_local_file_missing_is_soft_success(self):
        result = await self.a.send_document("chan_1", "/no/such/file.txt")
        assert result.success is True
        assert result.message_id is None


class TestKChatThreading:
    """Reconciled threading: explicit metadata thread_id vs reply_to, + proxy."""

    @pytest.mark.asyncio
    async def test_explicit_metadata_thread_id_always_threads(self):
        # reply_mode defaults to "off" — an explicit thread_id still threads.
        a = _make_adapter()
        a._resolve_root_id = AsyncMock(return_value="ROOT")
        a._api_post = AsyncMock(return_value={"id": "p1"})
        await a.send("c", "hi", metadata={"thread_id": "T123"})
        a._resolve_root_id.assert_awaited_once_with("T123")
        assert a._api_post.call_args[0][1]["root_id"] == "ROOT"

    @pytest.mark.asyncio
    async def test_reply_to_is_flat_when_reply_mode_off(self):
        a = _make_adapter()  # reply_mode "off"
        a._resolve_root_id = AsyncMock(return_value="ROOT")
        a._api_post = AsyncMock(return_value={"id": "p1"})
        await a.send("c", "hi", reply_to="R")
        a._resolve_root_id.assert_not_awaited()
        assert "root_id" not in a._api_post.call_args[0][1]

    @pytest.mark.asyncio
    async def test_reply_to_threads_when_reply_mode_thread(self):
        a = _make_adapter(reply_mode="thread")
        a._resolve_root_id = AsyncMock(return_value="ROOT")
        a._api_post = AsyncMock(return_value={"id": "p1"})
        await a.send("c", "hi", reply_to="R")
        a._resolve_root_id.assert_awaited_once_with("R")
        assert a._api_post.call_args[0][1]["root_id"] == "ROOT"

    @pytest.mark.asyncio
    async def test_proxy_kwargs_forwarded_to_requests(self):
        a = _make_adapter()
        a._req_kw = {"proxy": "http://px:8080"}
        resp = AsyncMock()
        resp.status = 200
        resp.json = AsyncMock(return_value={"id": "p1"})
        resp.text = AsyncMock(return_value="")
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        a._session = MagicMock()
        a._session.post = MagicMock(return_value=resp)
        await a.send("c", "hi")
        assert a._session.post.call_args[1]["proxy"] == "http://px:8080"


class TestKChatVoice:
    """Voice messages: kChat tags them type='voice' with an async transcript."""

    def setup_method(self):
        self.a = _make_adapter()
        self.a._bot_user_id = "bot_id"
        self.a._bot_username = "hermes-bot"
        self.a.handle_message = AsyncMock()

    def _voice_event(self, channel_type="D"):
        post = {
            "id": "v1", "user_id": "u2", "channel_id": "c_dm", "type": "voice",
            "message": "",
            "metadata": {"files": [
                {"id": "f_audio", "mime_type": "audio/mpeg", "name": "voice.mp3"}
            ]},
        }
        return {"event": "posted",
                "data": {"post": post, "channel_type": channel_type, "sender_name": "@bob"}}

    @pytest.mark.asyncio
    async def test_voice_dispatched_with_transcript(self):
        self.a._fetch_voice_transcript = AsyncMock(return_value="hello from voice")
        await self.a._handle_ws_event(self._voice_event())
        # audio file id is pulled from metadata.files (file_ids was empty)
        self.a._fetch_voice_transcript.assert_awaited_once_with("f_audio")
        self.a.handle_message.assert_called_once()
        ev = self.a.handle_message.call_args[0][0]
        assert ev.text == "hello from voice"
        assert ev.message_type.value == "voice"
        assert ev.message_id == "v1"
        assert ev.source.chat_type == "dm"

    @pytest.mark.asyncio
    async def test_voice_without_transcript_still_dispatched(self):
        self.a._fetch_voice_transcript = AsyncMock(return_value="")
        await self.a._handle_ws_event(self._voice_event())
        self.a.handle_message.assert_called_once()
        ev = self.a.handle_message.call_args[0][0]
        assert ev.message_type.value == "voice"
        assert ev.text == ""

    @pytest.mark.asyncio
    async def test_system_post_still_ignored(self):
        post = {"id": "s1", "user_id": "u2", "channel_id": "c", "type": "system_join_channel"}
        event = {"event": "posted", "data": {"post": post, "channel_type": "O"}}
        await self.a._handle_ws_event(event)
        assert not self.a.handle_message.called


def test_extract_transcript_text():
    from hermes_kchat.adapter import _extract_transcript_text
    assert _extract_transcript_text({"text": " Test."}) == "Test."
    assert _extract_transcript_text({"transcript": {"text": " hi "}}) == "hi"
    assert _extract_transcript_text({"transcript": "yo"}) == "yo"
    assert _extract_transcript_text({"transcript": []}) == ""   # not-ready (empty list)
    assert _extract_transcript_text({}) == ""
    assert _extract_transcript_text("nope") == ""
