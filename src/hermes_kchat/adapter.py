"""Infomaniak kChat gateway adapter.

Environment variables:
    KCHAT_URL                REST base, e.g. https://org.kchat.infomaniak.com
    KCHAT_TOKEN              Bot token
    KCHAT_WEBSOCKET_URL      Pusher host (default websocket.kchat.infomaniak.com)
    KCHAT_ALLOWED_USERS      Comma-separated user IDs
    KCHAT_HOME_CHANNEL       Channel ID for cron/notification delivery
    KCHAT_REPLY_MODE         "thread" to nest replies, else "off"
    KCHAT_REQUIRE_MENTION / KCHAT_FREE_RESPONSE_CHANNELS / KCHAT_ALLOWED_CHANNELS
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

from .pusher import PusherClient, PusherPermanentError
import aiohttp  # module-level so tests can patch hermes_kchat.adapter.aiohttp

logger = logging.getLogger(__name__)

MAX_POST_LENGTH = 4000

_CHANNEL_TYPE_MAP = {
    "D": "dm",
    "G": "group",
    "P": "group",
    "O": "channel",
}

_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

_DEFAULT_WS_HOST = "websocket.kchat.infomaniak.com"


def _extract_transcript_text(data: Any) -> str:
    """Pull the transcript text out of a kChat transcript response.

    kChat's transcript object looks like
    ``{"task": "transcribe", "language": "…", "text": " …", "segments": [...]}``.
    Handle the object directly, a ``{"transcript": {...}}`` wrapper, and a bare
    ``{"transcript": "…"}``. An empty/not-ready transcript yields ``""``.
    """
    if isinstance(data, dict):
        candidate = data.get("transcript", data)
        if isinstance(candidate, dict) and isinstance(candidate.get("text"), str):
            return candidate["text"].strip()
        if isinstance(candidate, str):
            return candidate.strip()
        if isinstance(data.get("text"), str):
            return data["text"].strip()
    return ""


def check_kchat_requirements() -> bool:
    """Return True if the kChat adapter can be used."""
    token = os.getenv("KCHAT_TOKEN", "")
    url = os.getenv("KCHAT_URL", "")
    if not token:
        logger.debug("kChat: KCHAT_TOKEN not set")
        return False
    if not url:
        logger.warning("kChat: KCHAT_URL not set")
        return False
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("kChat: aiohttp not installed")
        return False


class KChatAdapter(BasePlatformAdapter):
    """Gateway adapter for Infomaniak kChat."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("kchat"))

        self._base_url: str = (
            config.extra.get("url", "") or os.getenv("KCHAT_URL", "")
        ).rstrip("/")
        self._token: str = config.token or os.getenv("KCHAT_TOKEN", "")
        self._websocket_url: str = (
            config.extra.get("websocket_url", "")
            or os.getenv("KCHAT_WEBSOCKET_URL", "")
            or _DEFAULT_WS_HOST
        )

        self._bot_user_id: str = ""
        self._bot_username: str = ""

        self._session: Any = None       # aiohttp.ClientSession
        self._req_kw: Dict[str, Any] = {}  # per-request proxy kwargs (set in connect)
        self._pusher: Optional[PusherClient] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._closing = False

        self._reply_mode: str = (
            config.extra.get("reply_mode", "") or os.getenv("KCHAT_REPLY_MODE", "off")
        ).lower()

        self._dedup = MessageDeduplicator()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _api_get(self, path: str) -> Dict[str, Any]:
        import aiohttp
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.get(
                url, headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=30), **self._req_kw
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("kChat API GET %s -> %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("kChat API GET %s network error: %s", path, exc)
            return {}

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        import aiohttp
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.post(
                url, headers=self._headers(), json=payload,
                timeout=aiohttp.ClientTimeout(total=30), **self._req_kw,
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("kChat API POST %s -> %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("kChat API POST %s network error: %s", path, exc)
            return {}

    async def _api_put(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        import aiohttp
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        try:
            async with self._session.put(
                url, headers=self._headers(), json=payload, **self._req_kw
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("kChat API PUT %s -> %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("kChat API PUT %s network error: %s", path, exc)
            return {}

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def _resolve_root_id(self, post_id: str) -> str:
        """Resolve a post_id to its thread root (kChat rejects non-root root_id)."""
        if not post_id:
            return post_id
        data = await self._api_get(f"posts/{post_id}")
        if data and data.get("root_id"):
            return data["root_id"]
        return post_id

    async def _thread_root(
        self, reply_to: Optional[str], metadata: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Return the ``root_id`` to thread under, or ``None``.

        An explicit ``metadata['thread_id']`` (e.g. from the send_message tool)
        always threads; ``reply_to`` threads only when reply mode is ``thread``.
        Both resolve to the thread root — kChat rejects a non-root ``root_id``.
        This keeps the live send path consistent with ``_standalone_send``.
        """
        target = (metadata or {}).get("thread_id")
        if not target and reply_to and self._reply_mode == "thread":
            target = reply_to
        if not target:
            return None
        return await self._resolve_root_id(target)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not content:
            return SendResult(success=True)

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, MAX_POST_LENGTH)

        root_id = await self._thread_root(reply_to, metadata)
        last_id = None
        for chunk in chunks:
            payload: Dict[str, Any] = {"channel_id": chat_id, "message": chunk}
            if root_id:
                payload["root_id"] = root_id
            data = await self._api_post("posts", payload)
            if not data or "id" not in data:
                return SendResult(success=False, error="Failed to create post")
            last_id = data["id"]
        return SendResult(success=True, message_id=last_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        data = await self._api_get(f"channels/{chat_id}")
        if not data:
            return {"name": chat_id, "type": "channel"}
        ch_type = _CHANNEL_TYPE_MAP.get(data.get("type", "O"), "channel")
        display_name = data.get("display_name") or data.get("name") or chat_id
        return {"name": display_name, "type": ch_type}

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        await self._api_post(f"users/{self._bot_user_id}/typing", {"channel_id": chat_id})

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        formatted = self.format_message(content)
        data = await self._api_put(f"posts/{message_id}/patch", {"message": formatted})
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to edit post")
        return SendResult(success=True, message_id=data["id"])

    def format_message(self, content: str) -> str:
        """kChat uses Mattermost markdown — strip image syntax to bare URLs."""
        return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        import aiohttp

        if not self._base_url or not self._token:
            logger.error("kChat: URL or token not configured")
            return False

        # Honour KCHAT_PROXY (falling back to HTTPS/ALL_PROXY + system proxy)
        # for the live REST + websocket traffic, matching _standalone_send.
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url(platform_env_var="KCHAT_PROXY")
        _sess_kw, self._req_kw = proxy_kwargs_for_aiohttp(_proxy)
        if _proxy:
            logger.info("kChat: routing live connection through a proxy")
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30), **_sess_kw
        )
        self._closing = False

        me = await self._api_get("users/me")
        if not me or "id" not in me:
            logger.error("kChat: failed to authenticate — check KCHAT_TOKEN and KCHAT_URL")
            await self._session.close()
            return False

        self._bot_user_id = me["id"]
        self._bot_username = me.get("username", "")
        logger.info(
            "kChat: authenticated as @%s (%s) on %s",
            self._bot_username, self._bot_user_id, self._base_url,
        )

        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        self._closing = True
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("kChat: disconnected")

    # ------------------------------------------------------------------
    # WebSocket (Pusher)
    # ------------------------------------------------------------------

    async def _ws_loop(self) -> None:
        """Maintain the Pusher connection, reconnecting on transient failures."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except PusherPermanentError as exc:
                logger.error("kChat WS permanent failure: %s — stopping reconnect", exc)
                return
            except Exception as exc:
                if self._closing:
                    return
                logger.warning("kChat WS error: %s — reconnecting in %.0fs", exc, delay)

            if self._closing:
                return

            import random
            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> None:
        """One Pusher session: build the client and run it to completion."""
        auth_url = f"{self._base_url}/broadcasting/auth"
        channel = f"presence-teamUser.{self._bot_user_id}"
        self._pusher = PusherClient(
            session=self._session,
            websocket_host=self._websocket_url,
            auth_url=auth_url,
            token=self._token,
            channel=channel,
            on_channel_event=self._on_channel_event,
            connect_kwargs=self._req_kw,
        )
        await self._pusher.connect_and_listen()

    async def _on_channel_event(self, event_name: str, data: Dict[str, Any]) -> None:
        """Bridge decoded Pusher channel events into the inbound handler."""
        logger.debug(
            "kChat: on_channel_event=%s data_keys=%s",
            event_name,
            list(data.keys()) if isinstance(data, dict) else type(data).__name__,
        )
        if event_name == "posted":
            await self._handle_ws_event({"event": "posted", "data": data})
        # status_change and other presence events are ignored.

    async def _handle_ws_event(self, event: Dict[str, Any]) -> None:
        """Process a single decoded `posted` event (kChat == Mattermost shape)."""
        event_type = event.get("event")
        if event_type != "posted":
            return

        data = event.get("data", {})
        raw_post = data.get("post")
        if not raw_post:
            logger.debug(
                "kChat: posted event has no 'post' field; data keys=%s",
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return

        try:
            post = raw_post if isinstance(raw_post, dict) else json.loads(raw_post)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(
                "kChat: failed to decode 'post' (%s); type=%s value=%s",
                exc, type(raw_post).__name__, str(raw_post)[:200],
            )
            return

        # Ignore own messages (the bot's own replies echo back on this channel).
        if post.get("user_id") == self._bot_user_id:
            logger.debug("kChat: ignoring own message (post=%s)", post.get("id"))
            return

        post_type = post.get("type", "")
        if post_type and post_type != "voice":
            logger.debug("kChat: ignoring post type=%s", post_type)
            return
        is_voice = post_type == "voice"

        post_id = post.get("id", "")
        if self._dedup.is_duplicate(post_id):
            logger.debug("kChat: ignoring duplicate post=%s", post_id)
            return

        channel_id = post.get("channel_id", "")
        channel_type_raw = data.get("channel_type", "O")
        chat_type = _CHANNEL_TYPE_MAP.get(channel_type_raw, "channel")

        message_text = post.get("message", "")

        meta_files = (post.get("metadata") or {}).get("files") or []
        file_ids = list(post.get("file_ids") or [])
        for _mf in meta_files:
            _fid = _mf.get("id")
            if _fid and _fid not in file_ids:
                file_ids.append(_fid)

        if is_voice and not message_text:
            voice_fid = next(
                (mf.get("id") for mf in meta_files
                 if str(mf.get("mime_type", "")).startswith("audio")),
                file_ids[0] if file_ids else None,
            )
            if voice_fid:
                message_text = await self._fetch_voice_transcript(voice_fid)

        logger.info(
            "kChat: inbound post id=%s channel_type=%s voice=%s text=%r",
            post_id, channel_type_raw, is_voice, message_text[:80],
        )

        # Mention-gating for non-DM channels (env-driven).
        if channel_type_raw != "D":
            allowed_raw = self.config.extra.get("allowed_channels") if self.config.extra else None
            if allowed_raw is None:
                allowed_raw = os.getenv("KCHAT_ALLOWED_CHANNELS", "")
            if isinstance(allowed_raw, list):
                allowed_channels = {str(c).strip() for c in allowed_raw if str(c).strip()}
            else:
                allowed_channels = {c.strip() for c in str(allowed_raw).split(",") if c.strip()}
            if allowed_channels and channel_id not in allowed_channels:
                logger.debug("kChat: ignoring message in non-allowed channel: %s", channel_id)
                return

            require_mention = os.getenv("KCHAT_REQUIRE_MENTION", "true").lower() not in {
                "false", "0", "no"
            }
            free_channels_raw = os.getenv("KCHAT_FREE_RESPONSE_CHANNELS", "")
            free_channels = {ch.strip() for ch in free_channels_raw.split(",") if ch.strip()}
            is_free_channel = channel_id in free_channels

            mention_patterns = [f"@{self._bot_username}", f"@{self._bot_user_id}"]
            has_mention = any(p.lower() in message_text.lower() for p in mention_patterns)

            if require_mention and not is_free_channel and not has_mention:
                logger.debug(
                    "kChat: skipping non-DM message without @mention (channel=%s)",
                    channel_id,
                )
                return

            if has_mention:
                for pattern in mention_patterns:
                    message_text = re.sub(
                        re.escape(pattern), "", message_text, flags=re.IGNORECASE
                    ).strip()

        sender_id = post.get("user_id", "")
        sender_name = data.get("sender_name", "").lstrip("@") or sender_id
        thread_id = post.get("root_id") or None

        msg_type = MessageType.TEXT
        if message_text.startswith("/"):
            msg_type = MessageType.COMMAND

        media_urls: List[str] = []
        media_types: List[str] = []
        for fid in file_ids:
            try:
                file_info = await self._api_get(f"files/{fid}/info")
                fname = file_info.get("name", f"file_{fid}")
                ext = Path(fname).suffix or ""
                mime = file_info.get("mime_type", "application/octet-stream")

                import aiohttp
                dl_url = f"{self._base_url}/api/v4/files/{fid}"
                async with self._session.get(
                    dl_url,
                    headers={"Authorization": f"Bearer {self._token}"},
                    timeout=aiohttp.ClientTimeout(total=30), **self._req_kw,
                ) as resp:
                    if resp.status < 400:
                        file_data = await resp.read()
                        from gateway.platforms.base import (
                            cache_image_from_bytes, cache_document_from_bytes,
                        )
                        if mime.startswith("image/"):
                            local_path = cache_image_from_bytes(file_data, ext or ".png")
                            media_urls.append(local_path)
                            media_types.append(mime)
                        elif mime.startswith("audio/"):
                            from gateway.platforms.base import cache_audio_from_bytes
                            local_path = cache_audio_from_bytes(file_data, ext or ".ogg")
                            media_urls.append(local_path)
                            media_types.append(mime)
                        else:
                            local_path = cache_document_from_bytes(file_data, fname)
                            media_urls.append(local_path)
                            media_types.append(mime)
                    else:
                        logger.warning("kChat: failed to download file %s: HTTP %s", fid, resp.status)
            except Exception as exc:
                logger.warning("kChat: error downloading file %s: %s", fid, exc)

        if media_types and msg_type == MessageType.TEXT:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            elif media_types:
                msg_type = MessageType.DOCUMENT
        if is_voice:
            msg_type = MessageType.VOICE

        source = self.build_source(
            chat_id=channel_id,
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            thread_id=thread_id,
        )

        from gateway.platforms.base import resolve_channel_prompt
        _channel_prompt = resolve_channel_prompt(self.config.extra, channel_id, None)

        msg_event = MessageEvent(
            text=message_text,
            message_type=msg_type,
            source=source,
            raw_message=post,
            message_id=post_id,
            media_urls=media_urls if media_urls else None,
            media_types=media_types if media_types else None,
            channel_prompt=_channel_prompt,
        )

        logger.debug(
            "kChat: dispatching to agent (chat_id=%s chat_type=%s user=%s type=%s)",
            channel_id, chat_type, sender_id, msg_type,
        )
        await self.handle_message(msg_event)

    async def _fetch_voice_transcript(
        self, file_id: str, attempts: int = 4, delay: float = 1.2
    ) -> str:
        """Fetch a kChat voice transcript via POST /files/{id}/transcript.

        kChat transcribes voice messages asynchronously, so the transcript can
        be empty for a second or two after the voice post arrives — retry
        briefly. Returns the transcript text, or "" if unavailable (the audio
        is still delivered, so the agent can transcribe it as a fallback).
        """
        import aiohttp
        url = f"{self._base_url}/api/v4/files/{file_id}/transcript"
        for attempt in range(attempts):
            try:
                async with self._session.post(
                    url, headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=30), **self._req_kw,
                ) as resp:
                    if resp.status < 400:
                        text = _extract_transcript_text(await resp.json())
                        if text:
                            logger.info("kChat: voice transcript (%d chars)", len(text))
                            return text
                    else:
                        logger.debug("kChat: transcript HTTP %s for file %s", resp.status, file_id)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.debug("kChat: transcript fetch error for %s: %s", file_id, exc)
            if attempt < attempts - 1:
                await asyncio.sleep(delay)
        logger.info("kChat: no transcript available for voice file %s", file_id)
        return ""

    async def _upload_file(
        self, channel_id: str, file_data: bytes, filename: str,
        content_type: str = "application/octet-stream",
    ) -> Optional[str]:
        import aiohttp
        url = f"{self._base_url}/api/v4/files"
        form = aiohttp.FormData()
        form.add_field("channel_id", channel_id)
        form.add_field("files", file_data, filename=filename, content_type=content_type)
        headers = {"Authorization": f"Bearer {self._token}"}
        async with self._session.post(
            url, headers=headers, data=form,
            timeout=aiohttp.ClientTimeout(total=60), **self._req_kw
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                logger.error("kChat file upload -> %s: %s", resp.status, body[:200])
                return None
            data = await resp.json()
            infos = data.get("file_infos", [])
            return infos[0]["id"] if infos else None

    # ------------------------------------------------------------------
    # File senders
    # ------------------------------------------------------------------

    async def send_image(
        self, chat_id: str, image_url: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_url_as_file(
            chat_id, image_url, caption, reply_to, "image", metadata=metadata
        )

    async def send_image_file(
        self, chat_id: str, image_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(
            chat_id, image_path, caption, reply_to, metadata=metadata
        )

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None,
        file_name: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(
            chat_id, file_path, caption, reply_to, file_name, metadata=metadata
        )

    async def send_voice(
        self, chat_id: str, audio_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(
            chat_id, audio_path, caption, reply_to, metadata=metadata
        )

    async def send_video(
        self, chat_id: str, video_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(
            chat_id, video_path, caption, reply_to, metadata=metadata
        )

    async def _send_url_as_file(
        self, chat_id: str, url: str, caption: Optional[str],
        reply_to: Optional[str], kind: str = "file",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("kChat: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata)

        import aiohttp

        file_data = None
        ct = "application/octet-stream"
        fname = url.rsplit("/", 1)[-1].split("?")[0] or f"{kind}.png"

        for attempt in range(3):
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30), **self._req_kw
                ) as resp:
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                    if resp.status >= 400:
                        return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata)
                    file_data = await resp.read()
                    ct = resp.content_type or "application/octet-stream"
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                logger.warning("kChat: failed to download %s after %d attempts: %s", url, attempt + 1, exc)
                return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata)

        if file_data is None:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata)

        file_id = await self._upload_file(chat_id, file_data, fname, ct)
        if not file_id:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata)

        payload: Dict[str, Any] = {"channel_id": chat_id, "message": caption or "", "file_ids": [file_id]}
        root_id = await self._thread_root(reply_to, metadata)
        if root_id:
            payload["root_id"] = root_id
        data = await self._api_post("posts", payload)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post with file")
        return SendResult(success=True, message_id=data["id"])

    async def _send_local_file(
        self, chat_id: str, file_path: str, caption: Optional[str],
        reply_to: Optional[str], file_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        import mimetypes

        p = Path(file_path)
        if not p.exists():
            logger.warning("kChat: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)

        fname = file_name or p.name
        ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        file_data = p.read_bytes()

        file_id = await self._upload_file(chat_id, file_data, fname, ct)
        if not file_id:
            return SendResult(success=False, error="File upload failed")

        payload: Dict[str, Any] = {"channel_id": chat_id, "message": caption or "", "file_ids": [file_id]}
        root_id = await self._thread_root(reply_to, metadata)
        if root_id:
            payload["root_id"] = root_id
        data = await self._api_post("posts", payload)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post with file")
        return SendResult(success=True, message_id=data["id"])

    async def send_multiple_images(
        self, chat_id: str, images: List[Tuple[str, str]],
        metadata: Optional[Dict[str, Any]] = None, human_delay: float = 0.0,
    ) -> None:
        """Batch up to 5 images per post (kChat/Mattermost file_ids cap)."""
        if not images:
            return

        import mimetypes
        import aiohttp
        from urllib.parse import unquote as _unquote

        CHUNK = 5
        chunks = [images[i:i + CHUNK] for i in range(0, len(images), CHUNK)]

        for chunk_idx, chunk in enumerate(chunks):
            if human_delay > 0 and chunk_idx > 0:
                await asyncio.sleep(human_delay)

            file_ids: List[str] = []
            caption_parts: List[str] = []
            try:
                for image_url, alt_text in chunk:
                    if alt_text:
                        caption_parts.append(alt_text)

                    if image_url.startswith("file://"):
                        local_path = _unquote(image_url[7:])
                        p = Path(local_path)
                        if not p.exists():
                            logger.warning("kChat: skipping missing image %s", local_path)
                            continue
                        fname = p.name
                        ct = mimetypes.guess_type(fname)[0] or "image/png"
                        file_data = p.read_bytes()
                    else:
                        from tools.url_safety import is_safe_url
                        if not is_safe_url(image_url):
                            logger.warning("kChat: blocked unsafe image URL in batch")
                            continue
                        try:
                            async with self._session.get(
                                image_url, timeout=aiohttp.ClientTimeout(total=30), **self._req_kw
                            ) as resp:
                                if resp.status >= 400:
                                    logger.warning("kChat: failed to download image (HTTP %d): %s",
                                                   resp.status, image_url[:80])
                                    continue
                                file_data = await resp.read()
                                ct = resp.content_type or "image/png"
                        except Exception as dl_err:
                            logger.warning("kChat: download failed for %s: %s", image_url[:80], dl_err)
                            continue
                        fname = image_url.rsplit("/", 1)[-1].split("?")[0] or f"image_{len(file_ids)}.png"

                    fid = await self._upload_file(chat_id, file_data, fname, ct)
                    if fid:
                        file_ids.append(fid)

                if not file_ids:
                    continue

                payload: Dict[str, Any] = {
                    "channel_id": chat_id, "message": "\n".join(caption_parts), "file_ids": file_ids,
                }
                logger.info("kChat: sending %d image(s) as single post (chunk %d/%d)",
                            len(file_ids), chunk_idx + 1, len(chunks))
                data = await self._api_post("posts", payload)
                if not data or "id" not in data:
                    logger.warning("kChat: multi-image post failed, falling back")
                    await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)
            except Exception as e:
                logger.warning("kChat: multi-image send failed (chunk %d/%d), falling back: %s",
                               chunk_idx + 1, len(chunks), e, exc_info=True)
                await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)


# ---------------------------------------------------------------------------
# Plugin standalone-send (out-of-process cron delivery via kChat REST)
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    base_url = (
        (getattr(pconfig, "extra", {}) or {}).get("url") or os.getenv("KCHAT_URL", "")
    ).rstrip("/")
    token = (getattr(pconfig, "token", None) or os.getenv("KCHAT_TOKEN", "")).strip()
    if not base_url or not token:
        return {"error": "kChat standalone send: KCHAT_URL and KCHAT_TOKEN must both be set"}

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    upload_headers = {"Authorization": f"Bearer {token}"}
    media_files = media_files or []

    try:
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _proxy = resolve_proxy_url(platform_env_var="KCHAT_PROXY")
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60), **_sess_kw
        ) as session:
            file_ids: List[str] = []
            for media in media_files:
                file_path = media.get("path") if isinstance(media, dict) else media
                if not file_path or not os.path.exists(file_path):
                    continue
                form = aiohttp.FormData()
                form.add_field("channel_id", chat_id)
                with open(file_path, "rb") as fh:
                    form.add_field("files", fh.read(), filename=os.path.basename(file_path))
                async with session.post(
                    f"{base_url}/api/v4/files", data=form, headers=upload_headers, **_req_kw
                ) as upload_resp:
                    if upload_resp.status not in {200, 201}:
                        body = await upload_resp.text()
                        return {"error": f"kChat file upload failed ({upload_resp.status}): {body[:400]}"}
                    upload_data = await upload_resp.json()
                    for info in upload_data.get("file_infos", []):
                        if info.get("id"):
                            file_ids.append(info["id"])

            payload: Dict[str, Any] = {"channel_id": chat_id, "message": message}
            if thread_id:
                root_id = thread_id
                try:
                    async with session.get(
                        f"{base_url}/api/v4/posts/{thread_id}", headers=headers, **_req_kw
                    ) as pr:
                        if pr.status < 400:
                            pdata = await pr.json()
                            root_id = pdata.get("root_id") or thread_id
                except aiohttp.ClientError:
                    pass
                payload["root_id"] = root_id
            if file_ids:
                payload["file_ids"] = file_ids
            async with session.post(
                f"{base_url}/api/v4/posts", headers=headers, json=payload, **_req_kw
            ) as resp:
                if resp.status not in {200, 201}:
                    body = await resp.text()
                    return {"error": f"kChat API error ({resp.status}): {body[:400]}"}
                data = await resp.json()
            return {"success": True, "platform": "kchat", "chat_id": chat_id,
                    "message_id": data.get("id")}
    except aiohttp.ClientError as exc:
        return {"error": f"kChat send failed (network): {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"kChat send failed: {exc}"}


# ---------------------------------------------------------------------------
# Interactive setup wizard
# ---------------------------------------------------------------------------


def interactive_setup() -> None:
    """Guide the user through kChat bot setup."""
    from hermes_cli.config import get_env_value, save_env_value
    from hermes_cli.cli_output import (
        prompt, prompt_yes_no, print_header, print_info, print_success,
    )

    print_header("kChat")
    existing = get_env_value("KCHAT_TOKEN")
    if existing:
        print_info("kChat: already configured")
        if not prompt_yes_no("Reconfigure kChat?", False):
            return

    print_info("Works with Infomaniak kChat (kSuite).")
    print_info("   1. In kChat: open the Integrations page -> add a Bot")
    print_info("   2. Copy the bot token")
    print()
    url = prompt("kChat server URL (e.g. https://org.kchat.infomaniak.com)")
    if url:
        save_env_value("KCHAT_URL", url.rstrip("/"))
    token = prompt("Bot token", password=True)
    if not token:
        return
    save_env_value("KCHAT_TOKEN", token)
    print_success("kChat token saved")

    print()
    print_info("Security: restrict who can use your bot.")
    allowed_users = prompt("Allowed user IDs (comma-separated, empty for open access)")
    if allowed_users:
        save_env_value("KCHAT_ALLOWED_USERS", allowed_users.replace(" ", ""))
        print_success("kChat allowlist configured")
    else:
        print_info("No allowlist set - anyone who can message the bot can use it.")

    print()
    print_info("Home channel: where Hermes delivers cron results and notifications.")
    home_channel = prompt("Home channel ID (leave empty to set later)")
    if home_channel:
        save_env_value("KCHAT_HOME_CHANNEL", home_channel)


# ---------------------------------------------------------------------------
# YAML -> env config bridge
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, kchat_cfg: dict) -> "dict | None":
    """Translate config.yaml `kchat:` keys into KCHAT_* env vars (env wins)."""
    if "require_mention" in kchat_cfg and not os.getenv("KCHAT_REQUIRE_MENTION"):
        os.environ["KCHAT_REQUIRE_MENTION"] = str(kchat_cfg["require_mention"]).lower()
    frc = kchat_cfg.get("free_response_channels")
    if frc is not None and not os.getenv("KCHAT_FREE_RESPONSE_CHANNELS"):
        if isinstance(frc, list):
            frc = ",".join(str(v) for v in frc)
        os.environ["KCHAT_FREE_RESPONSE_CHANNELS"] = str(frc)
    ac = kchat_cfg.get("allowed_channels")
    if ac is not None and not os.getenv("KCHAT_ALLOWED_CHANNELS"):
        if isinstance(ac, list):
            ac = ",".join(str(v) for v in ac)
        os.environ["KCHAT_ALLOWED_CHANNELS"] = str(ac)
    return None


# ---------------------------------------------------------------------------
# is_connected probe + registration
# ---------------------------------------------------------------------------


def _is_connected(config) -> bool:
    """kChat is connected when both KCHAT_TOKEN and KCHAT_URL are set."""
    import hermes_cli.gateway as gateway_mod
    return bool(
        (gateway_mod.get_env_value("KCHAT_TOKEN") or "").strip()
        and (gateway_mod.get_env_value("KCHAT_URL") or "").strip()
    )


def _build_adapter(config):
    return KChatAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="kchat",
        label="kChat",
        adapter_factory=_build_adapter,
        check_fn=check_kchat_requirements,
        is_connected=_is_connected,
        required_env=["KCHAT_URL", "KCHAT_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="KCHAT_ALLOWED_USERS",
        allow_all_env="KCHAT_ALLOW_ALL_USERS",
        cron_deliver_env_var="KCHAT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_POST_LENGTH,
        platform_hint=(
            "You are on Infomaniak kChat. "
            "Standard Markdown renders. Keep posts concise; long messages are chunked."
        ),
        emoji="💬",
        allow_update_command=True,
    )
