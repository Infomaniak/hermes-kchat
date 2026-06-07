"""Async Pusher client for Infomaniak kChat.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_APP_KEY = "kchat-key"
DEFAULT_CLIENT_ID = "Pysher"
DEFAULT_VERSION = "1.0.7"
DEFAULT_PROTOCOL = 6
DEFAULT_ACTIVITY_TIMEOUT = 120.0

ChannelEventHandler = Callable[[str, Dict[str, Any]], Awaitable[None]]


class PusherPermanentError(Exception):
    """Reconnecting cannot help: 401/403 from /broadcasting/auth (bad/expired
    token) or a pusher:error in the 4000-4099 range."""


class PusherClient:
    """Minimal async Pusher (v6) client for one kChat presence channel."""

    def __init__(
        self,
        session: Any,                       # aiohttp.ClientSession
        websocket_host: str,
        auth_url: str,
        token: str,
        channel: str,
        on_channel_event: Optional[ChannelEventHandler],
        *,
        app_key: str = DEFAULT_APP_KEY,
        client_id: str = DEFAULT_CLIENT_ID,
        version: str = DEFAULT_VERSION,
        protocol: int = DEFAULT_PROTOCOL,
        connect_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._session = session
        self._websocket_host = websocket_host
        self._auth_url = auth_url
        self._token = token
        self._channel = channel
        self._on_channel_event = on_channel_event
        self._app_key = app_key
        self._client_id = client_id
        self._version = version
        self._protocol = protocol
        # Extra kwargs for ws_connect (e.g. {"proxy": "http://…"} for HTTP
        # proxies; SOCKS proxies ride the session connector and pass {}).
        self._connect_kwargs = connect_kwargs or {}

    def build_url(self) -> str:
        host = self._websocket_host.split("://", 1)[-1].rstrip("/")
        return (
            f"wss://{host}:443/app/{self._app_key}"
            f"?client={self._client_id}&version={self._version}&protocol={self._protocol}"
        )

    async def _authorize(self, socket_id: str) -> Dict[str, str]:
        """POST the presence-channel auth request (form-urlencoded, Bearer).

        Returns {"auth": ..., "channel_data": ...}. Raises PusherPermanentError
        on 401/403 (bad/expired token), RuntimeError on other HTTP failures.
        """
        body = {"channel_name": self._channel, "socket_id": socket_id}
        headers = {"Authorization": f"Bearer {self._token}"}
        async with self._session.post(self._auth_url, data=body, headers=headers) as resp:
            if resp.status in (401, 403):
                text = await resp.text()
                raise PusherPermanentError(
                    f"kChat /broadcasting/auth rejected token (HTTP {resp.status}): {text[:200]}"
                )
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(
                    f"kChat /broadcasting/auth failed (HTTP {resp.status}): {text[:200]}"
                )
            data = await resp.json()
        return {"auth": data["auth"], "channel_data": data.get("channel_data", "{}")}

    async def _handle_frame(self, ws: Any, frame: Dict[str, Any]) -> Optional[float]:
        """Process one decoded Pusher frame.

        Returns the activity_timeout (float) after handling
        connection_established (so the caller can start the pinger), else None.
        Raises PusherPermanentError on unrecoverable errors; RuntimeError on
        transient pusher:error (caller reconnects).
        """
        event = frame.get("event")

        if event == "pusher:connection_established":
            data = frame.get("data")
            if isinstance(data, str):
                data = json.loads(data)
            socket_id = data["socket_id"]
            activity_timeout = float(data.get("activity_timeout", DEFAULT_ACTIVITY_TIMEOUT))
            auth = await self._authorize(socket_id)
            await ws.send_str(json.dumps({
                "event": "pusher:subscribe",
                "data": {
                    "channel": self._channel,
                    "auth": auth["auth"],
                    "channel_data": auth["channel_data"],
                },
            }))
            logger.info("kChat: subscribed to %s", self._channel)
            return activity_timeout

        if event == "pusher:ping":
            await ws.send_str(json.dumps({"event": "pusher:pong", "data": ""}))
            return None

        if event == "pusher:pong":
            return None

        if event == "pusher:error":
            data = frame.get("data")
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    data = {}
            try:
                code = int((data or {}).get("code", 0))
            except (TypeError, ValueError):
                code = 0
            message = (data or {}).get("message", "")
            if 4000 <= code <= 4099:
                raise PusherPermanentError(f"Pusher error {code}: {message}")
            raise RuntimeError(f"Pusher error {code}: {message}")

        if event == "pusher_internal:subscription_succeeded":
            logger.info("kChat: subscription confirmed for %s", self._channel)
            return None

        # Channel events (posted, status_change, ...): the data field is a JSON
        # string that decodes one level into the Mattermost-shaped event object.
        if event and "channel" in frame:
            logger.debug("kChat: channel event '%s' on %s", event, frame.get("channel"))
            data = frame.get("data")
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    logger.warning("kChat: could not decode data for event '%s'", event)
                    return None
            if self._on_channel_event is not None:
                await self._on_channel_event(event, data or {})
            return None

        logger.debug("kChat: unhandled ws event=%r channel=%r", event, frame.get("channel"))
        return None

    async def _pinger(self, ws: Any, activity_timeout: float) -> None:
        """Proactively send a client pusher:ping before activity_timeout."""
        interval = max(activity_timeout - 5.0, 10.0)
        try:
            while True:
                await asyncio.sleep(interval)
                await ws.send_str(json.dumps({"event": "pusher:ping", "data": ""}))
        except asyncio.CancelledError:
            return

    async def connect_and_listen(self) -> None:
        """Run ONE websocket session: connect, auth, subscribe, listen.

        Returns when the socket closes. Raises PusherPermanentError on
        unrecoverable conditions (caller must NOT reconnect); raises other
        exceptions on transient failures (caller may reconnect).
        """
        import aiohttp

        url = self.build_url()
        logger.info("kChat: connecting to Pusher %s", url)
        ping_task: Optional[asyncio.Task] = None
        ws = await self._session.ws_connect(url, heartbeat=None, **self._connect_kwargs)
        try:
            async for raw in ws:
                if raw.type == aiohttp.WSMsgType.TEXT:
                    # Verbatim frame dump — DEBUG so it's available when
                    # troubleshooting but silent in normal operation.
                    logger.debug("kChat: ws<< %s", raw.data[:1200])
                    try:
                        frame = json.loads(raw.data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("kChat: non-JSON ws frame ignored: %r", raw.data[:200])
                        continue
                    activity_timeout = await self._handle_frame(ws, frame)
                    if activity_timeout is not None and ping_task is None:
                        ping_task = asyncio.create_task(self._pinger(ws, activity_timeout))
                elif raw.type in (
                    aiohttp.WSMsgType.ERROR,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                ):
                    logger.info("kChat: Pusher socket closed (%s)", raw.type)
                    break
        finally:
            if ping_task and not ping_task.done():
                ping_task.cancel()
            await ws.close()
