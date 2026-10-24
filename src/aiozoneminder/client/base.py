"""The contract every ZoneMinder backend client implements.

Each client talks to one API (zm-api, ZoneMinder API v3, or the CakePHP API v2
built into ZoneMinder) and offers the same methods, so callers can switch API
without changing code.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, tzinfo
from typing import Any

import aiohttp

from ..const import DEFAULT_TIMEOUT, SERVER_ACTIONS, STREAM_TYPE_MJPEG
from ..exceptions import ZMNotSupportedError
from ..models import (
    Event,
    EventSummary,
    Monitor,
    MonitorStatus,
    RunState,
    ServerInfo,
)


class ZoneMinderClient(ABC):
    """Common interface for a ZoneMinder backend."""

    #: Short identifier for the backend (``"v3"`` or ``"legacy"``).
    backend: str = "base"

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        session: aiohttp.ClientSession,
        *,
        timeout: int = DEFAULT_TIMEOUT,
        verify_ssl: bool = True,
        timezone: tzinfo | None = None,
    ) -> None:
        """Initialize the client.

        ``session`` is the caller's aiohttp session; the client never closes
        it. ``timezone`` is the one ZoneMinder's API v2 times are in, which
        defaults to this machine's.
        """
        self._host = host.rstrip("/")
        self._username = username
        self._password = password
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._verify_ssl = verify_ssl
        self._timezone = timezone
        self._authenticated = False

    @property
    def host(self) -> str:
        """Return the server URL, without a trailing slash."""
        return self._host

    @property
    def authenticated(self) -> bool:
        """Return True once a session has been established."""
        return self._authenticated

    def _build_url(self, endpoint: str) -> str:
        """Join the host with a relative endpoint path."""
        return f"{self._host}/{endpoint.lstrip('/')}"

    # -- Lifecycle -----------------------------------------------------------

    @abstractmethod
    async def authenticate(self) -> bool:
        """Authenticate with the server."""

    @abstractmethod
    async def get_version(self) -> str:
        """Return the ZoneMinder version string."""

    @abstractmethod
    async def get_server_info(self) -> ServerInfo:
        """Return version, daemon health and storage usage."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources and drop credentials."""

    # -- Monitors ------------------------------------------------------------

    @abstractmethod
    async def get_monitors(self) -> list[Monitor]:
        """Return every monitor."""

    @abstractmethod
    async def get_monitor(self, monitor_id: int) -> Monitor:
        """Return a single monitor."""

    async def get_monitor_statuses(self) -> dict[int, MonitorStatus]:
        """Return live capture stats keyed by monitor id.

        Backends without a status endpoint return an empty mapping.
        """
        return {}

    @abstractmethod
    async def set_monitor_state(
        self,
        monitor_id: int,
        *,
        capturing: str | None = None,
        analysing: str | None = None,
        recording: str | None = None,
        decoding: str | None = None,
    ) -> None:
        """Update one or more of a monitor's state fields.

        Raises a ``ZMError`` subclass when the change is rejected, so callers
        can surface the failure instead of silently doing nothing.
        """

    # -- Events --------------------------------------------------------------

    @abstractmethod
    async def get_events(
        self,
        monitor_id: int | None = None,
        limit: int = 100,
    ) -> list[Event]:
        """Return events newest first, optionally filtered to one monitor."""

    @abstractmethod
    async def get_event(self, event_id: int) -> Event:
        """Return a single event."""

    async def get_events_between(
        self,
        monitor_id: int,
        start: datetime,
        end: datetime,
        limit: int = 500,
    ) -> list[Event]:
        """Return a monitor's events that started inside a time window.

        Backends without a range filter fall back to slicing the most recent
        events, which is correct but only reaches as far back as ``limit``.
        """
        events = await self.get_events(monitor_id=monitor_id, limit=limit)
        return [event for event in events if start <= event.start_time < end]

    async def get_event_summary(
        self, monitor_id: int, *, include_archived: bool = True
    ) -> EventSummary | None:
        """Return event counts and disk usage for a monitor, if supported.

        With ``include_archived=False`` the event counts leave archived events
        out, as ZoneMinder's console counts them.
        """
        return None

    # -- Alarm and control ---------------------------------------------------

    @abstractmethod
    async def trigger_alarm(self, monitor_id: int, cause: str) -> None:
        """Force a monitor into alarm state."""

    @abstractmethod
    async def cancel_alarm(self, monitor_id: int) -> None:
        """Clear a forced alarm."""

    @abstractmethod
    async def ptz(self, monitor_id: int, command: str, **kwargs: Any) -> None:
        """Issue a PTZ command from the vocabulary in ``PTZ_COMMANDS``."""

    async def get_ptz_presets(self, monitor_id: int) -> dict[int, str]:
        """Return ``{preset_number: label}`` for a controllable monitor."""
        return {}

    # -- Configuration -------------------------------------------------------

    async def get_configs(self) -> dict[str, str]:
        """Return ZoneMinder's configuration keys and values."""
        return {}

    # -- Live streaming ------------------------------------------------------

    async def async_prepare_live(self, monitor_id: int) -> None:
        """Make sure a live session exists before a stream URL is fetched.

        Backends that stream without an explicit session do nothing here.
        """
        return None

    async def async_get_rtsp_url(self, monitor_id: int) -> str | None:
        """Return a self-authenticating RTSP URL, if the backend offers one."""
        return None

    @abstractmethod
    async def get_stream_source(self, monitor_id: int) -> str | None:
        """Return a stream URL that works without extra request headers.

        Returns None on API v3, whose HLS needs a bearer token: callers proxy
        it instead, or use the signed RTSP link.
        """

    def get_stream_type(self, monitor_id: int) -> str:
        """Return the kind of live stream this backend offers."""
        return STREAM_TYPE_MJPEG

    def get_webrtc_signaling_url(self, monitor_id: int) -> str | None:
        """Return the WebSocket URL for native WebRTC signaling, if any."""
        return None

    # -- Media ---------------------------------------------------------------

    @abstractmethod
    async def async_get_snapshot(self, monitor_id: int) -> bytes | None:
        """Return one JPEG still frame."""

    def get_live_media_path(self, monitor_id: int) -> str | None:
        """Return the relative path of the live HLS master playlist."""
        return None

    async def get_run_states(self) -> list[RunState]:
        """Return the server's saved run states."""
        return []

    async def save_run_state(self, name: str, definition: str) -> None:
        """Create or overwrite a named run state."""
        raise ZMNotSupportedError("This API cannot save run states")

    async def set_run_state(self, state: RunState) -> None:
        """Switch the server to a run state."""
        raise ZMNotSupportedError("This API cannot switch run states")

    async def server_control(self, action: str) -> None:
        """Start, stop or restart ZoneMinder itself.

        Needs System edit rights. ``action`` is one of ``SERVER_ACTIONS``.
        """
        if action not in SERVER_ACTIONS:
            raise ValueError(
                f"Unknown server action {action!r}; use one of {SERVER_ACTIONS}"
            )
        await self._async_server_control(action)

    async def _async_server_control(self, action: str) -> None:
        """Send a validated start, stop or restart to the server."""
        raise ZMNotSupportedError(
            f"The {self.backend} backend can't control ZoneMinder"
        )

    def get_event_media_path(self, event_id: int) -> str | None:
        """Return the relative path of an event's video."""
        return None

    def get_event_thumbnail_path(self, event_id: int) -> str | None:
        """Return the relative path of an event's thumbnail image."""
        return None

    @asynccontextmanager
    async def async_stream_media(
        self, path: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Yield an authenticated response for a relative media path.

        For relaying media to players that can't authenticate to ZoneMinder
        themselves. The caller owns the response only inside the ``with``.
        """
        raise ZMNotSupportedError(
            f"The {self.backend} backend does not support media proxying"
        )
        yield  # pragma: no cover - unreachable, satisfies the generator contract

    # -- API v3 only -----------------------------------------------------------
    # Declared here so every client has the same methods. API v2 has neither
    # a pushed event stream nor WebRTC.

    async def async_event_stream(
        self, since: int | None = None
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield (message type, data) as the server pushes event changes.

        Raises ZMNotSupportedError on backends without a stream. Poll
        :meth:`get_events` instead.
        """
        raise ZMNotSupportedError(f"The {self.backend} backend has no event stream")
        yield  # pragma: no cover - unreachable, makes this an async generator

    async def async_supports_whep(self, monitor_id: int) -> bool:
        """Return True when the server offers WebRTC playback over WHEP."""
        return False

    async def async_whep_offer(
        self, monitor_id: int, offer_sdp: str
    ) -> tuple[str, str]:
        """Send an SDP offer; return (answer SDP, session URL)."""
        raise ZMNotSupportedError(f"The {self.backend} backend has no WebRTC")

    async def async_whep_candidate(
        self, location: str, sdp_mid: str | None, candidate: str
    ) -> None:
        """Send one ICE candidate to an open WHEP session."""
        raise ZMNotSupportedError(f"The {self.backend} backend has no WebRTC")

    async def async_whep_close(self, location: str) -> None:
        """Close a WHEP session."""
        raise ZMNotSupportedError(f"The {self.backend} backend has no WebRTC")
