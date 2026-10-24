"""Client for the standalone zm-api v3 REST server.

Talks to ``/api/v3/...`` with JWT bearer authentication.
See https://github.com/SteveGilvarry/zm-api.
"""

from __future__ import annotations

import asyncio
import calendar
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import logging
import re
from typing import Any

import aiohttp

from ..const import (
    ALARM_STATES,
    BACKEND_V3,
    DEFAULT_TIMEOUT,
    LONG_TIMEOUT,
    PTZ_DEFAULT_SPEED,
    STREAM_TYPE_HLS,
    V3_CONFIGS,
    V3_CONTROL_PRESETS,
    V3_EVENT,
    V3_EVENT_STREAM,
    V3_EVENT_SUMMARY,
    V3_EVENT_THUMBNAIL,
    V3_EVENT_VIDEO,
    V3_EVENTS,
    V3_LIVE_HLS_MASTER,
    V3_LIVE_HLS_PLAYLIST,
    V3_LIVE_RTSP_LINK,
    V3_LIVE_START,
    V3_LIVE_STATS,
    V3_LIVE_STOP,
    V3_LIVE_WEBRTC_WS,
    V3_LIVE_WHEP,
    V3_LOGIN,
    V3_MONITOR,
    V3_MONITOR_ALARM,
    V3_MONITOR_SNAPSHOT,
    V3_MONITOR_STATUS,
    V3_MONITOR_STATUS_ONE,
    V3_MONITORS,
    V3_PTZ_FOCUS,
    V3_PTZ_HOME,
    V3_PTZ_MOVE,
    V3_PTZ_PRESET_GOTO,
    V3_PTZ_ZOOM,
    V3_REFRESH,
    V3_SERVER_CONTROL,
    V3_STATE,
    V3_STATES,
    V3_STORAGE,
    V3_SYSTEM_STATUS,
    V3_VERSION,
)
from ..exceptions import (
    ZMApiError,
    ZMAuthenticationError,
    ZMConnectionError,
    ZMError,
    ZMNotFoundError,
    ZMNotSupportedError,
)
from ..models import (
    Event,
    EventSummary,
    Monitor,
    MonitorStatus,
    RunState,
    ServerInfo,
    StorageArea,
)
from .base import ZoneMinderClient

_LOGGER = logging.getLogger(__name__)

#: Page size used when enumerating collections. The server caps this at 1000.
_PAGE_SIZE = 500
#: Refresh the access token this long before it actually expires.
_TOKEN_SKEW = timedelta(seconds=60)
#: zm-api revokes tokens issued before the second after a logout, so a token
#: renewed within that second is refused too. Wait this long, then renew again.
_REVOKED_RETRY_DELAY = 1.0
#: A live session is re-armed if we have not touched it for this long.
_LIVE_SESSION_TTL = timedelta(minutes=2)
#: zm-api reaps a session nobody has read for 90 s. When a live playlist is
#: gone, the session is started again, but no more often than this.
_LIVE_RESTART_INTERVAL = timedelta(seconds=30)
_LIVE_PLAYLIST = re.compile(r"api/v3/live/(\d+)/hls/[^/?]+\.m3u8(?:\?|$)")
#: How long to wait for a new live session to publish its first segment.
_FIRST_SEGMENT_TIMEOUT = 10.0
_FIRST_SEGMENT_POLL = 0.5
#: Lifetime requested for RTSP links, and how early to replace one.
_RTSP_LINK_TTL = timedelta(days=7)
_RTSP_LINK_RENEW = timedelta(days=1)
#: After the server says RTSP is unavailable, wait this long before asking again.
_RTSP_RETRY = timedelta(minutes=10)
#: zm-api refuses an alarm cause longer than this many bytes.
_ALARM_CAUSE_MAX = 31

#: What zm-api's WHEP status codes mean, in words the user can act on.
_WHEP_ERRORS = {
    400: "ZoneMinder could not use the browser's WebRTC offer",
    401: "ZoneMinder rejected the login",
    404: "This camera has no live stream right now",
    406: "This browser can't play the camera's video codec over WebRTC (H.265?)",
    503: "ZoneMinder's WebRTC is switched off or at its session limit",
}

_PTZ_MOVE_DIRECTIONS = {
    "up": "up",
    "down": "down",
    "left": "left",
    "right": "right",
    "up-left": "up-left",
    "up-right": "up-right",
    "down-left": "down-left",
    "down-right": "down-right",
    "stop": "stop",
}
_PTZ_ZOOM_DIRECTIONS = {"zoom-in": "in", "zoom-out": "out", "zoom-stop": "stop"}
_PTZ_FOCUS_DIRECTIONS = {
    "focus-near": "near",
    "focus-far": "far",
    "focus-auto": "auto",
    "focus-stop": "stop",
}


class ZoneMinderV3Client(ZoneMinderClient):
    """Client for the zm-api v3 REST server."""

    backend = BACKEND_V3

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the client."""
        super().__init__(*args, **kwargs)
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._token_expires: datetime | None = None
        self._live_sessions: dict[int, datetime] = {}
        self._live_restarts: dict[int, datetime] = {}
        #: Serialises token renewals: concurrent 401s share one (zm-api's
        #: /auth limiter refuses a burst of logins).
        self._renew_lock = asyncio.Lock()
        #: Renewals attempted so far, and the error of the last one if it failed.
        self._renewals = 0
        self._renew_error: ZMError | None = None
        self._rtsp_links: dict[int, tuple[str, datetime]] = {}
        self._rtsp_unavailable_until: datetime | None = None
        #: Monitors zm-api refused a link for (no access, or no such monitor).
        self._rtsp_refused_until: dict[int, datetime] = {}
        self._whep_supported: bool | None = None

    # -- Authentication ------------------------------------------------------

    @property
    def access_token(self) -> str | None:
        """Return the current access token."""
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        """Return the Authorization header for the current token."""
        if self._access_token:
            return {"Authorization": f"Bearer {self._access_token}"}
        return {}

    def _store_tokens(self, result: dict[str, Any]) -> None:
        """Record the tokens and expiry returned by login or refresh."""
        self._access_token = result.get("access_token")
        self._refresh_token = result.get("refresh_token") or self._refresh_token
        expires_in = result.get("expire_in")
        if isinstance(expires_in, (int, float)) and expires_in > 0:
            self._token_expires = datetime.now(UTC) + timedelta(seconds=expires_in)
        else:
            self._token_expires = None

    async def authenticate(self) -> bool:
        """Log in and store the JWT pair."""
        try:
            async with self._session.post(
                self._build_url(V3_LOGIN),
                json={"username": self._username, "password": self._password},
                timeout=self._timeout,
                ssl=self._verify_ssl,
            ) as response:
                if response.status in (400, 401, 403):
                    raise ZMAuthenticationError("Invalid username or password")
                if response.status == 404:
                    raise ZMConnectionError(
                        f"No ZoneMinder API v3 at {self._host}. Check the URL and "
                        "port, or use API v2 instead."
                    )
                response.raise_for_status()
                result = await response.json()
        except (aiohttp.ClientError, TimeoutError) as err:
            raise ZMConnectionError(f"Connection failed: {err}") from err

        self._store_tokens(result)
        if not self._access_token:
            raise ZMAuthenticationError("Login did not return an access token")
        self._authenticated = True
        _LOGGER.debug("Authenticated with zm-api v3 at %s", self._host)
        return True

    async def _refresh(self) -> bool:
        """Exchange the refresh token for a new access token."""
        if not self._refresh_token:
            return False
        try:
            async with self._session.post(
                self._build_url(V3_REFRESH),
                json={"token": self._refresh_token},
                timeout=self._timeout,
                ssl=self._verify_ssl,
            ) as response:
                if response.status != 200:
                    return False
                result = await response.json()
        except (aiohttp.ClientError, TimeoutError):
            return False
        self._store_tokens(result)
        return bool(self._access_token)

    async def _async_ensure_token(self) -> None:
        """Make sure a usable access token is available."""
        since = self._renewals
        async with self._renew_lock:
            if not self._authenticated or not self._access_token:
                await self._async_renew(since, refresh=False)
            elif (
                self._token_expires
                and datetime.now(UTC) >= self._token_expires - _TOKEN_SKEW
            ):
                _LOGGER.debug("Access token near expiry, refreshing")
                await self._async_renew(since)

    async def _renew(self, refused: str | None, since: int) -> None:
        """Renew the access token after ``refused`` was turned down.

        ``since`` is the renewal count when the refused request went out.
        Requests refused at the same time share one renewal: the first to take
        the lock renews, the others find a newer token and just send again.
        """
        async with self._renew_lock:
            if self._access_token != refused:
                return
            await self._async_renew(since)

    async def _async_renew(self, since: int, refresh: bool = True) -> None:
        """Refresh the token, or log in, with the renewal lock held.

        If a renewal was attempted after ``since`` and failed (zm-api's /auth
        limiter answering 429, or a changed password), its error is raised
        again instead of trying once more for every request that waited on it.
        """
        if self._renewals != since and self._renew_error is not None:
            raise self._renew_error
        self._renewals += 1
        try:
            if not (refresh and await self._refresh()):
                await self.authenticate()
        except ZMError as err:
            self._renew_error = err
            raise
        self._renew_error = None

    async def close(self) -> None:
        """Drop local tokens. JWT logout is stateless."""
        self._authenticated = False
        self._access_token = None
        self._refresh_token = None
        self._token_expires = None
        self._live_sessions.clear()
        self._live_restarts.clear()
        self._rtsp_links.clear()
        self._rtsp_refused_until.clear()
        self._whep_supported = None

    # -- Requests ------------------------------------------------------------

    async def _request(
        self, method: str, endpoint: str, timeout: int | None = None, **kwargs: Any
    ) -> Any:
        """Make an authenticated JSON request, retrying after a refresh.

        A 401 renews the token and retries. If the new token is refused too,
        it was renewed in the same second as a logout elsewhere on the
        account, so the client waits a second, renews again and retries once
        more.
        """
        await self._async_ensure_token()

        url = self._build_url(endpoint)
        client_timeout = (
            aiohttp.ClientTimeout(total=timeout) if timeout else self._timeout
        )

        extra_headers = kwargs.pop("headers", {})
        sent_token, since = self._access_token, self._renewals
        for attempt in (1, 2, 3):
            if attempt > 1:
                # Renew outside the refused response, so its connection is
                # back in the pool for the wait and the login.
                if attempt == 3:
                    await asyncio.sleep(_REVOKED_RETRY_DELAY)
                await self._renew(sent_token, since)
                sent_token, since = self._access_token, self._renewals
            headers = {**extra_headers, **self._auth_headers()}
            detail: str | None = None
            try:
                async with self._session.request(
                    method,
                    url,
                    headers=headers,
                    timeout=client_timeout,
                    ssl=self._verify_ssl,
                    **kwargs,
                ) as response:
                    if response.status == 401:
                        if attempt == 3:
                            raise ZMAuthenticationError(
                                "Authentication failed after refreshing the token"
                            )
                        continue
                    if response.status == 404:
                        raise ZMNotFoundError(f"Not found: {endpoint}")
                    if response.status >= 400:
                        detail = _error_message(await response.text())
                    response.raise_for_status()
                    if response.status == 204 or not response.content_length:
                        text = await response.text()
                        if not text:
                            return {}
                        return await response.json(content_type=None)
                    return await response.json(content_type=None)
            except aiohttp.ClientResponseError as err:
                raise ZMApiError(
                    f"{endpoint} returned {err.status}: {detail or err.message}"
                ) from err
            except (aiohttp.ClientError, TimeoutError) as err:
                raise ZMConnectionError(f"Request to {endpoint} failed: {err}") from err

        raise ZMApiError(f"Request to {endpoint} did not complete")

    @asynccontextmanager
    async def _async_open(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Yield the raw response to a request that carries the token.

        A 401 means zm-api revoked the token (a logout or password change
        elsewhere on the account), so the token is renewed and the request
        sent again, as :meth:`_request` does, including its second renewal
        after the logout second. The caller checks the status.
        """
        for attempt in (1, 2, 3):
            sent_token, since = self._access_token, self._renewals
            async with self._session.request(
                method,
                url,
                headers={**(headers or {}), **self._auth_headers()},
                ssl=self._verify_ssl,
                **kwargs,
            ) as response:
                if response.status != 401 or attempt == 3:
                    yield response
                    return
            if attempt == 2:
                await asyncio.sleep(_REVOKED_RETRY_DELAY)
            await self._renew(sent_token, since)

    async def _paginate(self, endpoint: str, **params: Any) -> list[dict[str, Any]]:
        """Collect every page of a paginated collection endpoint."""
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            response = await self._request(
                "GET",
                endpoint,
                params={**params, "page": page, "page_size": _PAGE_SIZE},
            )
            batch = response.get("items") or []
            items.extend(batch)
            last_page = response.get("last_page") or 1
            if page >= last_page or not batch:
                break
            page += 1
        return items

    # -- Server --------------------------------------------------------------

    async def get_version(self) -> str:
        """Return the ZoneMinder version."""
        response = await self._request("GET", V3_VERSION)
        return str(response.get("version") or "unknown")

    async def get_server_info(self) -> ServerInfo:
        """Return version, daemon health and storage usage."""
        info = ServerInfo()

        version = await self._request("GET", V3_VERSION)
        info.version = version.get("version", "unknown")
        info.api_version = version.get("api_version")
        info.db_version = version.get("db_version")

        try:
            status = await self._request("GET", V3_SYSTEM_STATUS)
            daemons = status.get("daemons") or []
            running = status.get("running")
            info.running = running if isinstance(running, bool) else None
            info.daemons_total = len(daemons)
            info.daemons_running = sum(
                1 for d in daemons if d.get("state") == "running"
            )
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("System status unavailable: %s", err)

        try:
            for item in await self._paginate(V3_STORAGE):
                info.storage.append(StorageArea.from_v3_dict(item))
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Storage list unavailable: %s", err)

        return info

    async def get_configs(self) -> dict[str, str]:
        """Return ZoneMinder's configuration keys and values."""
        configs: dict[str, str] = {}
        for item in await self._paginate(V3_CONFIGS):
            name = item.get("name")
            if name:
                configs[name] = item.get("value") or ""
        return configs

    # -- Monitors ------------------------------------------------------------

    async def get_monitors(self) -> list[Monitor]:
        """Return every monitor."""
        monitors = []
        for item in await self._paginate(V3_MONITORS):
            if item.get("deleted"):
                continue
            try:
                monitors.append(Monitor.from_v3_dict(item))
            except (KeyError, ValueError, TypeError) as err:
                _LOGGER.warning("Skipping unparsable monitor: %s", err)
        return monitors

    async def get_monitor(self, monitor_id: int) -> Monitor:
        """Return a single monitor."""
        response = await self._request("GET", V3_MONITOR.format(monitor_id=monitor_id))
        return Monitor.from_v3_dict(response)

    async def get_monitor_statuses(self) -> dict[int, MonitorStatus]:
        """Return live capture stats for every monitor in one request."""
        statuses: dict[int, MonitorStatus] = {}
        try:
            items = await self._paginate(V3_MONITOR_STATUS)
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Monitor status unavailable: %s", err)
            return statuses
        for item in items:
            try:
                status = MonitorStatus.from_v3_dict(item)
            except (KeyError, ValueError, TypeError):
                continue
            statuses[status.monitor_id] = status
        return statuses

    async def set_monitor_state(
        self,
        monitor_id: int,
        *,
        capturing: str | None = None,
        analysing: str | None = None,
        recording: str | None = None,
        decoding: str | None = None,
    ) -> None:
        """Update a monitor's capture, analysis, recording or decoding mode."""
        body = {
            key: value
            for key, value in (
                ("capturing", capturing),
                ("analysing", analysing),
                ("recording", recording),
                ("decoding", decoding),
            )
            if value is not None
        }
        if not body:
            return
        await self._request(
            "PATCH", V3_MONITOR.format(monitor_id=monitor_id), json=body
        )
        _LOGGER.debug("Monitor %d updated: %s", monitor_id, body)

    # -- Run states ----------------------------------------------------------

    async def get_run_states(self) -> list[RunState]:
        """Return the server's saved run states."""
        return [RunState.from_dict(item) for item in await self._paginate(V3_STATES)]

    async def save_run_state(self, name: str, definition: str) -> None:
        """Create the run state, or overwrite it if the name exists."""
        states = await self.get_run_states()
        existing = next((s for s in states if s.name == name), None)
        if existing is None:
            # zm-api requires is_active on create; a saved state starts inactive.
            await self._request(
                "POST",
                V3_STATES,
                json={"name": name, "definition": definition, "is_active": 0},
            )
        else:
            await self._request(
                "PATCH",
                V3_STATE.format(state_id=existing.id),
                json={"definition": definition},
            )

    async def _async_server_control(self, action: str) -> None:
        """zm-api runs systemctl, or zmcontrol.pl where there's no systemd."""
        await self._request("POST", V3_SERVER_CONTROL.format(action=action))

    async def set_run_state(self, state: RunState) -> None:
        """Apply a run state the way zmpkg.pl does, then mark it active.

        API v3 stores run states but has no "apply" call, so each monitor whose
        modes differ is updated directly. The ZoneMinder daemons pick monitor
        changes up without the full restart zmpkg.pl performs.
        """
        modes = state.monitor_modes()
        for monitor in await self.get_monitors():
            wanted = modes.get(monitor.id)
            if wanted is None:
                continue
            if wanted != (monitor.capturing, monitor.analysing, monitor.recording):
                capturing, analysing, recording = wanted
                await self.set_monitor_state(
                    monitor.id,
                    capturing=capturing,
                    analysing=analysing,
                    recording=recording,
                )
        for other in await self.get_run_states():
            should_be_active = other.id == state.id
            if other.active != should_be_active:
                await self._request(
                    "PATCH",
                    V3_STATE.format(state_id=other.id),
                    json={"is_active": int(should_be_active)},
                )

    # -- Events --------------------------------------------------------------

    async def get_events(
        self, monitor_id: int | None = None, limit: int = 100
    ) -> list[Event]:
        """Return events newest first, optionally filtered to one monitor."""
        params: dict[str, Any] = {
            "page": 1,
            "page_size": limit,
            "sort": "start_time",
            "direction": "desc",
        }
        if monitor_id is not None:
            params["monitor_id"] = monitor_id

        response = await self._request("GET", V3_EVENTS, params=params)
        events = []
        for item in response.get("items") or []:
            try:
                events.append(Event.from_v3_dict(item))
            except (KeyError, ValueError, TypeError) as err:
                _LOGGER.debug("Skipping unparsable event: %s", err)
        return events

    async def get_event(self, event_id: int) -> Event:
        """Return a single event."""
        response = await self._request("GET", V3_EVENT.format(event_id=event_id))
        return Event.from_v3_dict(response)

    async def get_events_between(
        self,
        monitor_id: int,
        start: datetime,
        end: datetime,
        limit: int = 500,
    ) -> list[Event]:
        """Return a monitor's events that started inside a time window.

        zm-api's ``end_time`` filter is on the event's end, which drops events
        still recording. So only the start is sent, events are read oldest
        first until one starts after the window, and the end is applied here.
        The newest ``limit`` are returned, newest first.
        """
        events: list[Event] = []
        page = 1
        while True:
            response = await self._request(
                "GET",
                V3_EVENTS,
                params={
                    "page": page,
                    "page_size": limit,
                    "monitor_id": monitor_id,
                    "start_time": _iso8601(start),
                    "sort": "start_time",
                    "direction": "asc",
                },
            )
            batch = response.get("items") or []
            past_end = False
            for item in batch:
                try:
                    event = Event.from_v3_dict(item)
                except (KeyError, ValueError, TypeError) as err:
                    _LOGGER.debug("Skipping unparsable event: %s", err)
                    continue
                if event.start_time >= end:
                    past_end = True
                    break
                events.append(event)
            if past_end or not batch or page >= (response.get("last_page") or 1):
                break
            page += 1
        events.reverse()
        return events[:limit]

    async def get_event_summary(
        self, monitor_id: int, *, include_archived: bool = True
    ) -> EventSummary | None:
        """Return event counts and disk usage for a monitor.

        zm-api's counts include archived events and take no filter for them
        (zm-api#205). Without them, the monitor's archived events of the last
        month are read and taken off each count; the total loses them all.
        """
        try:
            response = await self._request(
                "GET", V3_EVENT_SUMMARY.format(monitor_id=monitor_id)
            )
            summary = EventSummary.from_v3_dict(response)
        except ZMNotFoundError:
            # zm-api has no summary row for a monitor that has never recorded.
            return EventSummary(monitor_id=monitor_id)
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Event summary for monitor %d failed: %s", monitor_id, err)
            return None
        if include_archived or not summary.archived_events:
            return summary

        now = datetime.now(UTC)
        month_ago = _month_before(now)
        try:
            starts = await self._async_archived_starts(monitor_id, month_ago)
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Archived events of monitor %d failed: %s", monitor_id, err)
            return None
        since = {
            "hour_events": now - timedelta(hours=1),
            "day_events": now - timedelta(days=1),
            "week_events": now - timedelta(weeks=1),
            "month_events": month_ago,
        }
        counts = {
            field: max(0, getattr(summary, field) - sum(s >= start for s in starts))
            for field, start in since.items()
        }
        counts["total_events"] = max(0, summary.total_events - summary.archived_events)
        return replace(summary, **counts)

    async def _async_archived_starts(
        self, monitor_id: int, start: datetime
    ) -> list[datetime]:
        """Return when each of a monitor's archived events since ``start`` began."""
        starts: list[datetime] = []
        page = 1
        while True:
            response = await self._request(
                "GET",
                V3_EVENTS,
                params={
                    "page": page,
                    "page_size": 1000,
                    "monitor_id": monitor_id,
                    "archived": "true",
                    "start_time": _iso8601(start),
                },
            )
            for item in response.get("items") or []:
                try:
                    starts.append(Event.from_v3_dict(item).start_time)
                except (KeyError, ValueError, TypeError) as err:
                    _LOGGER.debug("Skipping unparsable event: %s", err)
            if not response.get("items") or page >= (response.get("last_page") or 1):
                return starts
            page += 1

    # -- Alarm and PTZ -------------------------------------------------------

    async def trigger_alarm(self, monitor_id: int, cause: str) -> None:
        """Force a monitor into alarm state.

        The cause is cut to the length ZoneMinder stores, which zm-api enforces.
        """
        cause = cause.encode()[:_ALARM_CAUSE_MAX].decode(errors="ignore")
        await self._request(
            "PATCH",
            V3_MONITOR_ALARM.format(monitor_id=monitor_id),
            json={"action": "on", "cause": cause},
        )

    async def cancel_alarm(self, monitor_id: int) -> None:
        """Clear a forced alarm."""
        await self._request(
            "PATCH",
            V3_MONITOR_ALARM.format(monitor_id=monitor_id),
            json={"action": "off"},
        )

    async def ptz(self, monitor_id: int, command: str, **kwargs: Any) -> None:
        """Issue a PTZ command.

        ``speed`` (0-100) sets how fast moves, zoom and focus go.
        """
        speed = kwargs.get("speed", PTZ_DEFAULT_SPEED)
        body: dict[str, Any] = {}
        if command in _PTZ_MOVE_DIRECTIONS:
            endpoint = V3_PTZ_MOVE.format(
                monitor_id=monitor_id, direction=_PTZ_MOVE_DIRECTIONS[command]
            )
            body = {"pan_speed": speed, "tilt_speed": speed}
        elif command in _PTZ_ZOOM_DIRECTIONS:
            endpoint = V3_PTZ_ZOOM.format(
                monitor_id=monitor_id, direction=_PTZ_ZOOM_DIRECTIONS[command]
            )
            body = {"speed": speed}
        elif command in _PTZ_FOCUS_DIRECTIONS:
            endpoint = V3_PTZ_FOCUS.format(
                monitor_id=monitor_id, direction=_PTZ_FOCUS_DIRECTIONS[command]
            )
            body = {"speed": speed}
        elif command == "home":
            endpoint = V3_PTZ_HOME.format(monitor_id=monitor_id)
        elif command == "preset":
            preset = kwargs.get("preset")
            if preset is None:
                raise ZMApiError("The 'preset' command requires a preset number")
            endpoint = V3_PTZ_PRESET_GOTO.format(monitor_id=monitor_id, preset=preset)
        else:
            raise ZMNotSupportedError(f"Unsupported PTZ command: {command}")

        await self._request("POST", endpoint, json=body)
        _LOGGER.debug("PTZ %s sent to monitor %d", command, monitor_id)

    async def get_alarm_state(self, monitor_id: int) -> str | None:
        """Return a monitor's alarm state, which zm-api reads from zmc's memory.

        zm-api leaves it out while zmc isn't running, and has no status row
        for a monitor that never started.
        """
        try:
            response = await self._request(
                "GET", V3_MONITOR_STATUS_ONE.format(monitor_id=monitor_id)
            )
        except ZMNotFoundError:
            return None
        state = response.get("alarm_state") if isinstance(response, dict) else None
        return state if state in ALARM_STATES else None

    async def get_ptz_presets(self, monitor_id: int) -> dict[int, str]:
        """Return the configured PTZ presets for a monitor."""
        presets: dict[int, str] = {}
        try:
            items = await self._paginate(V3_CONTROL_PRESETS, monitor_id=monitor_id)
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("PTZ presets for monitor %d failed: %s", monitor_id, err)
            return presets
        for item in items:
            if item.get("monitor_id") != monitor_id:
                continue
            number = item.get("preset")
            if number is None:
                continue
            presets[int(number)] = item.get("label") or f"Preset {number}"
        return presets

    # -- Live streaming ------------------------------------------------------

    async def async_prepare_live(self, monitor_id: int) -> None:
        """Start a live session so the HLS playlist and WebRTC socket exist.

        zm-api only materialises a monitor's playlist once a session is
        running, and sessions are reference counted server side, so we re-arm
        ours periodically rather than on every frame request. A session zm-api
        reaped in between is started again by :meth:`async_stream_media`.
        """
        last = self._live_sessions.get(monitor_id)
        if last and datetime.now(UTC) - last < _LIVE_SESSION_TTL:
            return
        await self._async_start_live(monitor_id)

    async def _async_start_live(self, monitor_id: int) -> None:
        """Start a monitor's live session and wait for its first segment."""
        try:
            await self._request(
                "POST",
                V3_LIVE_START.format(monitor_id=monitor_id),
                json={"enable_hls": True, "enable_webrtc": True},
                timeout=LONG_TIMEOUT,
            )
            _LOGGER.debug("Live session started for monitor %d", monitor_id)
        except ZMApiError as err:
            cause = err.__cause__
            if not (
                isinstance(cause, aiohttp.ClientResponseError) and cause.status == 409
            ):
                _LOGGER.warning(
                    "Could not start a live session for monitor %d: %s",
                    monitor_id,
                    err,
                )
                return
            # 409: the session is already running, which is all we need.
            _LOGGER.debug("Live session already running for monitor %d", monitor_id)
        except ZMConnectionError as err:
            _LOGGER.warning(
                "Could not start a live session for monitor %d: %s", monitor_id, err
            )
            return
        self._live_sessions[monitor_id] = datetime.now(UTC)
        await self._async_wait_for_first_segment(monitor_id)

    # -- Event stream ----------------------------------------------------------

    async def async_event_stream(
        self, since: int | None = None
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield (message type, data) from zm-api's server-sent event stream.

        The stream ends when the access token expires (about every ten
        minutes); the caller reconnects with ``since`` to pick up where it left
        off. Raises ZMNotFoundError on servers without the stream.
        """
        await self._async_ensure_token()
        params = {"since": str(since)} if since is not None else None
        async with self._async_open(
            "GET",
            self._build_url(V3_EVENT_STREAM),
            headers={"Accept": "text/event-stream"},
            params=params,
            # No overall limit; a silent minute means the link is dead, since
            # the server sends a heartbeat every 15 seconds.
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=60),
        ) as response:
            # Older servers route /events/stream to /events/{id} and reject
            # "stream" as an id (400 or 422) rather than answering 404.
            if response.status in (400, 404, 405, 422):
                raise ZMNotFoundError("This server has no event stream")
            if response.status in (401, 403):
                raise ZMAuthenticationError("Event stream refused the token")
            response.raise_for_status()

            message_type = "message"
            data: list[str] = []
            async for raw in response.content:
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if not line:
                    if data:
                        try:
                            payload = json.loads("\n".join(data))
                        except ValueError:
                            payload = None
                        if isinstance(payload, dict):
                            yield payload.get("type", message_type), payload
                    message_type, data = "message", []
                elif line.startswith(":"):
                    continue
                else:
                    field, _, value = line.partition(":")
                    value = value.removeprefix(" ")
                    if field == "event":
                        message_type = value
                    elif field == "data":
                        data.append(value)

    # -- WebRTC (WHEP) -------------------------------------------------------

    async def async_supports_whep(self, monitor_id: int) -> bool:
        """Return True when the server has WHEP playback (RFC 9725).

        An empty, non-SDP POST is cheap: a server with the endpoint refuses it
        with 415 (or 400) before doing any work; one without it answers 404.
        """
        if self._whep_supported is not None:
            return self._whep_supported
        await self._async_ensure_token()
        try:
            async with self._async_open(
                "POST",
                self._build_url(V3_LIVE_WHEP.format(monitor_id=monitor_id)),
                headers={"Content-Type": "text/plain"},
                data=b"",
                timeout=self._timeout,
            ) as response:
                self._whep_supported = response.status in (400, 415)
        except (aiohttp.ClientError, TimeoutError) as err:
            _LOGGER.debug("WHEP probe failed: %s", err)
            return False
        _LOGGER.debug(
            "WHEP playback %s", "available" if self._whep_supported else "not available"
        )
        return self._whep_supported

    async def async_whep_offer(
        self, monitor_id: int, offer_sdp: str
    ) -> tuple[str, str]:
        """Send a browser's SDP offer; return (answer SDP, session URL)."""
        await self._async_ensure_token()
        try:
            async with self._async_open(
                "POST",
                self._build_url(V3_LIVE_WHEP.format(monitor_id=monitor_id)),
                headers={"Content-Type": "application/sdp"},
                data=offer_sdp.encode(),
                timeout=self._timeout,
            ) as response:
                body = await response.text()
                if response.status != 201:
                    raise ZMApiError(
                        _WHEP_ERRORS.get(
                            response.status, body.strip() or str(response.status)
                        )
                    )
                location = response.headers.get("Location")
                if not location:
                    raise ZMApiError("WebRTC answer had no session location")
                return body, location
        except (aiohttp.ClientError, TimeoutError) as err:
            raise ZMConnectionError(f"WebRTC offer failed: {err}") from err

    def _whep_session_url(self, location: str) -> str:
        """Return a WHEP session's URL from its Location header.

        zm-api sends a path; RFC 9725 also allows an absolute URL.
        """
        if location.startswith(("http://", "https://")):
            return location
        return self._build_url(location)

    async def async_whep_candidate(
        self, location: str, sdp_mid: str | None, candidate: str
    ) -> None:
        """Trickle one browser ICE candidate to an open session."""
        lines = []
        if sdp_mid is not None:
            lines.append(f"a=mid:{sdp_mid}")
        lines.append(f"a={candidate}" if not candidate.startswith("a=") else candidate)
        await self._async_ensure_token()
        try:
            async with self._async_open(
                "PATCH",
                self._whep_session_url(location),
                headers={"Content-Type": "application/trickle-ice-sdpfrag"},
                data=("\r\n".join(lines) + "\r\n").encode(),
                timeout=self._timeout,
            ) as response:
                if response.status >= 400:
                    _LOGGER.debug(
                        "Candidate refused (%s) for %s", response.status, location
                    )
        except (aiohttp.ClientError, TimeoutError) as err:
            raise ZMConnectionError(
                f"Sending a WebRTC candidate failed: {err}"
            ) from err

    async def async_whep_close(self, location: str) -> None:
        """End a WebRTC session."""
        try:
            async with self._async_open(
                "DELETE", self._whep_session_url(location), timeout=self._timeout
            ):
                pass
        except (aiohttp.ClientError, TimeoutError, ZMError) as err:
            _LOGGER.debug("Closing WebRTC session %s failed: %s", location, err)

    async def async_get_rtsp_url(self, monitor_id: int) -> str | None:
        """Return a signed RTSP link for the monitor, or None.

        zm-api mints per-monitor, view-only links that carry their own
        credentials (``?st=``), so a player can open them directly, with no
        proxy. Links are reused
        until a day before they expire. Servers without RTSP (switched off, or
        older than the feature) answer 503 or 404, and we fall back to HLS.
        """
        now = datetime.now(UTC)
        cached = self._rtsp_links.get(monitor_id)
        if cached and cached[1] - now > _RTSP_LINK_RENEW:
            return cached[0]
        if self._rtsp_unavailable_until and now < self._rtsp_unavailable_until:
            return None
        refused = self._rtsp_refused_until.get(monitor_id)
        if refused and now < refused:
            return None

        try:
            result = await self._request(
                "POST",
                V3_LIVE_RTSP_LINK.format(monitor_id=monitor_id),
                json={"ttl_seconds": int(_RTSP_LINK_TTL.total_seconds())},
            )
        except ZMApiError as err:
            cause = err.__cause__
            if isinstance(cause, aiohttp.ClientResponseError) and cause.status == 503:
                # The RTSP server is off: no monitor can have a link.
                _LOGGER.debug("RTSP is not available, using HLS: %s", err)
                self._rtsp_unavailable_until = now + _RTSP_RETRY
            else:
                # Refused for this monitor only (no access to it, or no such
                # monitor); the others may still have links.
                _LOGGER.debug("No RTSP link for monitor %d: %s", monitor_id, err)
                self._rtsp_refused_until[monitor_id] = now + _RTSP_RETRY
            return None
        except ZMConnectionError as err:
            _LOGGER.debug(
                "Could not mint an RTSP link for monitor %d: %s", monitor_id, err
            )
            return None

        url = result.get("url") if isinstance(result, dict) else None
        if not isinstance(url, str) or not url:
            return None
        try:
            expires = datetime.fromtimestamp(float(result["expires_at"]), UTC)
        except (KeyError, TypeError, ValueError):
            expires = now + _RTSP_LINK_TTL
        self._rtsp_links[monitor_id] = (url, expires)
        self._rtsp_unavailable_until = None
        return url

    async def _async_wait_for_first_segment(self, monitor_id: int) -> None:
        """Wait until a new session's playlist lists a segment.

        A fresh session's playlist is empty for a few seconds, and some
        players (Home Assistant's stream worker, for one) give up on a
        playlist with no segments. So the URL is handed out only once there
        is something to play. Times out quietly; players retry.
        """
        path = V3_LIVE_HLS_PLAYLIST.format(monitor_id=monitor_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _FIRST_SEGMENT_TIMEOUT
        while True:
            try:
                async with self._async_get_media(path) as response:
                    if response.status == 200 and "#EXTINF" in await response.text():
                        return
            except (aiohttp.ClientError, TimeoutError) as err:
                _LOGGER.debug("Polling playlist for monitor %d: %s", monitor_id, err)
            if loop.time() >= deadline:
                _LOGGER.debug(
                    "Monitor %d published no segment within %.0fs",
                    monitor_id,
                    _FIRST_SEGMENT_TIMEOUT,
                )
                return
            await asyncio.sleep(_FIRST_SEGMENT_POLL)

    async def async_stop_live(self, monitor_id: int) -> None:
        """Release a live session."""
        self._live_sessions.pop(monitor_id, None)
        try:
            await self._request("DELETE", V3_LIVE_STOP.format(monitor_id=monitor_id))
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Stopping live session %d failed: %s", monitor_id, err)

    async def get_live_stats(self, monitor_id: int) -> dict[str, Any]:
        """Return live session statistics."""
        stats = await self._request("GET", V3_LIVE_STATS.format(monitor_id=monitor_id))
        return stats if isinstance(stats, dict) else {}

    def get_stream_type(self, monitor_id: int) -> str:
        """zm-api serves HLS."""
        return STREAM_TYPE_HLS

    async def get_stream_source(self, monitor_id: int) -> str | None:
        """Return nothing. v3 live streams go through the media proxy.

        HLS segments need a bearer token, which most players can't attach.
        Use :meth:`async_get_rtsp_url`, or relay :meth:`get_live_media_path`
        through :meth:`async_stream_media`, which adds the token.
        """
        return None

    def get_webrtc_signaling_url(self, monitor_id: int) -> str | None:
        """Return the WebSocket URL for zm-api's native WebRTC signaling."""
        base = self._host.replace("https://", "wss://").replace("http://", "ws://")
        return f"{base}/{V3_LIVE_WEBRTC_WS.format(monitor_id=monitor_id)}"

    # -- Media ---------------------------------------------------------------

    def get_live_media_path(self, monitor_id: int) -> str:
        """Return the relative path of the live HLS master playlist."""
        return V3_LIVE_HLS_MASTER.format(monitor_id=monitor_id)

    def get_event_media_path(self, event_id: int) -> str:
        """Return the relative path of an event's video."""
        return V3_EVENT_VIDEO.format(event_id=event_id)

    def get_event_thumbnail_path(self, event_id: int) -> str:
        """Return the relative path of an event's thumbnail."""
        return V3_EVENT_THUMBNAIL.format(event_id=event_id)

    async def async_get_snapshot(self, monitor_id: int) -> bytes | None:
        """Return one JPEG still frame."""
        await self._async_ensure_token()
        url = self._build_url(V3_MONITOR_SNAPSHOT.format(monitor_id=monitor_id))
        sent_token, since = self._access_token, self._renewals
        for attempt in (1, 2):
            if attempt == 2:
                await self._renew(sent_token, since)
            try:
                async with self._session.get(
                    url,
                    headers=self._auth_headers(),
                    timeout=aiohttp.ClientTimeout(total=LONG_TIMEOUT),
                    ssl=self._verify_ssl,
                ) as response:
                    if response.status == 401 and attempt == 1:
                        continue
                    if response.status == 404:
                        # zm-api has no frame for a monitor that isn't capturing.
                        _LOGGER.debug("No snapshot for monitor %d", monitor_id)
                        return None
                    response.raise_for_status()
                    return await response.read()
            except (aiohttp.ClientError, TimeoutError) as err:
                _LOGGER.error("Snapshot for monitor %d failed: %s", monitor_id, err)
                return None
        return None

    @asynccontextmanager
    async def async_stream_media(
        self, path: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Yield an authenticated upstream response for a media path.

        A live playlist that answers 404 belongs to a session zm-api reaped
        for being idle, so the session is started again and the playlist
        fetched once more.
        """
        async with self._async_get_media(path, headers) as response:
            match = _LIVE_PLAYLIST.match(path.lstrip("/"))
            if response.status != 404 or match is None:
                yield response
                return
            monitor_id = int(match.group(1))
            last = self._live_restarts.get(monitor_id)
            now = datetime.now(UTC)
            if last and now - last < _LIVE_RESTART_INTERVAL:
                yield response
                return
            self._live_restarts[monitor_id] = now
        _LOGGER.debug("Live session for monitor %d has ended, restarting", monitor_id)
        await self._async_start_live(monitor_id)
        async with self._async_get_media(path, headers) as response:
            yield response

    @asynccontextmanager
    async def _async_get_media(
        self, path: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Yield the upstream response for a media path, carrying the token."""
        await self._async_ensure_token()
        async with self._async_open(
            "GET",
            self._build_url(path),
            headers=headers,
            timeout=aiohttp.ClientTimeout(
                total=None, sock_connect=DEFAULT_TIMEOUT, sock_read=LONG_TIMEOUT
            ),
            allow_redirects=False,
        ) as response:
            yield response


def _error_message(body: str) -> str | None:
    """Return the reason from a zm-api error body, if it has one."""
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    message = payload.get("error_message") if isinstance(payload, dict) else None
    if not isinstance(message, str):
        return None
    return message.strip() or None


def _month_before(value: datetime) -> datetime:
    """Go back one calendar month, as ZoneMinder's ``INTERVAL 1 MONTH`` does."""
    year, month = (
        (value.year, value.month - 1) if value.month > 1 else (value.year - 1, 12)
    )
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _iso8601(value: datetime) -> str:
    """Format a datetime the way zm-api's time filters expect."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
