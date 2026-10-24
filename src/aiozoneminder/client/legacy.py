"""Client for ZoneMinder's built-in CakePHP API.

Talks to ``/api/...`` with query-parameter token authentication. This backend
works against a stock ZoneMinder install and is the fallback for people who do
not run the standalone zm-api server.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging
import re
import time
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import aiohttp

from ..const import (
    API_ALARM_OFF,
    API_ALARM_ON,
    API_CONFIGS,
    API_DAEMON_CHECK,
    API_EVENT,
    API_EVENTS,
    API_LOGIN,
    API_LOGOUT,
    API_MONITOR,
    API_MONITORS,
    API_SERVERS,
    API_STATE_CHANGE,
    API_STATES,
    API_VERSION,
    BACKEND_LEGACY,
    CONFIG_GO2RTC_PATH,
    DEFAULT_STREAM_MAXFPS,
    DEFAULT_TIMEOUT,
    EVENT_VIDEO_PATH,
    LONG_TIMEOUT,
    PTZ_DEFAULT_SPEED,
    STREAM_PATH,
    STREAM_TYPE_GO2RTC,
    STREAM_TYPE_MJPEG,
    WEB_REQUEST,
    ZMS_PATHS,
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
    _to_int,
)
from .base import ZoneMinderClient

_LOGGER = logging.getLogger(__name__)

#: Normalized PTZ command to the legacy control verb.
_PTZ_MAP = {
    "up": "moveConUp",
    "down": "moveConDown",
    "left": "moveConLeft",
    "right": "moveConRight",
    "up-left": "moveConUpLeft",
    "up-right": "moveConUpRight",
    "down-left": "moveConDownLeft",
    "down-right": "moveConDownRight",
    "stop": "moveStop",
    "zoom-in": "zoomConTele",
    "zoom-out": "zoomConWide",
    "zoom-stop": "zoomStop",
    "focus-near": "focusConNear",
    "focus-far": "focusConFar",
    "focus-auto": "focusAuto",
    "focus-stop": "focusStop",
    "home": "presetHome",
    "preset": "presetGoto",
}

#: Commands that go to a fixed position, so take no speed.
_PTZ_NO_SPEED = {"home", "preset"}

#: How long a monitor list's statuses and event counts are reused. The
#: coordinator asks for them straight after the list, in the same poll.
_MONITOR_LIST_TTL = 10.0

#: The most events ZoneMinder returns per page (its paginator's maxLimit).
_MAX_PAGE_SIZE = 100

#: Seconds to wait for states/change (a run state, or start/stop/restart): it
#: answers only after zmpkg has restarted every daemon.
_STATE_CHANGE_TIMEOUT = 120

#: ZoneMinder 1.37 renamed the single Function field. Writing both keeps this
#: backend working across 1.36 and 1.37+.
_FUNCTION_FROM_STATE = {
    ("None", "None", "None"): "None",
    ("Always", "None", "None"): "Monitor",
    ("Always", "Always", "OnMotion"): "Modect",
    ("Always", "None", "Always"): "Record",
    ("Always", "Always", "Always"): "Mocord",
    ("Always", "None", "OnMotion"): "Nodect",
}


class ZoneMinderLegacyClient(ZoneMinderClient):
    """Client for ZoneMinder's built-in API."""

    backend = BACKEND_LEGACY

    def __init__(
        self,
        *args: Any,
        zms_path: str | None = None,
        stream_scale: int | None = None,
        stream_maxfps: float | None = DEFAULT_STREAM_MAXFPS,
        **kwargs: Any,
    ) -> None:
        """Initialize the client.

        ``zms_path`` is where ZoneMinder serves nph-zms: a path from the
        server root such as ``/cgi-bin-zm/nph-zms``, or a full URL. When
        omitted it is found after login. ``stream_scale`` (percent) and
        ``stream_maxfps`` shrink and slow MJPEG to save bandwidth.
        """
        super().__init__(*args, **kwargs)
        self._stream_scale = stream_scale
        self._stream_maxfps = stream_maxfps
        #: Monitor id to the id of the server recording it, when not the main one.
        self._monitor_servers: dict[int, int] = {}
        #: Server id to (nph-zms URL, index.php URL); None until first needed.
        self._servers: dict[int, tuple[str, str]] | None = None
        self._auth_token: str | None = None
        self._zms_url: str | None = _zms_url(self._host, zms_path) if zms_path else None
        self._zms_checked = zms_path is not None
        self._statuses: dict[int, MonitorStatus] = {}
        self._summaries: dict[int, EventSummary] = {}
        self._monitors_fetched_at: float | None = None
        self._go2rtc: dict[str, Any] | None = None
        self._go2rtc_checked = False
        self._go2rtc_streams: dict[int, str] = {}

    # -- Authentication ------------------------------------------------------

    async def authenticate(self) -> bool:
        """Log in and store the API token."""
        try:
            async with self._session.post(
                self._build_url(API_LOGIN),
                data={"username": self._username, "password": self._password},
                timeout=self._timeout,
                ssl=self._verify_ssl,
                allow_redirects=True,
            ) as response:
                if response.status in (401, 403):
                    raise ZMAuthenticationError("Invalid username or password")
                if response.status == 404:
                    raise ZMConnectionError(
                        f"No ZoneMinder API at {self._host}. Check the URL includes "
                        "the ZoneMinder path (for example http://host/zm) and that "
                        "OPT_USE_API is enabled."
                    )
                response.raise_for_status()
                result = await response.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError) as err:
            raise ZMConnectionError(f"Connection failed: {err}") from err

        # Before 1.34 login returned only an auth= string, not a token; those
        # servers authenticate by the session cookie the login just set.
        self._auth_token = result.get("access_token")
        self._authenticated = True
        if not self._zms_checked:
            self._zms_checked = True
            self._zms_url = await self._async_find_zms()
        return True

    async def _async_find_zms(self) -> str | None:
        """Find where nph-zms is served.

        Packages put it in different places: under the web path on Debian
        and Ubuntu, /cgi-bin-zm on RPM, /cgi-bin from source. The right path
        answers anything but 404 (403 without a login).
        """
        origin = _origin(self._host)
        candidates = [self._build_url(STREAM_PATH)]
        candidates += [f"{origin}{path}" for path in ZMS_PATHS]
        for url in dict.fromkeys(candidates):
            try:
                async with self._session.get(
                    url, timeout=self._timeout, ssl=self._verify_ssl
                ) as response:
                    if response.status != 404:
                        _LOGGER.debug("Found nph-zms at %s", url)
                        return url
            except (aiohttp.ClientError, TimeoutError) as err:
                _LOGGER.debug("No nph-zms at %s: %s", url, err)
        _LOGGER.debug("nph-zms not found, assuming %s", candidates[0])
        return None

    async def close(self) -> None:
        """Log out."""
        if self._authenticated:
            try:
                async with self._session.get(
                    self._build_url(API_LOGOUT),
                    params={"token": self._auth_token} if self._auth_token else {},
                    timeout=self._timeout,
                    ssl=self._verify_ssl,
                ) as response:
                    response.raise_for_status()
            except (aiohttp.ClientError, TimeoutError) as err:
                _LOGGER.debug("Logout failed: %s", self._redact(str(err)))
        self._authenticated = False
        self._auth_token = None

    def _redact(self, text: str) -> str:
        """Remove the access token from text headed for a log or an error."""
        if self._auth_token:
            text = text.replace(self._auth_token, "***")
        return text

    async def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        """Make an authenticated request, retrying once after re-authenticating."""
        if not self._authenticated:
            await self.authenticate()

        url = endpoint if "://" in endpoint else self._build_url(endpoint)
        base_params = kwargs.pop("params", {})
        total = kwargs.pop("timeout", None)
        timeout = aiohttp.ClientTimeout(total=total) if total else self._timeout

        for attempt in (1, 2):
            params = dict(base_params)
            if self._auth_token:
                params["token"] = self._auth_token
            try:
                async with self._session.request(
                    method,
                    url,
                    params=params,
                    timeout=timeout,
                    ssl=self._verify_ssl,
                    **kwargs,
                ) as response:
                    if response.status in (401, 403):
                        if attempt == 2:
                            # The login just succeeded, so this is the user's
                            # permissions refusing the request, not a bad token.
                            message = await _error_message(response)
                            raise ZMApiError(
                                f"{endpoint} was refused: "
                                f"{message or 'insufficient privileges'}"
                            )
                        self._authenticated = False
                        await self.authenticate()
                        continue
                    if response.status == 404:
                        raise ZMNotFoundError(f"Not found: {endpoint}")
                    response.raise_for_status()
                    try:
                        return await response.json(content_type=None)
                    except ValueError as err:
                        raise ZMApiError(
                            f"{endpoint} returned a page that is not JSON"
                        ) from err
            # aiohttp's errors hold the URL, token included, so they aren't
            # chained on.
            except aiohttp.ClientResponseError as err:
                raise ZMApiError(
                    f"{endpoint} returned {err.status}: {err.message}"
                ) from None
            except (aiohttp.ClientError, TimeoutError) as err:
                detail = self._redact(str(err) or type(err).__name__)
                raise ZMConnectionError(
                    f"Request to {endpoint} failed: {detail}"
                ) from None

        raise ZMApiError(f"Request to {endpoint} did not complete")

    # -- Server --------------------------------------------------------------

    async def get_version(self) -> str:
        """Return the ZoneMinder version."""
        response = await self._request("GET", API_VERSION)
        return str(response.get("version") or "unknown")

    async def get_server_info(self) -> ServerInfo:
        """Return the version, and whether ZoneMinder's daemons are running.

        If daemonCheck fails, ``running`` stays None and the version still
        comes back.
        """
        info = ServerInfo(version=await self.get_version())
        try:
            check = await self._request("GET", API_DAEMON_CHECK)
        except ZMError as err:
            _LOGGER.debug("daemonCheck unavailable: %s", err)
        else:
            info.running = isinstance(check, dict) and _to_int(check.get("result")) == 1
        return info

    async def get_configs(self) -> dict[str, str]:
        """Return ZoneMinder's configuration keys and values."""
        response = await self._request("GET", API_CONFIGS)
        configs: dict[str, str] = {}
        for item in response.get("configs") or []:
            config = item.get("Config", item)
            name = config.get("Name")
            if name:
                configs[name] = config.get("Value") or ""
        return configs

    # -- Monitors ------------------------------------------------------------

    async def get_monitors(self) -> list[Monitor]:
        """Return every monitor that hasn't been deleted.

        The same response carries each monitor's capture status and event
        counts, which are kept for :meth:`get_monitor_statuses` and
        :meth:`get_event_summary`.
        """
        response = await self._request("GET", API_MONITORS)
        monitors = []
        statuses: dict[int, MonitorStatus] = {}
        summaries: dict[int, EventSummary] = {}
        monitor_servers: dict[int, int] = {}
        for item in response.get("monitors") or []:
            # ZoneMinder 1.38 keeps deleted monitors, marked Deleted.
            raw = item.get("Monitor", item)
            if str(raw.get("Deleted", "0")).lower() in ("1", "true"):
                continue
            try:
                monitors.append(Monitor.from_dict(item))
            except (KeyError, ValueError, TypeError) as err:
                _LOGGER.warning("Skipping unparsable monitor: %s", err)
                continue
            if server_id := monitors[-1].server_id:
                monitor_servers[monitors[-1].id] = server_id
            if status := MonitorStatus.from_dict(
                item.get("Monitor_Status") or {}, self._timezone
            ):
                statuses[status.monitor_id] = status
            # No summary row means no events yet: zero, as API v3 reports it.
            summaries[monitors[-1].id] = EventSummary.from_dict(
                item.get("Event_Summary") or {}
            ) or EventSummary(monitor_id=monitors[-1].id)
        self._statuses = statuses
        self._summaries = summaries
        self._monitors_fetched_at = time.monotonic()
        self._monitor_servers = monitor_servers
        if monitor_servers and self._servers is None:
            self._servers = await self._async_load_servers()
        return monitors

    async def _async_load_servers(self) -> dict[int, tuple[str, str]]:
        """Read the multi-server list: where each server serves streams and PTZ."""
        try:
            response = await self._request("GET", API_SERVERS)
        except ZMError as err:
            _LOGGER.warning(
                "Could not read ZoneMinder's servers, so every camera streams "
                "from %s: %s",
                self._host,
                err,
            )
            return {}
        servers: dict[int, tuple[str, str]] = {}
        for item in response.get("servers") or []:
            raw = item.get("Server", item)
            if not raw.get("Hostname"):
                continue
            scheme = raw.get("Protocol") or urlsplit(self._host).scheme
            netloc = raw["Hostname"]
            if str(raw.get("Port") or "").strip():
                netloc = f"{netloc}:{raw['Port']}"
            base = f"{scheme}://{netloc}"
            zms = raw.get("PathToZMS") or "/zm/cgi-bin/nph-zms"
            index = raw.get("PathToIndex") or "/zm/index.php"
            servers[_to_int(raw.get("Id"))] = (
                f"{base}/{zms.lstrip('/')}",
                f"{base}/{index.lstrip('/')}",
            )
        return servers

    def _monitor_server(self, monitor_id: int) -> tuple[str, str] | None:
        """Return (nph-zms URL, index.php URL) of a monitor's own server, if remote."""
        server_id = self._monitor_servers.get(monitor_id)
        if server_id is None or not self._servers:
            return None
        return self._servers.get(server_id)

    async def _async_fresh_monitor_list(self) -> None:
        """Fetch the monitor list unless one was fetched moments ago."""
        if (
            self._monitors_fetched_at is None
            or time.monotonic() - self._monitors_fetched_at > _MONITOR_LIST_TTL
        ):
            await self.get_monitors()

    async def get_monitor_statuses(self) -> dict[int, MonitorStatus]:
        """Return each monitor's capture status, from the monitor list."""
        await self._async_fresh_monitor_list()
        return dict(self._statuses)

    async def get_event_summary(self, monitor_id: int) -> EventSummary | None:
        """Return a monitor's event counts, from the monitor list."""
        await self._async_fresh_monitor_list()
        return self._summaries.get(monitor_id)

    async def get_monitor(self, monitor_id: int) -> Monitor:
        """Return a single monitor."""
        response = await self._request("GET", API_MONITOR.format(monitor_id=monitor_id))
        return Monitor.from_dict(response.get("monitor", response))

    async def set_monitor_state(
        self,
        monitor_id: int,
        *,
        capturing: str | None = None,
        analysing: str | None = None,
        recording: str | None = None,
        decoding: str | None = None,
    ) -> None:
        """Update a monitor's state fields.

        ZoneMinder 1.37+ accepts the split fields; older releases only accept
        ``Function``. We send both, filling any unspecified field from the
        monitor's current state so the derived Function stays correct.
        """
        current = await self.get_monitor(monitor_id)
        capturing = capturing if capturing is not None else current.capturing
        analysing = analysing if analysing is not None else current.analysing
        recording = recording if recording is not None else current.recording

        data = {
            "Monitor[Capturing]": capturing,
            "Monitor[Analysing]": analysing,
            "Monitor[Recording]": recording,
        }
        if decoding is not None:
            data["Monitor[Decoding]"] = decoding
        if function := _FUNCTION_FROM_STATE.get((capturing, analysing, recording)):
            data["Monitor[Function]"] = function

        response = await self._request(
            "POST", API_MONITOR.format(monitor_id=monitor_id), data=data
        )
        # A failed save still answers 200, with {"message": "Error ..."}.
        message = ""
        if isinstance(response, dict):
            message = str(response.get("message") or "")
        if message.startswith("Error"):
            raise ZMApiError(f"Monitor {monitor_id} was not saved: {message}")

    # -- Run states ----------------------------------------------------------

    async def get_run_states(self) -> list[RunState]:
        """Return the server's saved run states."""
        response = await self._request("GET", API_STATES)
        return [
            RunState.from_dict(item.get("State", item))
            for item in (response or {}).get("states", [])
        ]

    async def set_run_state(self, state: RunState) -> None:
        """Switch run state; ZoneMinder applies it and restarts its daemons.

        ZoneMinder answers only once zmpkg has restarted every daemon, which
        takes longer the more monitors there are (about 11 s for 45 in a lab),
        so this call waits longer than the others.
        """
        await self._request(
            "POST",
            API_STATE_CHANGE.format(name=quote(state.name, safe="")),
            timeout=_STATE_CHANGE_TIMEOUT,
        )

    async def _async_server_control(self, action: str) -> None:
        """zmpkg.pl takes start, stop and restart as well as run state names."""
        await self._request(
            "POST",
            API_STATE_CHANGE.format(name=action),
            timeout=_STATE_CHANGE_TIMEOUT,
        )

    # -- Events --------------------------------------------------------------

    async def get_events(
        self, monitor_id: int | None = None, limit: int = 100
    ) -> list[Event]:
        """Return events newest first, optionally filtered to one monitor."""
        endpoint = API_EVENTS
        if monitor_id is not None:
            # The CakePHP API expresses filters as path segments.
            endpoint = f"api/events/index/MonitorId:{monitor_id}.json"

        # ZoneMinder caps a page at 100 events, so larger limits take several.
        params: dict[str, Any] = {
            "sort": "StartTime",
            "direction": "desc",
            "limit": min(limit, _MAX_PAGE_SIZE),
        }
        events: dict[int, Event] = {}
        page = 1
        while True:
            try:
                response = await self._request("GET", endpoint, params=params)
            except ZMNotFoundError:
                if page == 1:
                    raise
                # Events deleted while paging can take the last page away.
                break
            seen = len(events)
            for item in response.get("events") or []:
                try:
                    event = Event.from_dict(item, self._timezone)
                except (KeyError, ValueError, TypeError) as err:
                    _LOGGER.debug("Skipping unparsable event: %s", err)
                    continue
                # A new event shifts the pages, so one can be seen twice.
                events[event.id] = event
            more = (response.get("pagination") or {}).get("nextPage")
            # A page with nothing new (a server ignoring ``page``) ends it too.
            if len(events) >= limit or not more or len(events) == seen:
                break
            page += 1
            params["page"] = page
        ordered = sorted(events.values(), key=lambda e: e.start_time, reverse=True)
        return ordered[:limit]

    async def get_event(self, event_id: int) -> Event:
        """Return a single event."""
        # Without noframes the reply carries every Frame row of the event.
        response = await self._request(
            "GET", API_EVENT.format(event_id=event_id), params={"noframes": "true"}
        )
        return Event.from_dict(response.get("event", response), self._timezone)

    # -- Alarm and PTZ -------------------------------------------------------

    async def trigger_alarm(self, monitor_id: int, cause: str) -> None:
        """Force a monitor into alarm state.

        The event's cause is always "Forced Web": ZoneMinder runs ``zmu -a``,
        which takes no cause, so ``cause`` is sent but not recorded. The web
        trigger (``ajax/trigger.php``) takes one, but needs Monitors=Edit for
        every monitor, which would refuse users who may edit only some.
        """
        response = await self._request(
            "GET", API_ALARM_ON.format(monitor_id=monitor_id), params={"cause": cause}
        )
        _check_alarm_reply(response, monitor_id)

    async def cancel_alarm(self, monitor_id: int) -> None:
        """Clear a forced alarm."""
        response = await self._request(
            "GET", API_ALARM_OFF.format(monitor_id=monitor_id)
        )
        _check_alarm_reply(response, monitor_id)

    async def ptz(self, monitor_id: int, command: str, **kwargs: Any) -> None:
        """Issue a PTZ command.

        Sent as ZoneMinder's own web UI sends it: the API's Controls endpoint
        only reads control definitions. ``speed`` (0-100) sets how fast moves,
        zoom and focus go; without one the control module stops the camera.
        """
        control = _PTZ_MAP.get(command)
        if control is None:
            raise ZMNotSupportedError(f"Unsupported PTZ command: {command}")

        data: dict[str, Any] = {
            "view": "request",
            "request": "control",
            "id": monitor_id,
            "control": control,
        }
        if command == "preset":
            preset = kwargs.get("preset")
            if preset is None:
                raise ZMApiError("The 'preset' command requires a preset number")
            data["preset"] = preset
        if command not in _PTZ_NO_SPEED:
            speed = kwargs.get("speed", PTZ_DEFAULT_SPEED)
            data["xge"] = data["yge"] = speed
        for key, param in (("x", "xCoord"), ("y", "yCoord")):
            if (value := kwargs.get(key)) is not None:
                data[param] = value

        server = self._monitor_server(monitor_id)
        endpoint = server[1] if server else WEB_REQUEST
        result = await self._request("POST", endpoint, data=data)
        if isinstance(result, dict) and result.get("result") == "Error":
            reason = result.get("message") or "no reason given"
            raise ZMApiError(f"ZoneMinder refused PTZ {command}: {reason}")

    # -- Streaming -----------------------------------------------------------

    def get_stream_url(self, monitor_id: int, mode: str = "jpeg") -> str:
        """Build an MJPEG or single-frame URL for ZoneMinder's streaming CGI."""
        params: dict[str, Any] = {"mode": mode, "monitor": monitor_id}
        if self._stream_scale is not None:
            params["scale"] = self._stream_scale
        if self._stream_maxfps is not None and mode != "single":
            params["maxfps"] = self._stream_maxfps
        params["buffer"] = 1000
        if self._auth_token:
            params["token"] = self._auth_token
        query = "&".join(f"{key}={value}" for key, value in params.items())
        server = self._monitor_server(monitor_id)
        zms = server[0] if server else self._zms_url or self._build_url(STREAM_PATH)
        return f"{zms}?{query}"

    def get_event_clip_url(self, event_id: int) -> str:
        """Build a URL for an event's MP4 clip."""
        url = f"{self._host}/{EVENT_VIDEO_PATH.format(event_id=event_id)}"
        if self._auth_token:
            url += f"?token={self._auth_token}"
        return url

    def get_stream_type(self, monitor_id: int) -> str:
        """Report the stream type actually being served.

        A go2rtc instance with its RTSP server switched off cannot be pulled
        from, so those monitors are MJPEG however many streams go2rtc lists.
        """
        if self._go2rtc_rtsp_url(monitor_id) is not None:
            return STREAM_TYPE_GO2RTC
        return STREAM_TYPE_MJPEG

    async def get_stream_source(self, monitor_id: int) -> str | None:
        """Return a go2rtc RTSP URL when one is usable, else the MJPEG URL."""
        await self._async_discover_go2rtc()
        if rtsp := self._go2rtc_rtsp_url(monitor_id):
            return rtsp
        return self.get_stream_url(monitor_id)

    async def async_get_snapshot(self, monitor_id: int) -> bytes | None:
        """Return one JPEG still frame, or None when there is none to be had."""
        for attempt in (1, 2):
            # The URL carries the token, so it is never logged.
            url = self.get_stream_url(monitor_id, mode="single")
            try:
                async with self._session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=LONG_TIMEOUT),
                    ssl=self._verify_ssl,
                ) as response:
                    if response.status in (401, 403) and attempt == 1:
                        # zms refuses an expired token; log in and try once more.
                        self._authenticated = False
                        await self.authenticate()
                        continue
                    if response.status >= 400:
                        _LOGGER.error(
                            "Snapshot for monitor %d failed: HTTP %d",
                            monitor_id,
                            response.status,
                        )
                        return None
                    image = await response.read()
            except (aiohttp.ClientError, TimeoutError) as err:
                _LOGGER.error(
                    "Snapshot for monitor %d failed: %s",
                    monitor_id,
                    self._redact(str(err) or type(err).__name__),
                )
                return None
            except (ZMAuthenticationError, ZMConnectionError) as err:
                _LOGGER.error("Snapshot for monitor %d failed: %s", monitor_id, err)
                return None
            if not image:
                # zms behind nginx/fcgiwrap, before ZoneMinder d27b9612f,
                # answers a refused request with an empty 200.
                _LOGGER.error(
                    "Snapshot for monitor %d failed: ZoneMinder sent no image",
                    monitor_id,
                )
                return None
            return image
        return None

    @asynccontextmanager
    async def async_stream_media(
        self, path: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Yield an upstream response for a media path.

        The legacy API authenticates with a query token, so its media URLs work
        directly and this only exists to satisfy the common contract.
        """
        if not self._authenticated:
            await self.authenticate()
        url = self._build_url(path)
        params = {"token": self._auth_token} if self._auth_token else {}
        async with self._session.get(
            url,
            params=params,
            headers=headers or {},
            timeout=aiohttp.ClientTimeout(
                total=None, sock_connect=DEFAULT_TIMEOUT, sock_read=LONG_TIMEOUT
            ),
            ssl=self._verify_ssl,
            allow_redirects=False,
        ) as response:
            yield response

    # -- go2rtc --------------------------------------------------------------

    async def _async_discover_go2rtc(self) -> dict[str, Any] | None:
        """Find ZoneMinder's go2rtc instance and map monitors to its streams.

        ZoneMinder names go2rtc streams after the monitor, but not always
        exactly: a monitor can appear as ``3``, ``3_0``, ``3_h264`` or
        ``3_ZoneMinderPrimary`` depending on how it was set up. Guessing the
        name is what made this silently fall back to MJPEG before, so the
        stream list is read and matched instead.
        """
        if self._go2rtc_checked:
            return self._go2rtc
        self._go2rtc_checked = True

        try:
            configs = await self.get_configs()
        except (ZMApiError, ZMConnectionError) as err:
            _LOGGER.debug("Could not read ZoneMinder configuration: %s", err)
            return None

        api_url = configs.get(CONFIG_GO2RTC_PATH)
        if not api_url:
            _LOGGER.debug("go2rtc is not configured (%s is unset)", CONFIG_GO2RTC_PATH)
            return None

        # ZoneMinder takes the address with or without the /api suffix, and
        # with credentials (user:pass@) when go2rtc requires them.
        api_url = api_url.rstrip("/")
        if not api_url.endswith("/api"):
            api_url += "/api"
        base_url = api_url[: -len("/api")]
        shown = _without_credentials(base_url)

        try:
            async with self._session.get(
                f"{api_url}/streams", timeout=self._timeout
            ) as response:
                response.raise_for_status()
                streams = await response.json(content_type=None)
            async with self._session.get(api_url, timeout=self._timeout) as response:
                response.raise_for_status()
                info = await response.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, ValueError) as err:
            _LOGGER.warning("go2rtc at %s did not respond: %s", shown, err)
            return None

        rtsp_port = _parse_rtsp_port(info)
        if rtsp_port is None:
            _LOGGER.info(
                "go2rtc at %s has its RTSP server disabled, so it can't be "
                "pulled from. Falling back to MJPEG",
                shown,
            )

        self._go2rtc = {
            "api_url": api_url,
            "base_url": base_url,
            "rtsp_port": rtsp_port,
            "host": _parse_host(base_url),
        }
        self._go2rtc_streams = _match_streams(streams)
        _LOGGER.info(
            "go2rtc at %s: %d monitor streams matched",
            shown,
            len(self._go2rtc_streams),
        )
        return self._go2rtc

    def _go2rtc_rtsp_url(self, monitor_id: int) -> str | None:
        """Return the go2rtc RTSP URL for a monitor, if one is usable."""
        if not self._go2rtc:
            return None
        port = self._go2rtc.get("rtsp_port")
        host = self._go2rtc.get("host")
        stream = self._go2rtc_streams.get(monitor_id)
        if not port or not host or not stream:
            return None
        return f"rtsp://{host}:{port}/{stream}"

    async def check_go2rtc(self) -> dict[str, Any] | None:
        """Return the discovered go2rtc configuration, if any."""
        return await self._async_discover_go2rtc()


def _origin(url: str) -> str:
    """Return a URL's scheme and host, without any path."""
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def _zms_url(host: str, zms_path: str) -> str:
    """Resolve a given nph-zms path: a full URL, or a path from the root."""
    if "://" in zms_path:
        return zms_path.rstrip("/")
    return f"{_origin(host)}/{zms_path.strip('/')}"


async def _error_message(response: aiohttp.ClientResponse) -> str | None:
    """Return the server's error text from an API v2 error reply.

    Errors come back as ``{"success": false, "data": {"message": ...}}``.
    """
    try:
        body = await response.json(content_type=None)
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    data = body.get("data")
    message = data.get("message") if isinstance(data, dict) else None
    return str(message) if message else None


def _check_alarm_reply(response: Any, monitor_id: int) -> None:
    """Raise when ZoneMinder's zmu failed to change a monitor's alarm.

    zmu failing (the monitor isn't capturing, or runs on another server) still
    answers 200, with ``{"status": "false", "code": <exit code>, "error": ...}``.
    """
    if not isinstance(response, dict) or response.get("status") != "false":
        return
    error = str(response.get("error") or "").strip() or "no output"
    raise ZMApiError(
        f"Alarm change on monitor {monitor_id} failed "
        f"(zmu exit {response.get('code')}): {error}"
    )


def _parse_host(url: str) -> str | None:
    """Extract the hostname from a URL, past any user:pass@."""
    return urlsplit(url).hostname


def _without_credentials(url: str) -> str:
    """Return the URL without user:pass@, for logging."""
    parts = urlsplit(url)
    if parts.username is None and parts.password is None:
        return url
    netloc = parts.hostname or ""
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit(parts._replace(netloc=netloc))


def _parse_rtsp_port(info: dict[str, Any]) -> int | None:
    """Return go2rtc's RTSP port, or None when its RTSP server is off.

    go2rtc reports ``{"rtsp": {"listen": ":8554"}}``. An empty listen address
    means no RTSP server is running, which is the default in some ZoneMinder
    packages.
    """
    listen = (info.get("rtsp") or {}).get("listen") or ""
    match = re.search(r":(\d+)$", listen)
    return int(match.group(1)) if match else None


def _match_streams(streams: Any) -> dict[int, str]:
    """Map monitor ids to go2rtc stream names.

    Prefers an exact numeric match, then the primary ``<id>_0`` sub-stream,
    then any other stream whose name starts with the monitor id.
    """
    if not isinstance(streams, dict):
        return {}

    candidates: dict[int, list[str]] = {}
    for name in streams:
        match = re.match(r"^(\d+)(?:_.*)?$", str(name))
        if match:
            candidates.setdefault(int(match.group(1)), []).append(str(name))

    matched: dict[int, str] = {}
    for monitor_id, names in candidates.items():
        exact = str(monitor_id)
        if exact in names:
            matched[monitor_id] = exact
        elif f"{monitor_id}_0" in names:
            matched[monitor_id] = f"{monitor_id}_0"
        else:
            matched[monitor_id] = sorted(names)[0]
    return matched
