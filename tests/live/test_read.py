"""Live tests that only read from the server."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import logging
from zoneinfo import ZoneInfo

import aiohttp
import pytest

from aiozoneminder import (
    BACKEND_LEGACY,
    BACKEND_V3,
    Monitor,
    ZMApiError,
    ZMAuthenticationError,
    ZMNotSupportedError,
    ZoneMinderClient,
)
from aiozoneminder.const import (
    ANALYSING_OPTIONS,
    CAPTURING_OPTIONS,
    DECODING_OPTIONS,
    RECORDING_OPTIONS,
    STREAM_TYPE_GO2RTC,
)

from .support import CONFIG, client_for, is_jpeg, until

pytestmark = pytest.mark.asyncio(loop_scope="session")

v2_only = pytest.mark.backends(BACKEND_LEGACY)
v3_only = pytest.mark.backends(BACKEND_V3)


async def test_login(
    backend: str, zm: ZoneMinderClient, live_http: aiohttp.ClientSession
) -> None:
    """Logging in works, and a wrong password says so rather than failing later."""
    assert zm.authenticated
    wrong = client_for(backend, live_http, password=CONFIG.password + "-wrong")
    with pytest.raises(ZMAuthenticationError):
        await wrong.authenticate()


async def test_server_info(zm: ZoneMinderClient) -> None:
    """The version, server info and configuration can be read."""
    version = await zm.get_version()
    assert version
    assert version != "unknown"
    info = await zm.get_server_info()
    assert info.version == version
    configs = await zm.get_configs()
    assert configs, "no configuration returned"
    assert any(name.startswith("ZM_") for name in configs)


async def test_monitors(
    zm: ZoneMinderClient, monitors: list[Monitor], monitor: Monitor
) -> None:
    """Monitors are listed with modes the library knows, and read singly the same."""
    assert monitors, "no monitors"
    ids = [m.id for m in monitors]
    assert len(ids) == len(set(ids)), f"monitor listed twice: {ids}"
    for m in monitors:
        assert m.capturing in CAPTURING_OPTIONS, (m.id, m.capturing)
        assert m.analysing in ANALYSING_OPTIONS, (m.id, m.analysing)
        assert m.recording in RECORDING_OPTIONS, (m.id, m.recording)
    one = await zm.get_monitor(monitor.id)
    listed = next(m for m in monitors if m.id == monitor.id)
    assert (one.name, one.capturing, one.analysing, one.recording) == (
        listed.name,
        listed.capturing,
        listed.analysing,
        listed.recording,
    )


async def test_decoding_values(monitors: list[Monitor]) -> None:
    """The decoding choices are ZoneMinder's, so a monitor's is never unknown."""
    assert "KeyFrames+Ondemand" in DECODING_OPTIONS
    for m in monitors:
        assert m.decoding in DECODING_OPTIONS, (m.id, m.decoding)


async def test_live_status(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Capture status and event counts are available for a capturing monitor."""
    statuses = await zm.get_monitor_statuses()
    assert monitor.id in statuses, (
        f"no status for monitor {monitor.id}: {sorted(statuses)}"
    )
    assert statuses[monitor.id].status
    assert await zm.get_event_summary(monitor.id) is not None


async def test_event_counts_without_archived(
    zm: ZoneMinderClient, monitor: Monitor
) -> None:
    """Leaving archived events out never raises a count; the total loses them all."""
    with_archived = await zm.get_event_summary(monitor.id)
    without = await zm.get_event_summary(monitor.id, include_archived=False)
    assert with_archived is not None and without is not None
    for field in ("hour_events", "day_events", "week_events", "month_events"):
        assert getattr(without, field) <= getattr(with_archived, field), field
    assert without.total_events == (
        with_archived.total_events - with_archived.archived_events
    )


async def test_snapshot(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """A capturing monitor's snapshot is a JPEG."""
    image = await zm.async_get_snapshot(monitor.id)
    assert is_jpeg(image), f"not a JPEG: {image[:40] if image else image!r}"


@v3_only
async def test_live_hls(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Live video: the HLS playlist is served through the media proxy."""
    path = zm.get_live_media_path(monitor.id)
    assert path
    await zm.async_prepare_live(monitor.id)

    async def playlist() -> str | None:
        async with zm.async_stream_media(path) as response:
            return await response.text() if response.status < 400 else None

    body = await until("HLS playlist", playlist, timeout=30)
    assert body.startswith("#EXTM3U"), body[:80]


@v2_only
async def test_live_mjpeg(
    zm: ZoneMinderClient, monitor: Monitor, live_http: aiohttp.ClientSession
) -> None:
    """Live video: the stream URL handed to players serves JPEG frames."""
    source = await zm.get_stream_source(monitor.id)
    assert source
    if source.startswith("rtsp://"):
        assert zm.get_stream_type(monitor.id) == STREAM_TYPE_GO2RTC
        return
    head = b""
    async with live_http.get(
        source, timeout=aiohttp.ClientTimeout(total=20)
    ) as response:
        assert response.status == 200
        while len(head) < 65536 and b"\xff\xd8" not in head:
            chunk = await response.content.read(4096)
            if not chunk:
                break
            head += chunk
    assert b"\xff\xd8" in head, f"no JPEG in the stream: {head[:80]!r}"


async def test_run_states(zm: ZoneMinderClient, monitors: list[Monitor]) -> None:
    """Run states are listed, at most one active, with definitions that parse."""
    states = await zm.get_run_states()
    names = [s.name for s in states]
    assert len(names) == len(set(names)), names
    assert sum(s.active for s in states) <= 1, [s.name for s in states if s.active]
    for state in states:
        for capturing, analysing, recording in state.monitor_modes().values():
            assert capturing in CAPTURING_OPTIONS, (state.name, capturing)
            assert analysing in ANALYSING_OPTIONS, (state.name, analysing)
            assert recording in RECORDING_OPTIONS, (state.name, recording)


async def test_events(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Events come newest first, and one read singly is the same event."""
    events = await zm.get_events(
        monitor_id=monitor.id, limit=10
    ) or await zm.get_events(limit=10)
    if not events:
        pytest.skip("no events on the server")
    starts = [e.start_time for e in events]
    assert starts == sorted(starts, reverse=True)
    one = await zm.get_event(events[0].id)
    assert (one.id, one.monitor_id) == (events[0].id, events[0].monitor_id)


@v3_only
async def test_event_media(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """A recorded event's video plays through the media proxy."""
    events = await zm.get_events(monitor_id=monitor.id, limit=25)
    events += await zm.get_events(limit=25)
    event = next((e for e in events if e.has_video), None)
    if event is None:
        pytest.skip("no finished event with video")
    path = zm.get_event_media_path(event.id)
    assert path
    async with zm.async_stream_media(
        path, headers={"Range": "bytes=0-1023"}
    ) as response:
        head = await response.read()
    assert response.status in (200, 206), response.status
    assert b"ftyp" in head[:64], f"not an MP4: {head[:40]!r}"


async def _event_count(zm: ZoneMinderClient, monitor_id: int | None) -> int | None:
    """Ask the API itself how many events match, or None if it doesn't say."""
    if zm.backend == BACKEND_LEGACY:
        endpoint = (
            "api/events.json"
            if monitor_id is None
            else f"api/events/index/MonitorId:{monitor_id}.json"
        )
        response = await zm._request("GET", endpoint, params={"limit": 1})  # type: ignore[attr-defined]
        count = (response.get("pagination") or {}).get("count")
        return None if count is None else int(count)
    params = {"page": 1, "page_size": 1}
    if monitor_id is not None:
        params["monitor_id"] = monitor_id
    response = await zm._request("GET", "api/v3/events", params=params)  # type: ignore[attr-defined]
    last_page = response.get("last_page")
    return None if last_page is None else int(last_page)


async def test_events_past_100(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Asking for 150 events gets them all, up to 150, when there are over 100.

    ZoneMinder's API v2 gives at most 100 events a page; the media browser and
    timeline ask for more.
    """
    for monitor_id in (monitor.id, None):
        count = await _event_count(zm, monitor_id)
        if count is not None and count > 100:
            break
    else:
        pytest.skip("neither the monitor nor the server has more than 100 events")
    events = await zm.get_events(monitor_id=monitor_id, limit=150)
    # Events recorded since the count can only add to it.
    want = min(count, 150)
    assert want <= len(events) <= 150, (
        f"get_events(limit=150) returned {len(events)} of {count}"
    )
    assert len({e.id for e in events}) == len(events), "an event was returned twice"
    starts = [e.start_time for e in events]
    assert starts == sorted(starts, reverse=True)


async def test_deleted_monitors_hidden(
    zm: ZoneMinderClient, monitors: list[Monitor]
) -> None:
    """A deleted monitor isn't listed, so it doesn't linger as a dead camera.

    Only checkable when the server has a deleted monitor; skipped otherwise.
    """
    if zm.backend == BACKEND_LEGACY:
        response = await zm._request("GET", "api/monitors.json")  # type: ignore[attr-defined]
        raw = [item.get("Monitor", item) for item in response.get("monitors") or []]
        deleted = {int(m["Id"]) for m in raw if m.get("Deleted") in (True, 1, "1")}
    else:
        items = await zm._paginate("api/v3/monitors")  # type: ignore[attr-defined]
        deleted = {int(m["id"]) for m in items if m.get("deleted")}
    if not deleted:
        pytest.skip("the server has no deleted monitor")
    listed = {m.id for m in await zm.get_monitors()}
    assert not deleted & listed, f"deleted monitors listed: {sorted(deleted & listed)}"


@v2_only
async def test_v2_event_times_any_zone(
    backend: str, zm: ZoneMinderClient, live_http: aiohttp.ClientSession
) -> None:
    """Event times are right even when the client's timezone isn't the server's.

    API v2 sends local times without a zone; Home Assistant passes its own,
    which often differs from the server's (a container on UTC, say).
    """
    events = await zm.get_events(limit=1)
    if not events:
        pytest.skip("no events on the server")
    raw = await zm._request(
        "GET", f"api/events/{events[0].id}.json", params={"noframes": "true"}
    )  # type: ignore[attr-defined]
    seconds = raw.get("event", raw).get("Event", {}).get("StartTimeSecs")
    if seconds is None:
        pytest.skip("this server sends no StartTimeSecs")
    truth = datetime.fromtimestamp(float(seconds), UTC)
    # Two zones 14 hours apart: at least one differs from the server's.
    for zone in (UTC, ZoneInfo("Pacific/Kiritimati")):
        other = client_for(backend, live_http, timezone=zone)
        await other.authenticate()
        event = await other.get_event(events[0].id)
        await other.close()
        off = (event.start_time - truth).total_seconds()
        assert abs(off) <= 1, (
            f"start time off by {off / 3600:+.1f} h with a client in {zone}"
        )


@v2_only
async def test_v2_event_without_frames(
    zm: ZoneMinderClient, monitor: Monitor, requests_seen: list[str]
) -> None:
    """Reading one event doesn't download all of its frames (slow for long events)."""
    events = await zm.get_events(monitor_id=monitor.id, limit=1) or await zm.get_events(
        limit=1
    )
    if not events:
        pytest.skip("no events on the server")
    requests_seen.clear()
    await zm.get_event(events[0].id)
    asked = [u for u in requests_seen if f"/events/{events[0].id}." in u]
    assert asked, requests_seen
    assert all("noframes=true" in u for u in asked), asked[0].split("token=")[0]


@v2_only
async def test_v2_snapshot_dead_token(
    backend: str,
    monitor: Monitor,
    live_http: aiohttp.ClientSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A snapshot with an expired token logs in again, and no token is logged."""
    zm = client_for(backend, live_http)
    await zm.authenticate()
    dead = "aiozm-live-dead-token"
    zm._auth_token = dead  # type: ignore[attr-defined]
    with caplog.at_level(logging.DEBUG, logger="aiozoneminder"):
        image = await zm.async_get_snapshot(monitor.id)
    await zm.close()
    leaked = [r.getMessage() for r in caplog.records if dead in r.getMessage()]
    assert not leaked, f"token in the log: {leaked[0][:120]}"
    assert is_jpeg(image), "no snapshot after the token expired"


async def test_restricted_user(
    backend: str, zm: ZoneMinderClient, live_http: aiohttp.ClientSession
) -> None:
    """A user without System view gets refusals, not a reauth loop or false 'stopped'.

    Needs ZM_LIVE_RESTRICTED_USER and ZM_LIVE_RESTRICTED_PASSWORD.
    """
    if not CONFIG.restricted_user:
        pytest.skip("set ZM_LIVE_RESTRICTED_USER and ZM_LIVE_RESTRICTED_PASSWORD")
    user = client_for(
        backend,
        live_http,
        user=CONFIG.restricted_user,
        password=CONFIG.restricted_password,
    )
    await user.authenticate()
    try:
        for call in (user.get_configs, user.get_run_states):
            try:
                await call()
            except ZMAuthenticationError as err:
                pytest.fail(
                    f"{call.__name__}: a refusal raised ZMAuthenticationError ({err})"
                )
            except (ZMApiError, ZMNotSupportedError):
                pass
        info = await user.get_server_info()
        admin = await zm.get_server_info()
        if admin.running:
            assert info.running is not False, (
                "running reported False for a user who can't read it"
            )
    finally:
        await user.close()


@v3_only
async def test_live_hls_after_idle(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Live video plays again after nobody watched it for a while (takes 2 minutes).

    zm-api ends a live session nobody has read for 90 seconds.
    """
    path = zm.get_live_media_path(monitor.id)
    assert path
    # Start from a fresh session, as a new viewer of a quiet camera would.
    await zm.async_stop_live(monitor.id)  # type: ignore[attr-defined]
    await zm.async_prepare_live(monitor.id)

    async def status() -> int | None:
        async with zm.async_stream_media(path) as response:
            return response.status if response.status < 400 else None

    await until("live playlist", status, timeout=30)
    await asyncio.sleep(110)
    await zm.async_prepare_live(monitor.id)  # what a player asks for a new viewer
    async with zm.async_stream_media(path) as response:
        assert response.status < 400, f"playlist {response.status} after 110 s idle"
