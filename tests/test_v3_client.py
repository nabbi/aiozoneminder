"""Tests for the API v3 (zm-api) client."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

import aiohttp
from aioresponses import CallbackResult, aioresponses
import pytest
from yarl import URL

from aiozoneminder import (
    RunState,
    ZMApiError,
    ZMAuthenticationError,
    ZMConnectionError,
    ZMNotFoundError,
    ZMNotSupportedError,
    ZoneMinderV3Client,
)

from .conftest import register_login, url
from .fixtures import HOST, LIVE_PLAYLIST, RUN_STATES, monitor, paginated


def calls(mocked: aioresponses, method: str, path: str) -> list[Any]:
    """Return the recorded calls to one endpoint, whatever their query."""
    found = []
    for (verb, called), requests in mocked.requests.items():
        if verb == method and str(called).split("?")[0] == f"{HOST}/{path}":
            found.extend(requests)
    return found


def bearer(call: Any) -> str | None:
    """Return the Authorization header a recorded call carried."""
    return (call.kwargs.get("headers") or {}).get("Authorization")


def register_fresh_token(mocked: aioresponses) -> None:
    """Answer a token refresh with a new access token."""
    mocked.post(
        f"{HOST}/api/v3/auth/refresh",
        payload={"access_token": "fresh-token", "expire_in": 600},
    )


# -- Logging in ---------------------------------------------------------------


async def test_authenticate_stores_the_token(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    assert await client.authenticate() is True
    assert client.authenticated
    assert client.access_token == "access-token"
    (login,) = calls(mocked, "POST", "api/v3/auth/login")
    assert login.kwargs["json"] == {"username": "admin", "password": "hunter2"}


@pytest.mark.parametrize("status", [400, 401, 403])
async def test_bad_credentials(
    client: ZoneMinderV3Client, mocked: aioresponses, status: int
) -> None:
    mocked.post(f"{HOST}/api/v3/auth/login", status=status)
    with pytest.raises(ZMAuthenticationError):
        await client.authenticate()


async def test_no_zm_api_suggests_api_v2(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    mocked.post(f"{HOST}/api/v3/auth/login", status=404)
    with pytest.raises(ZMConnectionError, match="API v2"):
        await client.authenticate()


async def test_unreachable_server(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    mocked.post(
        f"{HOST}/api/v3/auth/login", exception=aiohttp.ClientConnectionError("down")
    )
    with pytest.raises(ZMConnectionError):
        await client.authenticate()


async def test_login_without_a_token_is_refused(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    mocked.post(f"{HOST}/api/v3/auth/login", payload={"token_type": "Bearer"})
    with pytest.raises(ZMAuthenticationError):
        await client.authenticate()


async def test_first_request_logs_in(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    assert await client.get_version() == "1.39.22"
    (version,) = calls(zm_api, "GET", "api/v3/host/getVersion")
    assert bearer(version) == "Bearer access-token"


async def test_expired_token_is_refreshed_and_retried(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), status=401)
    mocked.post(
        f"{HOST}/api/v3/auth/refresh",
        payload={"access_token": "fresh-token", "expire_in": 600},
    )
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})

    assert await client.get_version() == "1.39.41"
    (refresh,) = calls(mocked, "POST", "api/v3/auth/refresh")
    assert refresh.kwargs["json"] == {"token": "refresh-token"}
    assert bearer(calls(mocked, "GET", "api/v3/host/getVersion")[-1]) == (
        "Bearer fresh-token"
    )


async def test_failed_refresh_logs_in_again(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), status=401)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401)
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})

    assert await client.get_version() == "1.39.41"
    assert len(calls(mocked, "POST", "api/v3/auth/login")) == 2


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the client's sleeps instead of waiting."""
    delays: list[float] = []

    async def _sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("aiozoneminder.client.v3.asyncio.sleep", _sleep)
    return delays


async def test_three_rejections_give_up(
    client: ZoneMinderV3Client, mocked: aioresponses, slept: list[float]
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), status=401, repeat=True)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401, repeat=True)
    with pytest.raises(ZMAuthenticationError):
        await client.get_version()
    assert len(calls(mocked, "GET", "api/v3/host/getVersion")) == 3
    assert slept == [1.0]


async def test_token_renewed_in_the_logout_second_is_renewed_again(
    client: ZoneMinderV3Client, mocked: aioresponses, slept: list[float]
) -> None:
    # A logout elsewhere revokes tokens issued before the next second, so the
    # token from the first re-login is refused too.
    register_login(mocked)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401, repeat=True)
    mocked.get(url("api/v3/host/getVersion"), status=401)
    mocked.get(url("api/v3/host/getVersion"), status=401)
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})

    assert await client.get_version() == "1.39.41"
    assert slept == [1.0]
    # The first login, one after each refusal.
    assert len(calls(mocked, "POST", "api/v3/auth/login")) == 3


async def test_concurrent_refusals_share_one_renewal(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # Ten requests refused at once (a revoked token) renew it once, not ten
    # times: zm-api's /auth limiter answers a burst of logins with 429.
    register_login(mocked)
    await client.authenticate()
    mocked.post(
        f"{HOST}/api/v3/auth/refresh",
        payload={"access_token": "fresh-token", "expire_in": 600},
        repeat=True,
    )

    refused: list[int] = []
    all_in = asyncio.Event()

    async def by_token(_url: Any, **kwargs: Any) -> CallbackResult:
        # The revoked token is refused, once all ten carry it (so they really
        # overlap); the renewed one works.
        if kwargs["headers"]["Authorization"] == "Bearer access-token":
            refused.append(1)
            if len(refused) == 10:
                all_in.set()
            await asyncio.wait_for(all_in.wait(), timeout=5)
            return CallbackResult(status=401)
        return CallbackResult(payload={"version": "1.39.41"})

    mocked.get(url("api/v3/host/getVersion"), callback=by_token, repeat=True)

    versions = await asyncio.gather(*(client.get_version() for _ in range(10)))

    assert versions == ["1.39.41"] * 10
    assert len(calls(mocked, "POST", "api/v3/auth/refresh")) == 1
    assert len(calls(mocked, "POST", "api/v3/auth/login")) == 1  # the first login only


async def test_concurrent_refusals_share_a_failed_renewal(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # When the shared renewal fails (zm-api's /auth limiter answers 429), the
    # requests waiting on it get its error instead of each logging in again.
    mocked.post(
        f"{HOST}/api/v3/auth/login",
        payload={"access_token": "access-token", "refresh_token": "refresh-token"},
    )
    await client.authenticate()
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=429, repeat=True)
    mocked.post(f"{HOST}/api/v3/auth/login", status=429, repeat=True)

    refused: list[int] = []
    all_in = asyncio.Event()

    async def all_refused(_url: Any, **kwargs: Any) -> CallbackResult:
        refused.append(1)
        if len(refused) == 10:
            all_in.set()
        await asyncio.wait_for(all_in.wait(), timeout=5)
        return CallbackResult(status=401)

    mocked.get(url("api/v3/host/getVersion"), callback=all_refused, repeat=True)

    results = await asyncio.gather(
        *(client.get_version() for _ in range(10)), return_exceptions=True
    )

    assert all(isinstance(result, ZMConnectionError) for result in results)
    assert len(calls(mocked, "POST", "api/v3/auth/refresh")) == 1
    # The first login, then one for the whole burst.
    assert len(calls(mocked, "POST", "api/v3/auth/login")) == 2

    # A request sent after the failure tries again rather than reusing it.
    with pytest.raises(ZMConnectionError):
        await client.get_version()
    assert len(calls(mocked, "POST", "api/v3/auth/login")) == 3


async def test_token_near_expiry_is_refreshed_first(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # Expiring within the 60 second margin counts as expired.
    register_login(mocked, expire_in=30)
    mocked.post(
        f"{HOST}/api/v3/auth/refresh",
        payload={"access_token": "fresh-token", "expire_in": 600},
    )
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})

    await client.authenticate()
    await client.get_version()
    assert len(calls(mocked, "POST", "api/v3/auth/refresh")) == 1
    assert client.access_token == "fresh-token"


async def test_close_forgets_the_session(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    await client.authenticate()
    await client.close()
    assert not client.authenticated
    assert client.access_token is None


# -- Errors -------------------------------------------------------------------


async def test_not_found(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/9"), status=404)
    with pytest.raises(ZMNotFoundError):
        await client.get_monitor(9)


async def test_server_error(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/1"), status=500)
    with pytest.raises(ZMApiError, match="500"):
        await client.get_monitor(1)


async def test_dropped_connection(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/1"), exception=aiohttp.ServerDisconnectedError())
    with pytest.raises(ZMConnectionError):
        await client.get_monitor(1)


# -- Server -------------------------------------------------------------------


async def test_server_info(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    info = await client.get_server_info()
    assert info.version == "1.39.22"
    assert info.api_version == "3.0.0-alpha.1"
    assert info.running is True
    assert (info.daemons_running, info.daemons_total) == (2, 3)
    assert info.total_disk_space == 51762574243


async def test_server_info_survives_missing_extras(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})
    mocked.get(url("api/v3/system/status"), status=500)
    mocked.get(url("api/v3/storage"), status=404)

    info = await client.get_server_info()
    assert info.version == "1.39.41"
    assert info.running is None
    assert info.daemons_total == 0
    assert info.storage == []


async def test_server_info_without_system_view(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # Users without the System view permission may not read daemon status.
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})
    mocked.get(url("api/v3/system/status"), status=403)
    mocked.get(url("api/v3/storage"), payload=paginated([]))

    info = await client.get_server_info()
    assert info.running is None
    assert (info.daemons_running, info.daemons_total) == (0, 0)


async def test_server_info_stopped(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/host/getVersion"), payload={"version": "1.39.41"})
    mocked.get(url("api/v3/system/status"), payload={"running": False, "daemons": []})
    mocked.get(url("api/v3/storage"), payload=paginated([]))

    assert (await client.get_server_info()).running is False


async def test_configs(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    configs = await client.get_configs()
    assert configs["ZM_MQTT_TOPIC_PREFIX"] == "ZoneMinder"
    assert configs["ZM_MQTT_HOSTNAME"] == "192.168.0.66"


# -- Monitors -----------------------------------------------------------------


async def test_monitors(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    monitors = await client.get_monitors()
    assert [m.name for m in monitors] == ["Driveway", "Back door", "Gate"]
    assert monitors[2].controllable is True


async def test_every_page_is_read(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    first = {**paginated([monitor(1, "One")]), "last_page": 2}
    second = {**paginated([monitor(2, "Two")]), "current_page": 2, "last_page": 2}
    mocked.get(url("api/v3/monitors"), payload=first)
    mocked.get(url("api/v3/monitors"), payload=second)

    assert [m.name for m in await client.get_monitors()] == ["One", "Two"]
    pages = [
        c.kwargs["params"]["page"] for c in calls(mocked, "GET", "api/v3/monitors")
    ]
    assert pages == [1, 2]


async def test_deleted_and_broken_monitors_are_skipped(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    deleted = {**monitor(2, "Gone"), "deleted": True}
    broken = {"id": "not a number"}
    mocked.get(
        url("api/v3/monitors"),
        payload=paginated([monitor(1, "Kept"), deleted, broken]),
    )
    assert [m.name for m in await client.get_monitors()] == ["Kept"]


async def test_monitor_statuses(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    statuses = await client.get_monitor_statuses()
    assert set(statuses) == {1, 2, 3}
    assert statuses[2].analysis_fps == pytest.approx(9.8)


async def test_monitor_statuses_unavailable(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitor-status"), status=404)
    assert await client.get_monitor_statuses() == {}


async def test_set_monitor_state_sends_only_what_changes(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.patch(url("api/v3/monitors/2"), payload={})

    await client.set_monitor_state(2, analysing="Always", recording="OnMotion")
    await client.set_monitor_state(2)  # Nothing to change: no request.

    (patch,) = calls(mocked, "PATCH", "api/v3/monitors/2")
    assert patch.kwargs["json"] == {"analysing": "Always", "recording": "OnMotion"}


# -- Events -------------------------------------------------------------------


async def test_events(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    events = await client.get_events(monitor_id=2, limit=10)
    assert [e.id for e in events] == [298, 297]
    params = calls(zm_api, "GET", "api/v3/events")[0].kwargs["params"]
    assert params["sort"] == "start_time"
    assert params["direction"] == "desc"
    assert params["page_size"] == 10


async def test_events_between_sends_utc(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    start = datetime(2026, 8, 22, 23, 0, tzinfo=UTC)
    end = datetime(2026, 8, 23, 1, 0, tzinfo=UTC)
    await client.get_events_between(1, start, end)
    params = calls(zm_api, "GET", "api/v3/events")[0].kwargs["params"]
    assert params["start_time"] == "2026-08-22T23:00:00Z"
    assert params["monitor_id"] == 1


async def test_events_between_include_the_running_event(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    start = datetime(2026, 8, 22, tzinfo=UTC)
    end = datetime(2026, 8, 23, tzinfo=UTC)
    events = await client.get_events_between(1, start, end)
    # 300 is still recording: zm-api's end filter would drop it.
    assert [e.id for e in events] == [300, 299]
    assert "end_time" not in calls(zm_api, "GET", "api/v3/events")[0].kwargs["params"]


async def test_events_between_stop_at_the_window_end(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    from .fixtures import event

    register_login(mocked)
    days = [f"2026-08-{day:02d}T12:00:00Z" for day in range(20, 26)]
    # Read oldest first, one page per event, so later days are never fetched.
    pages = [paginated([event(i, 1, start=day)]) for i, day in enumerate(days)]
    for page in pages:
        page["last_page"] = len(days)
    for page in pages:
        mocked.get(url("api/v3/events"), payload=page)

    events = await client.get_events_between(
        1,
        datetime(2026, 8, 20, tzinfo=UTC),
        datetime(2026, 8, 22, tzinfo=UTC),
        limit=1,
    )

    # Newest first, at most limit of them.
    assert [e.id for e in events] == [1]
    requests = calls(mocked, "GET", "api/v3/events")
    assert [r.kwargs["params"]["page"] for r in requests] == [1, 2, 3]
    assert requests[0].kwargs["params"]["direction"] == "asc"


async def test_one_event(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    from .fixtures import event

    register_login(mocked)
    mocked.get(url("api/v3/events/298"), payload=event(298, 2, alarm_frames=3))
    found = await client.get_event(298)
    assert found.monitor_id == 2
    assert found.is_alarm


async def test_event_summary(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    summary = await client.get_event_summary(1)
    assert summary is not None
    assert summary.day_events == 85


async def test_camera_without_events_has_a_zero_summary(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/event-summaries/4"), status=404)
    summary = await client.get_event_summary(4)
    assert summary is not None
    assert summary.total_events == 0


async def test_event_summary_failure(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/event-summaries/1"), status=500)
    assert await client.get_event_summary(1) is None


async def test_event_summary_without_archived(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    """zm-api can't leave archived events out (zm-api#205), so they're taken off."""
    from .fixtures import EVENT_SUMMARY, event

    register_login(mocked)
    mocked.get(url("api/v3/event-summaries/1"), payload=EVENT_SUMMARY)
    started = datetime.now(UTC) - timedelta(hours=2)
    mocked.get(
        url("api/v3/events"),
        payload={
            "items": [event(7, 1, start=started.strftime("%Y-%m-%dT%H:%M:%SZ"))],
            "last_page": 1,
        },
    )
    summary = await client.get_event_summary(1, include_archived=False)

    assert summary is not None
    # Fixture: hour 6, day 85, week 86, month 86, total 126, one archived.
    assert (summary.hour_events, summary.day_events) == (6, 84)
    assert (summary.week_events, summary.month_events) == (85, 85)
    assert (summary.total_events, summary.archived_events) == (125, 1)
    (request,) = [
        call
        for (verb, called), calls in mocked.requests.items()
        if verb == "GET" and called.path == "/api/v3/events"
        for call in calls
    ]
    assert request.kwargs["params"]["archived"] == "true"
    assert request.kwargs["params"]["monitor_id"] == 1


async def test_event_summary_without_archived_none_archived(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    """With nothing archived there is nothing to look up."""
    from .fixtures import EVENT_SUMMARY

    register_login(mocked)
    mocked.get(
        url("api/v3/event-summaries/1"),
        payload={**EVENT_SUMMARY, "archived_events": 0},
    )
    summary = await client.get_event_summary(1, include_archived=False)
    assert summary is not None and summary.day_events == 85
    assert not [key for key in mocked.requests if key[1].path == "/api/v3/events"]


# -- Run states ---------------------------------------------------------------


async def test_run_states(client: ZoneMinderV3Client, zm_api: aioresponses) -> None:
    states = await client.get_run_states()
    assert [s.name for s in states] == ["default", "Home", "Away"]
    assert next(s for s in states if s.active).name == "Home"


async def test_saving_a_new_run_state_creates_it(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    zm_api.post(url("api/v3/states"), payload={})
    await client.save_run_state("Holiday", "1:Always:Always:Always")
    (post,) = calls(zm_api, "POST", "api/v3/states")
    # zm-api's CreateStateRequest has no default for is_active: without it
    # the create answers 422.
    assert post.kwargs["json"] == {
        "name": "Holiday",
        "definition": "1:Always:Always:Always",
        "is_active": 0,
    }


async def test_saving_an_existing_run_state_overwrites_it(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    zm_api.patch(url("api/v3/states/3"), payload={})
    await client.save_run_state("Away", "1:Always:None:None")
    (patch,) = calls(zm_api, "PATCH", "api/v3/states/3")
    assert patch.kwargs["json"] == {"definition": "1:Always:None:None"}


async def test_switching_run_state(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    for monitor_id in (1, 2, 3):
        zm_api.patch(url(f"api/v3/monitors/{monitor_id}"), payload={})
    for state_id in (2, 3):
        zm_api.patch(url(f"api/v3/states/{state_id}"), payload={})

    away = RunState.from_dict(RUN_STATES[2])
    await client.set_run_state(away)

    # Cameras 1 and 3 differ from Away. Camera 2 already matches, so it's
    # left alone.
    assert calls(zm_api, "PATCH", "api/v3/monitors/1")[0].kwargs["json"] == {
        "capturing": "Always",
        "analysing": "Always",
        "recording": "Always",
    }
    assert len(calls(zm_api, "PATCH", "api/v3/monitors/3")) == 1
    assert calls(zm_api, "PATCH", "api/v3/monitors/2") == []
    # Home stops being active and Away starts. "default" was already off.
    assert calls(zm_api, "PATCH", "api/v3/states/2")[0].kwargs["json"] == {
        "is_active": 0
    }
    assert calls(zm_api, "PATCH", "api/v3/states/3")[0].kwargs["json"] == {
        "is_active": 1
    }
    assert calls(zm_api, "PATCH", "api/v3/states/1") == []


# -- Alarms and PTZ -----------------------------------------------------------


async def test_alarms(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.patch(url("api/v3/monitors/1/alarm"), payload={}, repeat=True)

    await client.trigger_alarm(1, "Doorbell")
    await client.cancel_alarm(1)

    on, off = calls(mocked, "PATCH", "api/v3/monitors/1/alarm")
    assert on.kwargs["json"] == {"action": "on", "cause": "Doorbell"}
    assert off.kwargs["json"] == {"action": "off"}


async def test_alarm_state(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.get(
        url("api/v3/monitor-status/1"),
        payload={"monitor_id": 1, "status": "Connected", "alarm_state": "alarm"},
    )
    # zm-api leaves the state out while zmc isn't running.
    mocked.get(
        url("api/v3/monitor-status/2"),
        payload={"monitor_id": 2, "status": "NotRunning"},
    )
    # A monitor that never started has no status row.
    mocked.get(url("api/v3/monitor-status/3"), status=404)

    assert await client.get_alarm_state(1) == "alarm"
    assert await client.get_alarm_state(2) is None
    assert await client.get_alarm_state(3) is None


async def test_long_alarm_cause_is_cut_to_fit(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.patch(url("api/v3/monitors/1/alarm"), payload={}, repeat=True)

    await client.trigger_alarm(1, "Front door opened by the postman again")
    # Multi-byte characters are not split.
    await client.trigger_alarm(1, "Klingel an der Gartentür gedrückt")

    first, second = calls(mocked, "PATCH", "api/v3/monitors/1/alarm")
    assert first.kwargs["json"]["cause"] == "Front door opened by the postma"
    assert second.kwargs["json"]["cause"] == "Klingel an der Gartentür gedr"


async def test_refusal_carries_the_reason(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.patch(
        url("api/v3/monitors/1/alarm"),
        status=400,
        payload={
            "kind": "INVALID_INPUT_ERROR",
            "error_message": "cause: length is greater than 31\n",
            "code": None,
            "details": [["cause", "length is greater than 31"]],
        },
    )
    with pytest.raises(ZMApiError, match=r"400: cause: length is greater than 31$"):
        await client.trigger_alarm(1, "Doorbell")


@pytest.mark.parametrize(
    ("command", "kwargs", "path", "body"),
    [
        # Control modules treat a missing speed as 0, which stops the camera,
        # so every move carries both speeds and zoom/focus carry theirs.
        ("up-left", {}, "move/up-left", {"pan_speed": 50, "tilt_speed": 50}),
        ("down", {"speed": 80}, "move/down", {"pan_speed": 80, "tilt_speed": 80}),
        ("stop", {}, "move/stop", {"pan_speed": 50, "tilt_speed": 50}),
        ("zoom-in", {}, "zoom/in", {"speed": 50}),
        ("focus-auto", {}, "focus/auto", {"speed": 50}),
        ("home", {}, "home", {}),
        ("preset", {"preset": 2}, "presets/2/goto", {}),
    ],
)
async def test_ptz_commands(
    client: ZoneMinderV3Client,
    mocked: aioresponses,
    *,
    command: str,
    kwargs: dict[str, Any],
    path: str,
    body: dict[str, Any],
) -> None:
    register_login(mocked)
    mocked.post(url(f"api/v3/ptz/monitors/3/{path}"), payload={})
    await client.ptz(3, command, **kwargs)
    (call,) = calls(mocked, "POST", f"api/v3/ptz/monitors/3/{path}")
    assert call.kwargs["json"] == body


async def test_ptz_preset_needs_a_number(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    with pytest.raises(ZMApiError, match="preset number"):
        await client.ptz(3, "preset")


async def test_unknown_ptz_command(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    with pytest.raises(ZMNotSupportedError):
        await client.ptz(3, "spin")


async def test_ptz_presets_are_for_one_camera(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(
        url("api/v3/control_presets"),
        payload=paginated(
            [
                {"monitor_id": 3, "preset": 1, "label": "Front gate"},
                {"monitor_id": 3, "preset": 2, "label": None},
                {"monitor_id": 4, "preset": 1, "label": "Elsewhere"},
            ]
        ),
    )
    assert await client.get_ptz_presets(3) == {1: "Front gate", 2: "Preset 2"}


async def test_ptz_presets_unavailable(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/control_presets"), status=404)
    assert await client.get_ptz_presets(3) == {}


# -- Event stream -------------------------------------------------------------

STREAM = (
    ": connected\n"
    "\n"
    "id: 300\n"
    "event: event_start\n"
    'data: {"type": "event_start", "event_id": 300, "monitor_id": 1}\n'
    "\n"
    ": heartbeat\n"
    "\n"
    "event: alarm_state\n"
    'data: {"monitor_id": 2,\n'
    'data:  "state": "alarm"}\n'
    "\n"
    "data: not json\n"
    "\n"
)


async def test_event_stream(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/events/stream"), body=STREAM)

    received = [message async for message in client.async_event_stream(since=299)]

    assert received == [
        ("event_start", {"type": "event_start", "event_id": 300, "monitor_id": 1}),
        # No "type" in the data, so the event: line names it.
        ("alarm_state", {"monitor_id": 2, "state": "alarm"}),
    ]
    (request,) = calls(mocked, "GET", "api/v3/events/stream")
    assert request.kwargs["params"] == {"since": "299"}
    assert request.kwargs["headers"]["Accept"] == "text/event-stream"
    assert bearer(request) == "Bearer access-token"


@pytest.mark.parametrize("status", [400, 404, 405, 422])
async def test_server_without_an_event_stream(
    client: ZoneMinderV3Client, mocked: aioresponses, status: int
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/events/stream"), status=status)
    with pytest.raises(ZMNotFoundError):
        async for _message in client.async_event_stream():
            pass


async def test_event_stream_refused(
    client: ZoneMinderV3Client, mocked: aioresponses, slept: list[float]
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/events/stream"), status=401, repeat=True)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401, repeat=True)
    with pytest.raises(ZMAuthenticationError):
        async for _message in client.async_event_stream():
            pass
    # Renewed, asked again, then renewed once more after the logout second.
    assert len(calls(mocked, "GET", "api/v3/events/stream")) == 3
    assert slept == [1.0]


async def test_event_stream_after_the_token_is_revoked(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    register_fresh_token(mocked)
    mocked.get(url("api/v3/events/stream"), status=401)
    mocked.get(url("api/v3/events/stream"), body=STREAM)

    received = [message async for message in client.async_event_stream()]

    assert len(received) == 2
    assert bearer(calls(mocked, "GET", "api/v3/events/stream")[-1]) == (
        "Bearer fresh-token"
    )


# -- WebRTC (WHEP) ------------------------------------------------------------

WHEP = "api/v3/live/1/webrtc/whep"
SESSION = "/api/v3/live/1/webrtc/whep/abc123"


@pytest.mark.parametrize(
    ("status", "supported"), [(415, True), (400, True), (404, False)]
)
async def test_whep_probe(
    client: ZoneMinderV3Client, mocked: aioresponses, status: int, supported: bool
) -> None:
    register_login(mocked)
    mocked.post(url(WHEP), status=status)
    assert await client.async_supports_whep(1) is supported
    # The answer is remembered.
    assert await client.async_supports_whep(1) is supported
    assert len(calls(mocked, "POST", WHEP)) == 1


async def test_whep_probe_unreachable(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.post(url(WHEP), exception=aiohttp.ClientConnectionError())
    assert await client.async_supports_whep(1) is False


async def test_whep_offer(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.post(
        url(WHEP),
        status=201,
        body="v=0\r\nanswer",
        headers={"Location": SESSION},
        content_type="application/sdp",
    )

    answer, location = await client.async_whep_offer(1, "v=0\r\noffer")

    assert answer == "v=0\r\nanswer"
    assert location == SESSION
    (offer,) = calls(mocked, "POST", WHEP)
    assert offer.kwargs["data"] == b"v=0\r\noffer"
    assert offer.kwargs["headers"]["Content-Type"] == "application/sdp"


@pytest.mark.parametrize(
    ("status", "message"),
    [(406, "codec"), (503, "session limit"), (418, "teapot")],
)
async def test_whep_offer_refused(
    client: ZoneMinderV3Client, mocked: aioresponses, status: int, message: str
) -> None:
    register_login(mocked)
    mocked.post(url(WHEP), status=status, body="I'm a teapot")
    with pytest.raises(ZMApiError, match=message):
        await client.async_whep_offer(1, "v=0")


async def test_whep_after_the_token_is_revoked(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    register_fresh_token(mocked)
    mocked.post(url(WHEP), status=401)
    mocked.post(url(WHEP), status=201, body="v=0", headers={"Location": SESSION})
    mocked.patch(f"{HOST}{SESSION}", status=401)
    mocked.patch(f"{HOST}{SESSION}", status=204)

    assert await client.async_whep_offer(1, "v=0") == ("v=0", SESSION)
    await client.async_whep_candidate(SESSION, "0", "candidate:1")

    assert bearer(calls(mocked, "POST", WHEP)[-1]) == "Bearer fresh-token"
    assert len(calls(mocked, "PATCH", SESSION.lstrip("/"))) == 2


async def test_whep_answer_without_a_session(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.post(url(WHEP), status=201, body="v=0")
    with pytest.raises(ZMApiError, match="location"):
        await client.async_whep_offer(1, "v=0")


async def test_whep_offer_unreachable(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.post(url(WHEP), exception=aiohttp.ClientConnectionError())
    with pytest.raises(ZMConnectionError):
        await client.async_whep_offer(1, "v=0")


async def test_whep_candidate(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.patch(f"{HOST}{SESSION}", status=204, repeat=True)

    await client.async_whep_candidate(
        SESSION, "0", "candidate:1 1 udp 1 10.0.0.2 5 typ host"
    )
    await client.async_whep_candidate(SESSION, None, "a=end-of-candidates")

    first, second = calls(mocked, "PATCH", SESSION.lstrip("/"))
    assert first.kwargs["data"] == (
        b"a=mid:0\r\na=candidate:1 1 udp 1 10.0.0.2 5 typ host\r\n"
    )
    assert first.kwargs["headers"]["Content-Type"] == "application/trickle-ice-sdpfrag"
    assert second.kwargs["data"] == b"a=end-of-candidates\r\n"


async def test_whep_candidate_unreachable(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.patch(f"{HOST}{SESSION}", exception=aiohttp.ClientConnectionError())
    with pytest.raises(ZMConnectionError):
        await client.async_whep_candidate(SESSION, "0", "candidate:1")


async def test_whep_session_at_an_absolute_url(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    elsewhere = "https://media.example/whep/abc123"
    mocked.patch(elsewhere, status=204)
    mocked.delete(elsewhere, status=200)

    await client.async_whep_candidate(elsewhere, "0", "candidate:1")
    await client.async_whep_close(elsewhere)

    assert ("PATCH", URL(elsewhere)) in mocked.requests
    assert ("DELETE", URL(elsewhere)) in mocked.requests


async def test_whep_close(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    await client.authenticate()
    mocked.delete(f"{HOST}{SESSION}", status=200)
    await client.async_whep_close(SESSION)
    (close,) = calls(mocked, "DELETE", SESSION.lstrip("/"))
    assert bearer(close) == "Bearer access-token"


async def test_whep_close_tolerates_a_dead_server(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    mocked.delete(f"{HOST}{SESSION}", exception=aiohttp.ClientConnectionError())
    await client.async_whep_close(SESSION)


# -- Live video and media -----------------------------------------------------


async def test_rtsp_link_is_reused(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    expires = datetime(2099, 1, 1, tzinfo=UTC).timestamp()
    mocked.post(
        url("api/v3/live/1/rtsp/link"),
        payload={"url": "rtsp://zm.local:8554/1?st=signed", "expires_at": expires},
    )

    first = await client.async_get_rtsp_url(1)
    second = await client.async_get_rtsp_url(1)

    assert first == second == "rtsp://zm.local:8554/1?st=signed"
    (request,) = calls(mocked, "POST", "api/v3/live/1/rtsp/link")
    assert request.kwargs["json"] == {"ttl_seconds": 7 * 24 * 3600}


async def test_rtsp_switched_off_is_not_asked_again_at_once(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.post(url(r"api/v3/live/\d+/rtsp/link"), status=503, repeat=True)

    assert await client.async_get_rtsp_url(1) is None
    assert await client.async_get_rtsp_url(2) is None
    assert len(calls(mocked, "POST", "api/v3/live/1/rtsp/link")) == 1
    assert calls(mocked, "POST", "api/v3/live/2/rtsp/link") == []


@pytest.mark.parametrize("status", [403, 404])
async def test_rtsp_refused_for_one_monitor_leaves_the_others(
    client: ZoneMinderV3Client, mocked: aioresponses, status: int
) -> None:
    # A monitor the user can't see (or that doesn't exist) is refused; that
    # says nothing about the others, unlike a 503 for the whole RTSP server.
    register_login(mocked)
    expires = datetime(2099, 1, 1, tzinfo=UTC).timestamp()
    mocked.post(url("api/v3/live/1/rtsp/link"), status=status, repeat=True)
    mocked.post(
        url("api/v3/live/2/rtsp/link"),
        payload={"url": "rtsp://zm.local:8554/2?st=signed", "expires_at": expires},
    )

    assert await client.async_get_rtsp_url(1) is None
    assert await client.async_get_rtsp_url(2) == "rtsp://zm.local:8554/2?st=signed"
    # Monitor 1 isn't asked again at once.
    assert await client.async_get_rtsp_url(1) is None
    assert len(calls(mocked, "POST", "api/v3/live/1/rtsp/link")) == 1


async def test_live_session_is_started_once(
    client: ZoneMinderV3Client, zm_api: aioresponses
) -> None:
    zm_api.post(url("api/v3/live/1/start"), payload={"status": "started"}, repeat=True)

    await client.async_prepare_live(1)
    await client.async_prepare_live(1)

    (start,) = calls(zm_api, "POST", "api/v3/live/1/start")
    assert start.kwargs["json"] == {"enable_hls": True, "enable_webrtc": True}


async def test_live_session_already_running_counts_as_started(
    client: ZoneMinderV3Client,
    zm_api: aioresponses,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # zm-api answers 409 when the monitor already has a session.
    zm_api.post(url("api/v3/live/1/start"), status=409, repeat=True)

    await client.async_prepare_live(1)
    await client.async_prepare_live(1)

    assert len(calls(zm_api, "POST", "api/v3/live/1/start")) == 1
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


async def test_reaped_live_session_is_started_again(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.post(url("api/v3/live/1/start"), payload={"status": "started"}, repeat=True)
    mocked.get(url(r"api/v3/live/1/hls/live\.m3u8"), body=LIVE_PLAYLIST, repeat=True)
    master = client.get_live_media_path(1)
    # zm-api reaped the idle session, so the playlist is gone until a restart.
    mocked.get(url(master), status=404)
    mocked.get(url(master), body="#EXTM3U\nlive.m3u8\n")

    await client.async_prepare_live(1)
    async with client.async_stream_media(master) as resp:
        assert resp.status == 200

    assert len(calls(mocked, "POST", "api/v3/live/1/start")) == 2
    assert len(calls(mocked, "GET", master)) == 2


async def test_live_restart_is_not_repeated_on_every_request(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # A monitor that isn't capturing: no session can start.
    register_login(mocked)
    mocked.post(url("api/v3/live/1/start"), status=404, repeat=True)
    master = client.get_live_media_path(1)
    mocked.get(url(master), status=404, repeat=True)

    for _ in range(3):
        async with client.async_stream_media(master) as resp:
            assert resp.status == 404

    assert len(calls(mocked, "POST", "api/v3/live/1/start")) == 1


async def test_missing_recording_does_not_start_a_live_session(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    path = client.get_event_media_path(298)
    mocked.get(url(path), status=404)

    async with client.async_stream_media(path) as resp:
        assert resp.status == 404
    assert calls(mocked, "POST", "api/v3/live/298/start") == []


async def test_snapshot(client: ZoneMinderV3Client, mocked: aioresponses) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/1/snapshot"), status=401)
    mocked.get(url("api/v3/monitors/1/snapshot"), body=b"\xff\xd8jpeg")
    assert await client.async_get_snapshot(1) == b"\xff\xd8jpeg"


async def test_snapshot_failure(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/1/snapshot"), status=500)
    assert await client.async_get_snapshot(1) is None


async def test_snapshot_of_a_monitor_not_capturing(
    client: ZoneMinderV3Client,
    mocked: aioresponses,
    caplog: pytest.LogCaptureFixture,
) -> None:
    register_login(mocked)
    mocked.get(url("api/v3/monitors/1/snapshot"), status=404)
    assert await client.async_get_snapshot(1) is None
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


async def test_media_is_streamed_with_the_token(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    register_login(mocked)
    path = client.get_event_media_path(298)
    mocked.get(url(path), body=b"mp4 bytes")

    async with client.async_stream_media(path, headers={"Range": "bytes=0-"}) as resp:
        assert await resp.read() == b"mp4 bytes"

    (request,) = calls(mocked, "GET", path)
    assert bearer(request) == "Bearer access-token"
    assert request.kwargs["headers"]["Range"] == "bytes=0-"


async def test_media_after_the_token_is_revoked(
    client: ZoneMinderV3Client, mocked: aioresponses
) -> None:
    # A logout elsewhere on the account revokes the token before it expires.
    register_login(mocked)
    register_fresh_token(mocked)
    path = client.get_event_media_path(298)
    mocked.get(url(path), status=401)
    mocked.get(url(path), body=b"mp4 bytes")

    async with client.async_stream_media(path, headers={"Range": "bytes=0-"}) as resp:
        assert resp.status == 200
        assert await resp.read() == b"mp4 bytes"

    first, second = calls(mocked, "GET", path)
    assert bearer(first) == "Bearer access-token"
    assert bearer(second) == "Bearer fresh-token"
    assert second.kwargs["headers"]["Range"] == "bytes=0-"


async def test_media_refused_twice_is_passed_on(
    client: ZoneMinderV3Client, mocked: aioresponses, slept: list[float]
) -> None:
    register_login(mocked)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401, repeat=True)
    path = client.get_event_media_path(298)
    mocked.get(url(path), status=401, repeat=True)

    async with client.async_stream_media(path) as resp:
        assert resp.status == 401
    assert len(calls(mocked, "GET", path)) == 3
    assert slept == [1.0]


async def test_media_in_the_logout_second_is_renewed_again(
    client: ZoneMinderV3Client, mocked: aioresponses, slept: list[float]
) -> None:
    # The token renewed in the logout's second is refused too; after the wait
    # a second renewal gets through.
    register_login(mocked)
    mocked.post(f"{HOST}/api/v3/auth/refresh", status=401, repeat=True)
    path = client.get_event_media_path(298)
    mocked.get(url(path), status=401)
    mocked.get(url(path), status=401)
    mocked.get(url(path), status=206, body=b"ftyp")

    async with client.async_stream_media(path) as resp:
        assert resp.status == 206
    assert slept == [1.0]


def test_media_paths(client: ZoneMinderV3Client) -> None:
    assert client.get_live_media_path(1) == "api/v3/live/1/hls/master.m3u8"
    assert client.get_event_media_path(5) == "api/v3/events/5/video"
    assert client.get_event_thumbnail_path(5) == "api/v3/events/5/thumbnail"
    assert client.get_webrtc_signaling_url(1) == (
        "ws://192.168.0.45:8080/api/v3/live/1/webrtc/ws"
    )


@pytest.mark.parametrize("action", ["start", "stop", "restart"])
async def test_server_control(
    client: ZoneMinderV3Client, mocked: aioresponses, action: str
) -> None:
    register_login(mocked)
    mocked.post(url(f"api/v3/server/control/{action}"), payload={"message": "ok"})
    await client.server_control(action)
    assert len(calls(mocked, "POST", f"api/v3/server/control/{action}")) == 1


async def test_server_control_rejects_other_actions(
    client: ZoneMinderV3Client,
) -> None:
    with pytest.raises(ValueError, match="reboot"):
        await client.server_control("reboot")
