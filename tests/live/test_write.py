"""Live tests that change the server; they run only with ZM_LIVE_WRITE=1.

Each one puts back what it changed. Forced alarms leave their events.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
import logging
import secrets
import sys
from typing import Any
import warnings

import aiohttp
import pytest

from aiozoneminder import (
    BACKEND_V3,
    Monitor,
    RunState,
    ZMError,
    ZMNotSupportedError,
    ZoneMinderClient,
)
from aiozoneminder.const import DECODING_OPTIONS

from .support import (
    API_NAMES,
    CAUSE,
    CONFIG,
    client_for,
    forced_alarm,
    mentions,
    restoring_modes,
    until,
    wait_event_closed,
    wait_until_capturing,
)

pytestmark = [pytest.mark.asyncio(loop_scope="session"), pytest.mark.write]

v3_only = pytest.mark.backends(BACKEND_V3)


async def test_modes(zm: ZoneMinderClient, monitor: Monitor) -> None:
    """Changing a monitor's analysing mode takes effect."""
    async with restoring_modes(zm, monitor.id) as (_c, analysing, _r, _d):
        new = "None" if analysing != "None" else "Always"
        await zm.set_monitor_state(monitor.id, analysing=new)
        assert (await zm.get_monitor(monitor.id)).analysing == new


async def test_capturing(zm: ZoneMinderClient, spare_monitor: Monitor) -> None:
    """Capturing switches off and back on, and the monitor serves images again."""
    mid = spare_monitor.id
    async with restoring_modes(zm, mid):
        await zm.set_monitor_state(mid, capturing="None")
        assert (await zm.get_monitor(mid)).capturing == "None"
        if zm.backend == BACKEND_V3:

            async def no_snapshot() -> bool:
                return await zm.async_get_snapshot(mid) is None

            await until(f"end of snapshots from monitor {mid}", no_snapshot, timeout=30)
    # restoring_modes waits for snapshots from the monitor again.


async def test_alarm(zm: ZoneMinderClient, recording_monitor: Monitor) -> None:
    """A forced alarm records an event that reads as open, then closed after cancelling.

    Home Assistant's motion sensor and notifications follow ``in_progress``.
    On API v2 ZoneMinder records the cause as "Forced Web" whatever is sent.
    """
    async with forced_alarm(zm, recording_monitor.id) as event:
        assert event.in_progress, f"event {event.id} reads as finished while recording"
        if zm.backend == BACKEND_V3:
            assert CAUSE in event.cause, event.cause
        assert (await zm.get_event(event.id)).in_progress
    closed = await wait_event_closed(zm, event.id)
    assert closed.end_time is not None
    assert closed.end_time >= closed.start_time


@v3_only
async def test_event_stream(zm: ZoneMinderClient, recording_monitor: Monitor) -> None:
    """The pushed event stream reports a new event and its end."""
    since = max((e.id for e in await zm.get_events(limit=1)), default=0)
    pushed: list[tuple[str, dict[str, Any]]] = []

    async def listen() -> None:
        async for kind, data in zm.async_event_stream(since=since):
            pushed.append((kind, data))

    task = asyncio.create_task(listen())
    try:
        await asyncio.sleep(1)
        async with forced_alarm(zm, recording_monitor.id) as event:

            async def started() -> bool:
                return any(mentions(d, event.id) for _, d in pushed)

            await until(f"pushed message about event {event.id}", started, 20, 1)

        async def ended() -> bool:
            return any(k == "event_end" and mentions(d, event.id) for k, d in pushed)

        await until(f"event_end for event {event.id}", ended, 30, 1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ZMError):
            await task


async def test_events_between_running(
    zm: ZoneMinderClient, recording_monitor: Monitor
) -> None:
    """A running event is in the time-window query the timeline uses."""
    async with forced_alarm(zm, recording_monitor.id) as event:
        now = datetime.now(UTC)
        found = await zm.get_events_between(
            recording_monitor.id, now - timedelta(hours=1), now + timedelta(minutes=1)
        )
    assert event.id in [e.id for e in found], f"running event {event.id} missing"


async def test_alarm_state(
    zm: ZoneMinderClient, recording_monitor: Monitor, spare_monitor: Monitor
) -> None:
    """The alarm state follows a forced alarm, and is unknown without capture.

    Home Assistant shows it as the camera recording and the force alarm switch.
    """
    mid = recording_monitor.id
    async with forced_alarm(zm, mid):
        assert await zm.get_alarm_state(mid) in ("alarm", "alert")

    async def idle() -> bool:
        return await zm.get_alarm_state(mid) == "idle"

    await until(f"monitor {mid} back to idle", idle, timeout=30)

    spare = spare_monitor.id
    async with restoring_modes(zm, spare):
        await zm.set_monitor_state(spare, capturing="None")

        async def unknown() -> bool:
            return await zm.get_alarm_state(spare) is None

        await until(f"no alarm state for monitor {spare}", unknown, timeout=30)


async def test_alarm_not_capturing(
    zm: ZoneMinderClient, spare_monitor: Monitor
) -> None:
    """An alarm the server can't raise (monitor not capturing) is reported as failed."""
    mid = spare_monitor.id
    async with restoring_modes(zm, mid):
        await zm.set_monitor_state(mid, capturing="None")
        await asyncio.sleep(3)
        try:
            await zm.trigger_alarm(mid, CAUSE)
        except ZMError:
            return
        with contextlib.suppress(ZMError):
            await zm.cancel_alarm(mid)
        pytest.fail("trigger_alarm on a monitor that isn't capturing reported success")


@v3_only
async def test_snapshot_not_capturing_quiet(
    zm: ZoneMinderClient, spare_monitor: Monitor, caplog: pytest.LogCaptureFixture
) -> None:
    """A switched-off camera's snapshot doesn't log an error on every refresh."""
    mid = spare_monitor.id
    async with restoring_modes(zm, mid):
        await zm.set_monitor_state(mid, capturing="None")
        await asyncio.sleep(3)
        with caplog.at_level(logging.DEBUG, logger="aiozoneminder"):
            assert await zm.async_get_snapshot(mid) is None
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert not errors, errors[0]


@v3_only
async def test_long_alarm_cause(
    zm: ZoneMinderClient, recording_monitor: Monitor
) -> None:
    """An alarm with a long cause (40 characters) is still raised."""
    cause = f"{CAUSE} long cause ".ljust(40, "x")
    async with forced_alarm(zm, recording_monitor.id, cause) as event:
        assert event.cause, "event without a cause"
        assert cause.startswith(event.cause[:20])


async def test_decoding_values_round_trip(
    zm: ZoneMinderClient, spare_monitor: Monitor
) -> None:
    """Every decoding choice the library offers is accepted and reads back the same."""
    mid = spare_monitor.id
    async with restoring_modes(zm, mid):
        for value in DECODING_OPTIONS:
            await zm.set_monitor_state(mid, decoding=value)
            got = (await zm.get_monitor(mid)).decoding
            assert got == value, f"decoding {value!r} read back as {got!r}"


async def test_ptz(zm: ZoneMinderClient, monitors: list[Monitor]) -> None:
    """PTZ commands are accepted for a controllable camera (left, then stop).

    Whether the camera moved can only be seen on the camera.
    """
    target = next(
        (m for m in monitors if m.controllable and m.capturing != "None"), None
    )
    if CONFIG.monitor is not None:
        target = next(
            (m for m in monitors if m.id == CONFIG.monitor and m.controllable), None
        )
    if target is None:
        pytest.skip("no capturing monitor with PTZ control")
    assert isinstance(await zm.get_ptz_presets(target.id), dict)
    try:
        await zm.ptz(target.id, "left")
        await asyncio.sleep(1)
    finally:
        await zm.ptz(target.id, "stop")


def _definition(modes: dict[int, tuple[str, str, str]]) -> str:
    return ",".join(f"{i}:{c}:{a}:{r}" for i, (c, a, r) in sorted(modes.items()))


async def _save_state(zm: ZoneMinderClient, name: str, definition: str) -> None:
    """Save a run state with the library, or the API where the library can't."""
    if zm.backend == BACKEND_V3:
        await zm.save_run_state(name, definition)
        return
    with pytest.raises(ZMNotSupportedError):
        await zm.save_run_state(name, definition)
    reply = await zm._request(  # type: ignore[attr-defined]
        "POST",
        "api/states.json",
        data={
            "State[Name]": name,
            "State[Definition]": definition,
            "State[IsActive]": 0,
        },
    )
    assert (reply or {}).get("message") == "Saved", reply


async def _set_active(zm: ZoneMinderClient, state: RunState, active: bool) -> None:
    """Set a run state's active flag through the API, without applying it."""
    if zm.backend == BACKEND_V3:
        await zm._request(  # type: ignore[attr-defined]
            "PATCH", f"api/v3/states/{state.id}", json={"is_active": int(active)}
        )
    else:
        await zm._request(  # type: ignore[attr-defined]
            "POST", f"api/states/{state.id}.json", data={"State[IsActive]": int(active)}
        )


async def _delete_state(zm: ZoneMinderClient, state: RunState) -> None:
    endpoint = (
        f"api/v3/states/{state.id}"
        if zm.backend == BACKEND_V3
        else f"api/states/{state.id}.json"
    )
    # API v2 answers a delete with a redirect page rather than JSON.
    with contextlib.suppress(ZMError, ValueError):
        await zm._request("DELETE", endpoint)  # type: ignore[attr-defined]


async def _put_back(
    zm: ZoneMinderClient,
    names: set[str],
    was_active: str | None,
    modes: dict[int, tuple[str, str, str]],
) -> list[str]:
    """Remove our run states, restore the active one and every monitor's modes."""
    problems: list[str] = []
    try:
        states = await zm.get_run_states()
        for state in states:
            if state.name == was_active and not state.active:
                await _set_active(zm, state, True)
            elif state.name != was_active and state.active and state.name not in names:
                await _set_active(zm, state, False)
        for state in states:
            if state.name in names:
                await _delete_state(zm, state)
        states = await zm.get_run_states()
        left = sorted(s.name for s in states if s.name in names)
        if left:
            problems.append(
                f"run states left on the server, delete them by hand: {left}"
            )
        active = [s.name for s in states if s.active and s.name not in names]
        if active != ([was_active] if was_active else []):
            problems.append(f"active run state {active}, was {was_active}")
        for m in await zm.get_monitors():
            want = modes.get(m.id)
            if want and want != (m.capturing, m.analysing, m.recording):
                c, a, r = want
                await zm.set_monitor_state(m.id, capturing=c, analysing=a, recording=r)
        now = {
            m.id: (m.capturing, m.analysing, m.recording)
            for m in await zm.get_monitors()
        }
        wrong = sorted(i for i, want in modes.items() if now.get(i) != want)
        if wrong:
            problems.append(f"monitors {wrong} not back to their modes")
    except ZMError as err:
        problems.append(f"cleaning up failed: {err}")
    return problems


async def test_run_state_switch(
    backend: str, zm: ZoneMinderClient, spare_monitor: Monitor
) -> None:
    """Saving run states and switching between them changes the monitors.

    Two states named aiozm-live-* are saved (through the API on v2, whose
    library call isn't supported): one with a monitor switched off, one with
    every monitor as it is. The test switches to each, then deletes them and
    puts the previously active state and every monitor's modes back.
    """
    modes = {
        m.id: (m.capturing, m.analysing, m.recording) for m in await zm.get_monitors()
    }
    was_active = next((s.name for s in await zm.get_run_states() if s.active), None)
    tag = secrets.token_hex(3)
    off_name = f"aiozm-live-{API_NAMES[backend]}-off-{tag}"
    on_name = f"aiozm-live-{API_NAMES[backend]}-on-{tag}"
    mid = spare_monitor.id
    try:
        await _save_state(
            zm, off_name, _definition({**modes, mid: ("None", "None", "None")})
        )
        await _save_state(zm, on_name, _definition(modes))
        states = {s.name: s for s in await zm.get_run_states()}
        assert off_name in states, sorted(states)
        assert states[off_name].monitor_modes().get(mid) == ("None", "None", "None")

        await zm.set_run_state(states[off_name])

        async def stopped() -> bool:
            return (await zm.get_monitor(mid)).capturing == "None"

        await until(f"monitor {mid} off after switching run state", stopped, 60)
        active = [s.name for s in await zm.get_run_states() if s.active]
        assert active == [off_name], f"active run states {active}"

        await zm.set_run_state(states[on_name])

        async def restored() -> bool:
            m = await zm.get_monitor(mid)
            return (m.capturing, m.analysing, m.recording) == modes[mid]

        await until(f"monitor {mid} back after switching run state", restored, 90)
    finally:
        failing = sys.exc_info()[0] is not None
        problems = await _put_back(zm, {off_name, on_name}, was_active, modes)
        if modes[mid][0] != "None":
            with contextlib.suppress(pytest.fail.Exception):
                await wait_until_capturing(zm, mid)
        if problems and failing:
            warnings.warn("; ".join(problems), stacklevel=1)
        elif problems:
            pytest.fail("; ".join(problems))


# Last: these log out every session of ZM_LIVE_USER.

#: Seconds to let zm-api's login rate limit refill before a revocation.
LOGIN_PAUSE = 20


async def _revoke_tokens(backend: str, session: aiohttp.ClientSession) -> None:
    """Log out a second session, which revokes every token of the user.

    Waits first: zm-api allows a burst of 10 logins and refreshes, then one
    every 2 seconds, and a revocation costs a few of them.
    """
    await asyncio.sleep(LOGIN_PAUSE)
    other = client_for(backend, session)
    await other.authenticate()
    token = getattr(other, "access_token", None)
    async with session.get(
        f"{CONFIG.urls[backend]}/api/v3/auth/logout",
        headers={"Authorization": f"Bearer {token}"},
    ) as response:
        assert response.status < 300, f"logout answered {response.status}"


@v3_only
async def test_media_after_logout_elsewhere(
    backend: str,
    zm: ZoneMinderClient,
    monitor: Monitor,
    live_http: aiohttp.ClientSession,
) -> None:
    """Media keeps playing right after the user logs out in another session.

    Logs out every session of ZM_LIVE_USER.
    """
    events = await zm.get_events(monitor_id=monitor.id, limit=1)
    path = (
        zm.get_event_media_path(events[0].id)
        if events
        else zm.get_live_media_path(monitor.id)
    )
    assert path
    await _revoke_tokens(backend, live_http)
    try:
        async with zm.async_stream_media(
            path, headers={"Range": "bytes=0-99"}
        ) as response:
            status = response.status
        assert status != 401, "media refused with the revoked token"
    finally:
        await asyncio.sleep(2)  # zm-api revokes tokens issued up to a second after
        await zm.authenticate()


@v3_only
async def test_requests_after_logout_elsewhere(
    backend: str, zm: ZoneMinderClient, live_http: aiohttp.ClientSession
) -> None:
    """A logout in another session doesn't make the next request ask to log in again.

    Logs out every session of ZM_LIVE_USER.
    """
    await _revoke_tokens(backend, live_http)
    try:
        assert await zm.get_monitors()
    finally:
        await asyncio.sleep(2)
        await zm.authenticate()
