"""Tests for the API v2 (built-in CakePHP API) client."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
import logging
import re
from typing import Any

import aiohttp
from aioresponses import CallbackResult, aioresponses
import pytest

from aiozoneminder import (
    ZMApiError,
    ZMAuthenticationError,
    ZMConnectionError,
    ZMNotSupportedError,
    ZoneMinderLegacyClient,
)
from aiozoneminder.const import DECODING_OPTIONS, STREAM_TYPE_GO2RTC, STREAM_TYPE_MJPEG

ZM = "http://zm.local/zm"
GO2RTC = "http://192.168.0.45:1984/api"

# The stream names a real ZoneMinder go2rtc instance advertises. They are not
# plain monitor ids, which is what the old guess-the-name code assumed.
GO2RTC_STREAMS = {
    "1_0": {},
    "1_1": {},
    "2_0": {},
    "3": {},
    "3_ZoneMinderPrimary": {},
    "3_h264": {},
    "4_0": {},
}

CONFIGS_RESPONSE = {
    "configs": [
        {"Config": {"Name": "ZM_GO2RTC_PATH", "Value": GO2RTC}},
        {"Config": {"Name": "ZM_MQTT_TOPIC_PREFIX", "Value": "ZoneMinder"}},
    ]
}


@pytest.fixture
def client(session: aiohttp.ClientSession) -> ZoneMinderLegacyClient:
    """Return an API v2 client pointed at a stand-in ZoneMinder."""
    return ZoneMinderLegacyClient(ZM, "admin", "hunter2", session)


def _login(mocked: aioresponses) -> None:
    """Register a successful login."""
    mocked.post(
        f"{ZM}/api/host/login.json",
        payload={"access_token": "token"},
        repeat=True,
    )


async def test_authenticate(client: ZoneMinderLegacyClient) -> None:
    """A successful login stores the token."""
    with aioresponses() as mocked:
        _login(mocked)
        assert await client.authenticate() is True
    assert client.authenticated is True


async def test_bad_credentials(client: ZoneMinderLegacyClient) -> None:
    """A rejected login raises ZMAuthenticationError."""
    with aioresponses() as mocked:
        mocked.post(f"{ZM}/api/host/login.json", status=401)
        with pytest.raises(ZMAuthenticationError):
            await client.authenticate()


async def test_missing_api_explains_itself(client: ZoneMinderLegacyClient) -> None:
    """A 404 on login usually means the URL is missing the /zm path."""
    with aioresponses() as mocked:
        mocked.post(f"{ZM}/api/host/login.json", status=404)
        with pytest.raises(ZMConnectionError, match="OPT_USE_API"):
            await client.authenticate()


async def test_go2rtc_config_is_read_from_the_right_path(
    client: ZoneMinderLegacyClient,
) -> None:
    """The config endpoint sits at api/configs.json, not zm/api/configs.json.

    The old path produced http://host/zm/zm/api/configs.json, whose 404 was
    swallowed, so go2rtc was never detected and every camera silently fell
    back to MJPEG.
    """
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/api/configs.json?token=token", payload=CONFIGS_RESPONSE)
        mocked.get(f"{GO2RTC}/streams", payload=GO2RTC_STREAMS)
        mocked.get(GO2RTC, payload={"rtsp": {"listen": ":8554"}})

        config = await client.check_go2rtc()

    assert config is not None
    assert config["base_url"] == "http://192.168.0.45:1984"
    assert config["rtsp_port"] == 8554


async def test_go2rtc_stream_names_are_discovered(
    client: ZoneMinderLegacyClient,
) -> None:
    """Stream names are matched against the real list, not guessed."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/api/configs.json?token=token", payload=CONFIGS_RESPONSE)
        mocked.get(f"{GO2RTC}/streams", payload=GO2RTC_STREAMS)
        mocked.get(GO2RTC, payload={"rtsp": {"listen": ":8554"}})

        # Monitor 3 is published under its bare id, the others under _0.
        assert await client.get_stream_source(3) == "rtsp://192.168.0.45:8554/3"
        assert await client.get_stream_source(1) == "rtsp://192.168.0.45:8554/1_0"
        assert client.get_stream_type(1) == STREAM_TYPE_GO2RTC


@pytest.mark.parametrize(
    "path",
    ["http://192.168.0.45:1984", "http://192.168.0.45:1984/", f"{GO2RTC}/"],
)
async def test_go2rtc_path_with_or_without_api(
    client: ZoneMinderLegacyClient, path: str
) -> None:
    """ZoneMinder takes ZM_GO2RTC_PATH with or without the /api suffix."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            f"{ZM}/api/configs.json?token=token",
            payload={
                "configs": [{"Config": {"Name": "ZM_GO2RTC_PATH", "Value": path}}]
            },
        )
        mocked.get(f"{GO2RTC}/streams", payload=GO2RTC_STREAMS)
        mocked.get(GO2RTC, payload={"rtsp": {"listen": ":8554"}})

        assert await client.get_stream_source(3) == "rtsp://192.168.0.45:8554/3"


async def test_go2rtc_with_credentials(
    client: ZoneMinderLegacyClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A user:pass@ address keeps the real RTSP host, and its password out of logs."""
    caplog.set_level(logging.DEBUG, logger="aiozoneminder")
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            f"{ZM}/api/configs.json?token=token",
            payload={
                "configs": [
                    {
                        "Config": {
                            "Name": "ZM_GO2RTC_PATH",
                            "Value": "http://admin:s3cret@192.168.0.45:1984/api",
                        }
                    }
                ]
            },
        )
        mocked.get(
            "http://admin:s3cret@192.168.0.45:1984/api/streams", payload=GO2RTC_STREAMS
        )
        mocked.get(
            "http://admin:s3cret@192.168.0.45:1984/api",
            payload={"rtsp": {"listen": ":8554"}},
        )

        assert await client.get_stream_source(3) == "rtsp://192.168.0.45:8554/3"
    assert "s3cret" not in caplog.text
    assert "192.168.0.45:1984" in caplog.text


async def test_falls_back_to_mjpeg_when_rtsp_is_off(
    client: ZoneMinderLegacyClient,
) -> None:
    """go2rtc ships with its RTSP server disabled in some packages.

    Handing a player an rtsp:// URL nothing is listening on produces a
    camera that never loads, so an unusable RTSP port means MJPEG instead.
    """
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/api/configs.json?token=token", payload=CONFIGS_RESPONSE)
        mocked.get(f"{GO2RTC}/streams", payload=GO2RTC_STREAMS)
        # An empty listen address means no RTSP server is running.
        mocked.get(GO2RTC, payload={"rtsp": {"listen": ""}})

        source = await client.get_stream_source(1)

    assert source.startswith(f"{ZM}/cgi-bin/nph-zms")
    assert client.get_stream_type(1) == STREAM_TYPE_MJPEG


async def test_no_go2rtc_configured(client: ZoneMinderLegacyClient) -> None:
    """Without ZM_GO2RTC_PATH there is nothing to discover."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/api/configs.json?token=token", payload={"configs": []})
        assert await client.check_go2rtc() is None
        assert await client.get_stream_source(1) is not None


async def test_set_monitor_state_writes_both_shapes(
    client: ZoneMinderLegacyClient,
) -> None:
    """1.36 wants Function, 1.37+ wants the split fields, so send both."""
    posted: dict[str, str] = {}

    def _capture(url: Any, **kwargs: Any) -> CallbackResult:
        posted.update(kwargs.get("data") or {})
        return CallbackResult(payload={})

    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            f"{ZM}/api/monitors/1.json?token=token",
            payload={
                "monitor": {
                    "Monitor": {
                        "Id": "1",
                        "Name": "Driveway",
                        "Function": "Monitor",
                        "Type": "Ffmpeg",
                        "Width": "1920",
                        "Height": "1080",
                    }
                }
            },
        )
        mocked.post(f"{ZM}/api/monitors/1.json?token=token", callback=_capture)

        await client.set_monitor_state(1, analysing="Always", recording="Always")

    assert posted["Monitor[Analysing]"] == "Always"
    assert posted["Monitor[Recording]"] == "Always"
    # Capturing was left alone, so it keeps the monitor's current value.
    assert posted["Monitor[Capturing]"] == "Always"
    assert posted["Monitor[Function]"] == "Mocord"


def test_decoding_options_are_zoneminder_values() -> None:
    """Every decoding option is a value of ZoneMinder's Monitors.Decoding enum.

    "KeyFramesOndemand" isn't one: API v2 answered 500, and API v3 read back
    "KeyFrames+Ondemand", which no option matched.
    """
    assert DECODING_OPTIONS == [
        "None",
        "Ondemand",
        "KeyFrames",
        "KeyFrames+Ondemand",
        "Always",
    ]


async def test_reauthenticates_once_on_401(client: ZoneMinderLegacyClient) -> None:
    """An expired token is refreshed and the request retried."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/api/host/getVersion.json?token=token", status=401)
        mocked.get(
            f"{ZM}/api/host/getVersion.json?token=token",
            payload={"version": "1.36.33"},
        )
        assert await client.get_version() == "1.36.33"


async def test_permission_refusal_is_not_a_login_failure(
    client: ZoneMinderLegacyClient,
) -> None:
    """A 401 that survives a fresh login is the user's permissions, not reauth.

    ZoneMinder answers 401 "Insufficient privileges" to a user without System
    or Events view. Raising ZMAuthenticationError sent HA into reauth for a
    login that works.
    """
    refused = {
        "success": False,
        "data": {
            "name": "Insufficient privileges",
            "message": "Insufficient privileges",
        },
    }
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/configs\.json"), status=401, payload=refused, repeat=True)
        with pytest.raises(ZMApiError, match="Insufficient privileges") as err:
            await client.get_configs()
        assert len(sent(mocked, "POST", "api/host/login.json")) == 2

    assert not isinstance(err.value, ZMAuthenticationError)
    assert client.authenticated


async def test_failed_relogin_is_still_an_auth_error(
    client: ZoneMinderLegacyClient,
) -> None:
    """When the fresh login itself is rejected, it stays ZMAuthenticationError."""
    with aioresponses() as mocked:
        mocked.post(f"{ZM}/api/host/login.json", payload={"access_token": "token"})
        mocked.post(f"{ZM}/api/host/login.json", status=401)
        mocked.get(v2(r"api/configs\.json"), status=401)
        with pytest.raises(ZMAuthenticationError):
            await client.get_configs()


def _event(event_id: int, start: str) -> dict[str, Any]:
    """Build an API v2 event."""
    return {
        "Event": {
            "Id": str(event_id),
            "MonitorId": "1",
            "Name": "Event",
            "Cause": "Motion",
            "StartDateTime": start,
            "EndDateTime": None,
            "Length": "0",
            "Frames": "0",
            "AlarmFrames": "3",
        }
    }


async def test_event_times_use_the_given_timezone(
    session: aiohttp.ClientSession,
) -> None:
    """API v2 times have no zone; the client's timezone is applied."""
    sydney = timezone(timedelta(hours=11))
    client = ZoneMinderLegacyClient(ZM, "admin", "hunter2", session, timezone=sydney)
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            f"{ZM}/api/events.json?direction=desc&limit=100&sort=StartTime&token=token",
            payload={
                "events": [
                    _event(1, "2026-10-24 09:00:00"),
                    _event(2, "2026-10-24 10:00:00"),
                ]
            },
        )
        events = await client.get_events()

    assert [e.id for e in events] == [2, 1]  # Newest first.
    assert events[0].start_time == datetime(2026, 10, 23, 23, 0, tzinfo=UTC)


async def test_event_is_read_without_its_frames(
    client: ZoneMinderLegacyClient,
) -> None:
    """One event's reply lists every frame unless noframes is asked for."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/events/7\.json"),
            payload={"event": _event(7, "2026-10-24 09:00:00")},
        )
        event = await client.get_event(7)
        (call,) = sent(mocked, "GET", "api/events/7.json")

    assert event.id == 7
    assert call.kwargs["params"] == {"noframes": "true", "token": "token"}


async def test_events_page_past_the_server_cap(
    client: ZoneMinderLegacyClient,
) -> None:
    """ZoneMinder returns at most 100 events a page; larger limits page through."""
    newest = datetime(2026, 10, 24, 12, 0)

    def _page(url: Any, **kwargs: Any) -> CallbackResult:
        params = kwargs["params"]
        # The server caps a page at 100, whatever limit asks for.
        size, page = min(int(params["limit"]), 100), int(params.get("page", 1))
        ids = range(250 - (page - 1) * size, max(250 - page * size, 0), -1)
        events = [
            _event(i, f"{newest - timedelta(minutes=250 - i):%Y-%m-%d %H:%M:%S}")
            for i in ids
        ]
        pagination = {"page": page, "nextPage": page * size < 250}
        return CallbackResult(payload={"events": events, "pagination": pagination})

    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/events\.json"), callback=_page, repeat=True)
        events = await client.get_events(limit=220)
        assert len(sent(mocked, "GET", "api/events.json")) == 3
        everything = await client.get_events(limit=1000)

    assert [e.id for e in events] == list(range(250, 30, -1))  # Newest first.
    assert len(everything) == 250


async def test_run_states_switch_by_name(client: ZoneMinderLegacyClient) -> None:
    """API v2 applies a run state itself, so switching is one call."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            f"{ZM}/api/states.json?token=token",
            payload={
                "states": [
                    {
                        "State": {
                            "Id": "1",
                            "Name": "default",
                            "Definition": "",
                            "IsActive": "1",
                        }
                    },
                    {
                        "State": {
                            "Id": "2",
                            "Name": "Night time",
                            "Definition": "1:Modect:1",
                            "IsActive": "0",
                        }
                    },
                ]
            },
        )
        mocked.post(f"{ZM}/api/states/change/Night%20time.json?token=token", payload={})

        states = await client.get_run_states()
        await client.set_run_state(states[1])

    assert [s.name for s in states] == ["default", "Night time"]
    assert states[0].active


async def test_run_state_switch_waits_for_the_restart(
    client: ZoneMinderLegacyClient,
) -> None:
    """ZoneMinder answers a switch only after restarting every daemon.

    With 45 monitors that took 11 s in a lab, past the 10 s default, so the
    switch reported a failure for a change that went through.
    """
    from aiozoneminder import RunState

    with aioresponses() as mocked:
        _login(mocked)
        mocked.post(v2(r"api/states/change/default\.json"), payload={})
        await client.set_run_state(RunState(1, "default", "", True))
        (call,) = sent(mocked, "POST", "api/states/change/default.json")
    assert call.kwargs["timeout"].total >= 60


async def test_saving_run_states_needs_api_v3(client: ZoneMinderLegacyClient) -> None:
    """API v2 can switch run states but not save them."""
    with pytest.raises(ZMNotSupportedError):
        await client.save_run_state("Holiday", "1:Modect:1")


async def test_v3_only_features_say_so(client: ZoneMinderLegacyClient) -> None:
    """The event stream and WebRTC exist on every client but need API v3."""
    with pytest.raises(ZMNotSupportedError):
        async for _message in client.async_event_stream():
            pass
    assert await client.async_supports_whep(1) is False
    with pytest.raises(ZMNotSupportedError):
        await client.async_whep_offer(1, "v=0")
    with pytest.raises(ZMNotSupportedError):
        await client.async_whep_candidate("/session", "0", "candidate:1")
    with pytest.raises(ZMNotSupportedError):
        await client.async_whep_close("/session")


async def test_reads_without_v3_return_nothing(client: ZoneMinderLegacyClient) -> None:
    """Reads API v2 can't do return empty results rather than failing."""
    assert await client.get_ptz_presets(1) == {}
    assert await client.async_get_rtsp_url(1) is None


async def test_event_paging_stops_when_pages_repeat(
    client: ZoneMinderLegacyClient,
) -> None:
    """A server that ignores ``page`` but says there's more doesn't loop forever."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/events\.json"),
            payload={
                "events": [_event(1, "2026-10-24 09:00:00")],
                "pagination": {"nextPage": True},
            },
            repeat=True,
        )
        events = await client.get_events(limit=500)
        assert len(sent(mocked, "GET", "api/events.json")) == 2
    assert [e.id for e in events] == [1]


def v2(path: str) -> re.Pattern[str]:
    """Match an API v2 endpoint whatever its query string."""
    return re.compile(rf"^{re.escape(ZM)}/{path}(\?.*)?$")


def sent(mocked: aioresponses, method: str, path: str) -> list[Any]:
    """Return the recorded calls to one endpoint."""
    return [
        call
        for (verb, called), requests in mocked.requests.items()
        if verb == method and str(called).split("?")[0] == f"{ZM}/{path}"
        for call in requests
    ]


async def test_monitors(client: ZoneMinderLegacyClient) -> None:
    """Monitors parse from the StudlyCaps payload; broken ones are skipped."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={
                "monitors": [
                    {
                        "Monitor": {
                            "Id": "1",
                            "Name": "Driveway",
                            "Function": "Modect",
                            "Controllable": "1",
                        }
                    },
                    {"Monitor": {"Name": "No id"}},
                ]
            },
        )
        monitors = await client.get_monitors()

    (driveway,) = monitors
    assert driveway.name == "Driveway"
    assert (driveway.analysing, driveway.recording) == ("Always", "OnMotion")
    assert driveway.controllable


async def test_configs_and_server_info(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/configs\.json"), payload=CONFIGS_RESPONSE)
        mocked.get(v2(r"api/host/getVersion\.json"), payload={"version": "1.36.33"})
        configs = await client.get_configs()
        info = await client.get_server_info()

    assert configs["ZM_MQTT_TOPIC_PREFIX"] == "ZoneMinder"
    assert info.version == "1.36.33"
    # API v2 has no daemon status, so whether ZoneMinder runs is unknown.
    assert info.running is None
    assert info.storage == []


async def test_alarms(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors/alarm/id:1/command:on\.json"), payload={})
        mocked.get(v2(r"api/monitors/alarm/id:1/command:off\.json"), payload={})
        await client.trigger_alarm(1, "Doorbell")
        await client.cancel_alarm(1)

        (on,) = sent(mocked, "GET", "api/monitors/alarm/id:1/command:on.json")
        assert on.kwargs["params"] == {"cause": "Doorbell", "token": "token"}
        assert sent(mocked, "GET", "api/monitors/alarm/id:1/command:off.json")


async def test_alarm_failures_raise(client: ZoneMinderLegacyClient) -> None:
    """zmu failing still answers 200; the error inside is raised, not dropped."""
    failed = {
        "status": "false",
        "code": 255,
        "error": "Monitor 1 is not capturing",
    }
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors/alarm/id:1/command:on\.json"), payload=failed)
        mocked.get(v2(r"api/monitors/alarm/id:1/command:off\.json"), payload=failed)
        with pytest.raises(ZMApiError, match="not capturing"):
            await client.trigger_alarm(1, "Doorbell")
        with pytest.raises(ZMApiError, match="255"):
            await client.cancel_alarm(1)


async def test_failed_monitor_save_raises(client: ZoneMinderLegacyClient) -> None:
    """A monitor save that fails answers 200 with an "Error ..." message."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors/1\.json"),
            payload={"monitor": {"Monitor": {"Id": "1", "Name": "Driveway"}}},
        )
        mocked.post(
            v2(r"api/monitors/1\.json"),
            payload={"message": "Error Array\n(\n    [Decoding] => ...\n)\n"},
        )
        with pytest.raises(ZMApiError, match="not saved"):
            await client.set_monitor_state(1, decoding="Always")


@pytest.mark.parametrize(
    ("command", "kwargs", "expected"),
    [
        ("up", {}, {"control": "moveConUp", "xge": 50, "yge": 50}),
        ("down", {"speed": 80}, {"control": "moveConDown", "xge": 80, "yge": 80}),
        ("zoom-in", {}, {"control": "zoomConTele", "xge": 50, "yge": 50}),
        ("home", {}, {"control": "presetHome"}),
        ("preset", {"preset": 4}, {"control": "presetGoto", "preset": 4}),
        (
            "left",
            {"x": 10, "y": 20},
            {
                "control": "moveConLeft",
                "xge": 50,
                "yge": 50,
                "xCoord": 10,
                "yCoord": 20,
            },
        ),
    ],
)
async def test_ptz(
    client: ZoneMinderLegacyClient,
    command: str,
    kwargs: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    """PTZ goes through the web request ZoneMinder's own UI uses.

    The Controls API only reads control definitions, so it can't move a
    camera. Without xge/yge the control module gets speed 0 and stops.
    """
    with aioresponses() as mocked:
        _login(mocked)
        mocked.post(v2(r"index\.php"), payload={"result": "Ok"})
        await client.ptz(3, command, **kwargs)
        (call,) = sent(mocked, "POST", "index.php")
    assert call.kwargs["params"] == {"token": "token"}
    assert call.kwargs["data"] == {
        "view": "request",
        "request": "control",
        "id": 3,
        **expected,
    }


async def test_ptz_refused(client: ZoneMinderLegacyClient) -> None:
    """ZoneMinder refuses a command with HTTP 200 and an error result."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.post(
            v2(r"index\.php"),
            payload={"result": "Error", "message": "No command received"},
        )
        with pytest.raises(ZMApiError, match="No command received"):
            await client.ptz(3, "up")


async def test_html_instead_of_json(client: ZoneMinderLegacyClient) -> None:
    """A login page or proxy error page is an API error, not a crash."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.post(
            v2(r"index\.php"), body="<html>Login</html>", content_type="text/html"
        )
        with pytest.raises(ZMApiError, match="not JSON"):
            await client.ptz(3, "up")


async def test_ptz_mistakes(client: ZoneMinderLegacyClient) -> None:
    with pytest.raises(ZMNotSupportedError):
        await client.ptz(3, "spin")
    with pytest.raises(ZMApiError, match="preset number"):
        await client.ptz(3, "preset")


async def test_not_found_and_server_errors(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors/9\.json"), status=404)
        mocked.get(v2(r"api/monitors/1\.json"), status=500)
        mocked.get(
            v2(r"api/monitors/2\.json"), exception=aiohttp.ServerDisconnectedError()
        )
        with pytest.raises(ZMApiError, match="Not found"):
            await client.get_monitor(9)
        with pytest.raises(ZMApiError, match="500"):
            await client.get_monitor(1)
        with pytest.raises(ZMConnectionError):
            await client.get_monitor(2)


async def test_unreachable_login(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        mocked.post(
            f"{ZM}/api/host/login.json", exception=aiohttp.ClientConnectionError()
        )
        with pytest.raises(ZMConnectionError):
            await client.authenticate()


async def test_snapshot(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        mocked.get(v2("cgi-bin/nph-zms"), body=b"\xff\xd8jpeg")
        mocked.get(v2("cgi-bin/nph-zms"), status=500)
        assert await client.async_get_snapshot(1) == b"\xff\xd8jpeg"
        assert await client.async_get_snapshot(1) is None


async def test_snapshot_logs_no_token(
    client: ZoneMinderLegacyClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed snapshot's log line leaves out the URL and its token."""
    with aioresponses() as mocked:
        _login(mocked)
        await client.authenticate()
        mocked.get(
            v2("cgi-bin/nph-zms"),
            exception=aiohttp.InvalidURL(client.get_stream_url(1, mode="single")),
        )
        mocked.get(v2("cgi-bin/nph-zms"), status=500)
        assert await client.async_get_snapshot(1) is None
        assert await client.async_get_snapshot(1) is None

    assert "Snapshot for monitor 1 failed" in caplog.text
    assert "token=token" not in caplog.text


async def test_snapshot_logs_in_again_once(client: ZoneMinderLegacyClient) -> None:
    """zms refusing the token (403) gets one fresh login, then the frame."""
    with aioresponses() as mocked:
        _login(mocked)
        await client.authenticate()
        mocked.get(v2("cgi-bin/nph-zms"), status=403)
        mocked.get(v2("cgi-bin/nph-zms"), body=b"\xff\xd8jpeg")
        assert await client.async_get_snapshot(1) == b"\xff\xd8jpeg"
        assert len(sent(mocked, "POST", "api/host/login.json")) == 2

        mocked.get(v2("cgi-bin/nph-zms"), status=403, repeat=True)
        assert await client.async_get_snapshot(1) is None
        assert len(sent(mocked, "POST", "api/host/login.json")) == 3


async def test_empty_snapshot_is_a_failure(client: ZoneMinderLegacyClient) -> None:
    """An empty 200 is no image.

    zms behind nginx/fcgiwrap, before ZoneMinder d27b9612f, answers a refused
    request that way.
    """
    with aioresponses() as mocked:
        mocked.get(v2("cgi-bin/nph-zms"), body=b"")
        assert await client.async_get_snapshot(1) is None


async def test_media_carries_the_token(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/events/5\.mp4"), body=b"mp4")
        async with client.async_stream_media("api/events/5.mp4") as response:
            assert await response.read() == b"mp4"
        (call,) = sent(mocked, "GET", "api/events/5.mp4")
    assert call.kwargs["params"] == {"token": "token"}


async def test_urls_carry_the_token(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        await client.authenticate()
    assert client.get_stream_url(1).startswith(f"{ZM}/cgi-bin/nph-zms?mode=jpeg")
    assert client.get_stream_url(1).endswith("token=token")
    assert client.get_event_clip_url(5) == f"{ZM}/api/events/5.mp4?token=token"


async def test_close_logs_out(client: ZoneMinderLegacyClient) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/host/logout\.json"), payload={})
        await client.authenticate()
        await client.close()
        assert sent(mocked, "GET", "api/host/logout.json")
    assert not client.authenticated


def _monitor_item(
    monitor_id: int,
    *,
    deleted: Any = None,
    status: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One entry of api/monitors.json, as ZoneMinder 1.38 returns it."""
    monitor: dict[str, Any] = {
        "Id": str(monitor_id),
        "Name": f"Camera {monitor_id}",
        "Capturing": "Always",
        "Analysing": "Always",
        "Recording": "OnMotion",
    }
    if deleted is not None:
        monitor["Deleted"] = deleted
    return {
        "Monitor": monitor,
        "Monitor_Status": status
        or {
            "MonitorId": str(monitor_id),
            "Status": "Connected",
            "CaptureFPS": "14.97",
            "AnalysisFPS": "5.00",
            "CaptureBandwidth": "312000",
            "UpdatedOn": "2026-10-24 09:15:02",
        },
        "Event_Summary": summary
        or {
            "MonitorId": str(monitor_id),
            "TotalEvents": "812",
            "TotalEventDiskSpace": "9000000",
            "HourEvents": "3",
            "DayEvents": "41",
            "DayEventDiskSpace": "450000",
            "WeekEvents": "210",
            "MonthEvents": "790",
            "ArchivedEvents": "2",
        },
    }


async def test_deleted_monitors_are_left_out(client: ZoneMinderLegacyClient) -> None:
    """ZoneMinder 1.38 keeps deleted monitors, marked Deleted, in the API."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={
                "monitors": [
                    _monitor_item(1),
                    _monitor_item(2, deleted="1"),
                    _monitor_item(3, deleted=True),
                    _monitor_item(4, deleted="0"),
                ]
            },
        )
        monitors = await client.get_monitors()
        statuses = await client.get_monitor_statuses()

    assert [monitor.id for monitor in monitors] == [1, 4]
    assert sorted(statuses) == [1, 4]


async def test_health_comes_with_the_monitor_list(
    client: ZoneMinderLegacyClient,
) -> None:
    """Capture status is in api/monitors.json, so it costs no extra request."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={
                "monitors": [
                    _monitor_item(1),
                    _monitor_item(2, status={"MonitorId": "2", "Status": "NotRunning"}),
                ]
            },
        )
        await client.get_monitors()
        statuses = await client.get_monitor_statuses()
        assert len(sent(mocked, "GET", "api/monitors.json")) == 1

    assert statuses[1].connected
    assert statuses[1].capture_fps == pytest.approx(14.97)
    assert statuses[1].capture_bandwidth == 312000
    assert statuses[1].updated_on is not None
    assert not statuses[2].connected
    assert statuses[2].status == "NotRunning"


async def test_health_on_its_own(client: ZoneMinderLegacyClient) -> None:
    """Without a fresh monitor list, statuses fetch one."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors\.json"), payload={"monitors": [_monitor_item(1)]})
        statuses = await client.get_monitor_statuses()
    assert statuses[1].connected


async def test_monitor_without_a_status_row(client: ZoneMinderLegacyClient) -> None:
    """A monitor ZoneMinder has never started has no status and no events.

    No events is a count of zero, as on API v3, not an unknown.
    """
    item = _monitor_item(1)
    item["Monitor_Status"] = {"MonitorId": None, "Status": None}
    item["Event_Summary"] = {"MonitorId": None}
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors\.json"), payload={"monitors": [item]})
        await client.get_monitors()
        assert await client.get_monitor_statuses() == {}
        summary = await client.get_event_summary(1)
    assert summary is not None
    assert (summary.monitor_id, summary.day_events, summary.total_events) == (1, 0, 0)


async def test_event_counts_come_with_the_monitor_list(
    client: ZoneMinderLegacyClient,
) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors\.json"), payload={"monitors": [_monitor_item(1)]})
        await client.get_monitors()
        summary = await client.get_event_summary(1)
        assert await client.get_event_summary(9) is None
        assert len(sent(mocked, "GET", "api/monitors.json")) == 1

    assert summary is not None
    assert (summary.hour_events, summary.day_events) == (3, 41)
    assert (summary.week_events, summary.month_events) == (210, 790)
    assert (summary.total_events, summary.archived_events) == (812, 2)
    assert summary.total_disk_space == 9000000
    assert summary.day_disk_space == 450000


# -- Finding nph-zms ----------------------------------------------------------

ORIGIN = "http://zm.local"


def requests_to(mocked: aioresponses, full_url: str) -> list[Any]:
    """Return the recorded GETs to one absolute URL, ignoring the query."""
    return [
        call
        for (verb, called), calls in mocked.requests.items()
        if verb == "GET" and str(called).split("?")[0] == full_url
        for call in calls
    ]


async def test_zms_path_is_found_after_login(session: aiohttp.ClientSession) -> None:
    """RPM installs serve nph-zms from /cgi-bin-zm, not under the web path.

    The right path answers anything but 404 (403 without a login); wrong
    ones are 404. Found once, then reused.
    """
    client = ZoneMinderLegacyClient(ZM, "admin", "hunter2", session)
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(f"{ZM}/cgi-bin/nph-zms", status=404)
        mocked.get(f"{ORIGIN}/zm/cgi-bin/nph-zms", status=404)
        mocked.get(f"{ORIGIN}/cgi-bin/nph-zms", status=404)
        mocked.get(f"{ORIGIN}/cgi-bin-zm/nph-zms", status=403, repeat=True)
        await client.authenticate()
        await client.authenticate()
        assert len(requests_to(mocked, f"{ORIGIN}/cgi-bin-zm/nph-zms")) == 1

    assert client.get_stream_url(1).startswith(f"{ORIGIN}/cgi-bin-zm/nph-zms?mode=jpeg")


async def test_zms_path_falls_back_to_the_web_path(
    client: ZoneMinderLegacyClient,
) -> None:
    """When nothing answers, use the Debian layout under the web path."""
    with aioresponses() as mocked:
        _login(mocked)
        await client.authenticate()
    assert client.get_stream_url(1).startswith(f"{ZM}/cgi-bin/nph-zms?")


@pytest.mark.parametrize(
    ("zms_path", "expected"),
    [
        ("/cgi-bin-zm/nph-zms", f"{ORIGIN}/cgi-bin-zm/nph-zms"),
        (
            "https://cams.example/cgi-bin/nph-zms",
            "https://cams.example/cgi-bin/nph-zms",
        ),
    ],
)
async def test_zms_path_can_be_given(
    session: aiohttp.ClientSession, zms_path: str, expected: str
) -> None:
    """A path given by the caller is used as is, without probing."""
    client = ZoneMinderLegacyClient(ZM, "admin", "hunter2", session, zms_path=zms_path)
    with aioresponses() as mocked:
        _login(mocked)
        await client.authenticate()
        assert not [verb for verb, _url in mocked.requests if verb == "GET"]
    assert client.get_stream_url(1).startswith(f"{expected}?mode=jpeg")


async def test_old_login_credentials_are_not_sent_as_a_token(
    client: ZoneMinderLegacyClient,
) -> None:
    """Before 1.34, login gave only an auth= string, which isn't a token.

    Those servers authenticate by session cookie instead.
    """
    with aioresponses() as mocked:
        mocked.post(
            f"{ZM}/api/host/login.json",
            payload={"credentials": "auth=abc123", "append_password": 0},
        )
        mocked.get(v2(r"api/host/getVersion\.json"), payload={"version": "1.32.3"})
        assert await client.get_version() == "1.32.3"
        (call,) = sent(mocked, "GET", "api/host/getVersion.json")
    assert "token" not in call.kwargs["params"]


# -- Server health --------------------------------------------------------------


async def test_server_info_says_if_zoneminder_is_running(
    client: ZoneMinderLegacyClient,
) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/host/getVersion\.json"), payload={"version": "1.38.1"})
        mocked.get(v2(r"api/host/daemonCheck\.json"), payload={"result": 1})
        mocked.get(v2(r"api/host/getVersion\.json"), payload={"version": "1.38.1"})
        mocked.get(v2(r"api/host/daemonCheck\.json"), payload={"result": 0})
        assert (await client.get_server_info()).running
        assert not (await client.get_server_info()).running


async def test_server_info_without_daemon_check(client: ZoneMinderLegacyClient) -> None:
    """Without System rights daemonCheck fails; the version still comes back."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/host/getVersion\.json"), payload={"version": "1.38.1"})
        mocked.get(v2(r"api/host/daemonCheck\.json"), status=500)
        info = await client.get_server_info()
    assert info.version == "1.38.1"
    assert not info.running


# -- Multi-server ---------------------------------------------------------------

SERVERS_RESPONSE = {
    "servers": [
        {
            "Server": {
                "Id": "1",
                "Name": "main",
                "Protocol": "http",
                "Hostname": "zm.local",
                "Port": None,
                "PathToIndex": "/zm/index.php",
                "PathToZMS": "/zm/cgi-bin/nph-zms",
            }
        },
        {
            "Server": {
                "Id": "2",
                "Name": "garage",
                "Protocol": "https",
                "Hostname": "garage.local",
                "Port": "8443",
                "PathToIndex": "/zoneminder/index.php",
                "PathToZMS": "/cgi-bin-zm/nph-zms",
            }
        },
    ]
}


def _monitor_on(
    monitor_id: int, server_id: int, *, controllable: bool = False
) -> dict[str, Any]:
    item = _monitor_item(monitor_id)
    item["Monitor"]["ServerId"] = str(server_id)
    item["Monitor"]["Controllable"] = "1" if controllable else "0"
    return item


async def test_streams_come_from_the_monitors_own_server(
    client: ZoneMinderLegacyClient,
) -> None:
    """In a multi-server setup each camera streams from the server recording it."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={
                "monitors": [_monitor_on(1, 0), _monitor_on(2, 2), _monitor_on(3, 9)]
            },
        )
        mocked.get(v2(r"api/servers\.json"), payload=SERVERS_RESPONSE)
        await client.authenticate()
        await client.get_monitors()

    assert client.get_stream_url(1).startswith(f"{ZM}/cgi-bin/nph-zms?")
    assert client.get_stream_url(2).startswith(
        "https://garage.local:8443/cgi-bin-zm/nph-zms?"
    )
    # A server id ZoneMinder doesn't list falls back to the main server.
    assert client.get_stream_url(3).startswith(f"{ZM}/cgi-bin/nph-zms?")


async def test_single_server_never_asks_for_servers(
    client: ZoneMinderLegacyClient,
) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={"monitors": [_monitor_on(1, 0), _monitor_item(2)]},
        )
        await client.get_monitors()
        assert not sent(mocked, "GET", "api/servers.json")


async def test_servers_list_unavailable(client: ZoneMinderLegacyClient) -> None:
    """Without the servers list, streams use the main server."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors\.json"), payload={"monitors": [_monitor_on(2, 2)]})
        mocked.get(v2(r"api/servers\.json"), status=500)
        assert [m.id for m in await client.get_monitors()] == [2]
    assert client.get_stream_url(2).startswith(f"{ZM}/cgi-bin/nph-zms?")


async def test_ptz_goes_to_the_monitors_own_server(
    client: ZoneMinderLegacyClient,
) -> None:
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors\.json"),
            payload={"monitors": [_monitor_on(2, 2, controllable=True)]},
        )
        mocked.get(v2(r"api/servers\.json"), payload=SERVERS_RESPONSE)
        remote = "https://garage.local:8443/zoneminder/index.php"
        mocked.post(
            re.compile(rf"^{re.escape(remote)}(\?.*)?$"), payload={"result": "Ok"}
        )
        await client.get_monitors()
        await client.ptz(2, "up")
        (call,) = [
            c
            for (verb, called), calls in mocked.requests.items()
            if verb == "POST" and str(called).startswith(remote)
            for c in calls
        ]
    assert call.kwargs["data"]["control"] == "moveConUp"
    assert call.kwargs["params"] == {"token": "token"}


# -- Stream scale and frame rate --------------------------------------------------


async def test_stream_scale_and_frame_rate(session: aiohttp.ClientSession) -> None:
    """Scaled-down, slower MJPEG saves bandwidth when viewing remotely."""
    client = ZoneMinderLegacyClient(
        ZM, "admin", "hunter2", session, stream_scale=50, stream_maxfps=5
    )
    live = client.get_stream_url(1)
    still = client.get_stream_url(1, mode="single")
    assert "scale=50" in live
    assert "maxfps=5" in live
    assert "scale=50" in still
    # A single frame has no frame rate.
    assert "maxfps" not in still


async def test_default_stream_is_full_size(client: ZoneMinderLegacyClient) -> None:
    live = client.get_stream_url(1)
    assert "scale" not in live
    assert "maxfps=30" in live


@pytest.mark.parametrize("action", ["start", "stop", "restart"])
async def test_server_control(client: ZoneMinderLegacyClient, action: str) -> None:
    """Start, stop and restart go to zmpkg.pl through the states API.

    Like a run state switch, the reply waits for the daemons to restart.
    """
    with aioresponses() as mocked:
        _login(mocked)
        mocked.post(v2(rf"api/states/change/{action}\.json"), payload={})
        await client.server_control(action)
        (call,) = sent(mocked, "POST", f"api/states/change/{action}.json")
    assert call.kwargs["timeout"].total >= 60


async def test_server_control_rejects_other_actions(
    client: ZoneMinderLegacyClient,
) -> None:
    with pytest.raises(ValueError, match="reboot"):
        await client.server_control("reboot")


async def test_failed_logout_logs_no_token(
    client: ZoneMinderLegacyClient, caplog: pytest.LogCaptureFixture
) -> None:
    """aiohttp puts the URL, token and all, in a failed logout's error."""
    caplog.set_level("DEBUG", logger="aiozoneminder")
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/host/logout\.json"), status=500)
        await client.authenticate()
        await client.close()
    assert "Logout failed" in caplog.text
    assert "token=token" not in caplog.text


async def test_api_errors_carry_no_token(client: ZoneMinderLegacyClient) -> None:
    """Nothing in a raised error's chain holds the token-bearing URL."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(v2(r"api/monitors\.json"), status=500)
        with pytest.raises(ZMApiError) as caught:
            await client.get_monitors()
    chain: list[BaseException] = []
    err: BaseException | None = caught.value
    while err is not None:
        chain.append(err)
        # The chain a traceback prints.
        err = err.__cause__ or (None if err.__suppress_context__ else err.__context__)
    assert all("token=token" not in str(e) for e in chain)


async def test_set_monitor_state_takes_any_reply_shape(
    client: ZoneMinderLegacyClient,
) -> None:
    """A save answered with something other than a JSON object isn't an error."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/monitors/1\.json"),
            payload={"monitor": {"Monitor": _monitor_item(1)["Monitor"]}},
        )
        mocked.post(v2(r"api/monitors/1\.json"), payload="Saved")
        await client.set_monitor_state(1, recording="Always")


async def test_event_paging_keeps_what_it_has_on_a_missing_page(
    client: ZoneMinderLegacyClient,
) -> None:
    """A page that went away (events deleted mid-paging) ends the list."""
    with aioresponses() as mocked:
        _login(mocked)
        mocked.get(
            v2(r"api/events\.json"),
            payload={
                "events": [_event(2, "2026-10-24 09:00:00")],
                "pagination": {"nextPage": True},
            },
        )
        mocked.get(v2(r"api/events\.json"), status=404)
        events = await client.get_events(limit=500)
    assert [e.id for e in events] == [2]
