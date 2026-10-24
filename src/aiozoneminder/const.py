"""Constants for talking to ZoneMinder."""

from typing import Final

# -- Backends ----------------------------------------------------------------

#: zm-api, ZoneMinder API v3: the standalone REST server (/api/v3).
BACKEND_V3: Final = "v3"
#: ZoneMinder API v2: the CakePHP API built into ZoneMinder (/zm/api).
BACKEND_LEGACY: Final = "legacy"
DEFAULT_BACKEND: Final = BACKEND_V3
BACKENDS: Final = [BACKEND_V3, BACKEND_LEGACY]
DEFAULT_V3_PORT: Final = 8080

# -- Timeouts, in seconds ----------------------------------------------------

DEFAULT_TIMEOUT: Final = 10
#: For endpoints that read from disk or start a session.
LONG_TIMEOUT: Final = 30

#: API v2 MJPEG frame rate cap unless the caller sets one.
DEFAULT_STREAM_MAXFPS: Final = 30

# -- ZoneMinder config names -------------------------------------------------

CONFIG_MQTT_TOPIC_PREFIX: Final = "ZM_MQTT_TOPIC_PREFIX"
CONFIG_MQTT_HOSTNAME: Final = "ZM_MQTT_HOSTNAME"
CONFIG_GO2RTC_PATH: Final = "ZM_GO2RTC_PATH"

# -- API v2 endpoints (joined with the host) ---------------------------------

API_LOGIN: Final = "api/host/login.json"
API_LOGOUT: Final = "api/host/logout.json"
API_VERSION: Final = "api/host/getVersion.json"
API_CONFIGS: Final = "api/configs.json"
API_MONITORS: Final = "api/monitors.json"
API_MONITOR: Final = "api/monitors/{monitor_id}.json"
API_EVENTS: Final = "api/events.json"
API_EVENT: Final = "api/events/{event_id}.json"
API_EVENT_COUNTS_UNARCHIVED: Final = (
    "api/events/consoleEvents/{interval}/Archived=:0.json"
)
API_ALARM_ON: Final = "api/monitors/alarm/id:{monitor_id}/command:on.json"
API_ALARM_OFF: Final = "api/monitors/alarm/id:{monitor_id}/command:off.json"
API_STATES: Final = "api/states.json"
API_SERVERS: Final = "api/servers.json"
API_DAEMON_CHECK: Final = "api/host/daemonCheck.json"
API_STATE_CHANGE: Final = "api/states/change/{name}.json"
STREAM_PATH: Final = "cgi-bin/nph-zms"
#: Where else packages serve nph-zms, from the server root: a web root
#: install, source builds, and RPM. STREAM_PATH under the web path is tried
#: first, which is where Debian and Ubuntu put it.
ZMS_PATHS: Final = ["/zm/cgi-bin/nph-zms", "/cgi-bin/nph-zms", "/cgi-bin-zm/nph-zms"]
#: ZoneMinder's web request handler, which takes PTZ commands.
WEB_REQUEST: Final = "index.php"
EVENT_VIDEO_PATH: Final = "api/events/{event_id}.mp4"

# -- API v3 (zm-api) endpoints (joined with the host) ------------------------

V3_LOGIN: Final = "api/v3/auth/login"
V3_REFRESH: Final = "api/v3/auth/refresh"
V3_LOGOUT: Final = "api/v3/auth/logout"
V3_VERSION: Final = "api/v3/host/getVersion"
V3_ME: Final = "api/v3/me"
V3_CONFIGS: Final = "api/v3/configs"

V3_MONITORS: Final = "api/v3/monitors"
V3_STATES: Final = "api/v3/states"
V3_STATE: Final = "api/v3/states/{state_id}"
V3_MONITOR: Final = "api/v3/monitors/{monitor_id}"
V3_MONITOR_ALARM: Final = "api/v3/monitors/{monitor_id}/alarm"
V3_MONITOR_SNAPSHOT: Final = "api/v3/monitors/{monitor_id}/snapshot"
V3_MONITOR_STATUS: Final = "api/v3/monitor-status"

V3_EVENTS: Final = "api/v3/events"
V3_EVENT_STREAM: Final = "api/v3/events/stream"
V3_EVENT: Final = "api/v3/events/{event_id}"
V3_EVENT_INFO: Final = "api/v3/events/{event_id}/info"
V3_EVENT_VIDEO: Final = "api/v3/events/{event_id}/video"
V3_EVENT_THUMBNAIL: Final = "api/v3/events/{event_id}/thumbnail"
V3_EVENT_PLAYLIST: Final = "api/v3/events/{event_id}/stream/playlist.m3u8"
V3_EVENT_SUMMARY: Final = "api/v3/event-summaries/{monitor_id}"
V3_EVENT_COUNTS_BY_MONITOR: Final = "api/v3/events/counts-by-monitor/{hours}"

V3_LIVE_START: Final = "api/v3/live/{monitor_id}/start"
V3_LIVE_STOP: Final = "api/v3/live/{monitor_id}/stop"
V3_LIVE_STATS: Final = "api/v3/live/{monitor_id}/stats"
V3_LIVE_HLS_MASTER: Final = "api/v3/live/{monitor_id}/hls/master.m3u8"
V3_LIVE_HLS_PLAYLIST: Final = "api/v3/live/{monitor_id}/hls/live.m3u8"
V3_LIVE_RTSP_LINK: Final = "api/v3/live/{monitor_id}/rtsp/link"
V3_LIVE_WHEP: Final = "api/v3/live/{monitor_id}/webrtc/whep"
V3_LIVE_WEBRTC_WS: Final = "api/v3/live/{monitor_id}/webrtc/ws"

V3_PTZ_MOVE: Final = "api/v3/ptz/monitors/{monitor_id}/move/{direction}"
V3_PTZ_ZOOM: Final = "api/v3/ptz/monitors/{monitor_id}/zoom/{direction}"
V3_PTZ_FOCUS: Final = "api/v3/ptz/monitors/{monitor_id}/focus/{direction}"
V3_PTZ_HOME: Final = "api/v3/ptz/monitors/{monitor_id}/home"
V3_PTZ_PRESET_GOTO: Final = "api/v3/ptz/monitors/{monitor_id}/presets/{preset}/goto"
V3_PTZ_CAPABILITIES: Final = "api/v3/ptz/monitors/{monitor_id}/capabilities"
V3_CONTROL_PRESETS: Final = "api/v3/control_presets"

V3_STORAGE: Final = "api/v3/storage"
V3_SYSTEM_STATUS: Final = "api/v3/system/status"
V3_SERVER_HEALTH: Final = "api/v3/server/health_check"
V3_SERVER_CONTROL: Final = "api/v3/server/control/{action}"

#: What server_control accepts: ZoneMinder's own start, stop and restart.
SERVER_ACTIONS: Final = ["start", "stop", "restart"]

# -- Monitor modes -----------------------------------------------------------
# ZoneMinder 1.37 split the single "Function" into separate fields. These
# values come from the zm-api OpenAPI schema.

CAPTURING_OPTIONS: Final = ["None", "Ondemand", "Always"]
ANALYSING_OPTIONS: Final = ["None", "Always"]
RECORDING_OPTIONS: Final = ["None", "OnMotion", "Always"]
DECODING_OPTIONS: Final = [
    "None",
    "Ondemand",
    "KeyFrames",
    # The database value; zm-api also takes "KeyFramesOndemand" but returns this.
    "KeyFrames+Ondemand",
    "Always",
]

#: The single-field modes from before 1.37, still used by API v2.
MONITOR_FUNCTIONS: Final = [
    "None",
    "Monitor",
    "Modect",
    "Record",
    "Mocord",
    "Nodect",
]

#: ZoneMinder orientation values, with readable labels.
ORIENTATION_LABELS: Final = {
    "Rotate0": "Normal",
    "Rotate90": "Rotate right",
    "Rotate180": "Inverted",
    "Rotate270": "Rotate left",
    "FlipHori": "Flipped horizontally",
    "FlipVert": "Flipped vertically",
    # ZoneMinder 1.36 and earlier used underscored names.
    "ROTATE_0": "Normal",
    "ROTATE_90": "Rotate right",
    "ROTATE_180": "Inverted",
    "ROTATE_270": "Rotate left",
    "FLIP_HORI": "Flipped horizontally",
    "FLIP_VERT": "Flipped vertically",
}

#: Rotation in degrees per orientation, for turning live video upright.
ORIENTATION_DEGREES: Final = {
    "Rotate0": 0,
    "Rotate90": 90,
    "Rotate180": 180,
    "Rotate270": 270,
    "ROTATE_0": 0,
    "ROTATE_90": 90,
    "ROTATE_180": 180,
    "ROTATE_270": 270,
}

# -- PTZ ---------------------------------------------------------------------

#: Speed, in percent, for moves, zoom and focus when the caller gives none.
#: ZoneMinder's control modules treat a missing speed as 0, which stops the
#: camera instead of moving it.
PTZ_DEFAULT_SPEED: Final = 50

#: The PTZ commands both backends accept.
PTZ_MOVE_COMMANDS: Final = [
    "up",
    "down",
    "left",
    "right",
    "up-left",
    "up-right",
    "down-left",
    "down-right",
    "stop",
]
PTZ_ZOOM_COMMANDS: Final = ["zoom-in", "zoom-out", "zoom-stop"]
PTZ_FOCUS_COMMANDS: Final = ["focus-near", "focus-far", "focus-auto", "focus-stop"]
PTZ_OTHER_COMMANDS: Final = ["home", "preset"]
PTZ_COMMANDS: Final = [
    *PTZ_MOVE_COMMANDS,
    *PTZ_ZOOM_COMMANDS,
    *PTZ_FOCUS_COMMANDS,
    *PTZ_OTHER_COMMANDS,
]

# -- Streams -----------------------------------------------------------------

STREAM_TYPE_HLS: Final = "hls"
STREAM_TYPE_RTSP: Final = "rtsp"
STREAM_TYPE_GO2RTC: Final = "go2rtc"
STREAM_TYPE_MJPEG: Final = "mjpeg"
