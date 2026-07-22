#!/usr/bin/env python3
"""
belabox-hotplug: watches for any external USB video capture device AND the
onboard HDMI-in port coming and going, and tells belaUI (over its local
websocket API) to stop/restart streaming accordingly.

Two independent detection mechanisms are used, because they're fundamentally
different kinds of events:

  - USB sources: real udev add/remove events (the device node appears and
    disappears from the system).
  - The onboard HDMI-in (rk_hdmirx driver): the /dev/videoX node for it is
    always present, cable or no cable, so udev events don't apply. Instead
    this polls the driver's `power_present` V4L2 control, which reflects
    whether the connected source is actually driving power/signal on the
    HDMI line (verified against this board's hardware - the /sys/class/hdmirx
    status file it looked like we could use instead does NOT reflect real
    cable/signal state, it stays "connected" even with nothing plugged in).

When a source is confirmed active, its supported resolution is probed via
`v4l2-ctl --list-formats-ext` and matched against belaUI's live pipeline list
(cached from the 'pipelines' websocket message) by source-type keyword and
closest resolution, instead of using one hardcoded pipeline.

Run via the accompanying systemd unit (belabox-hotplug.service), which
should be installed to start at boot alongside belaUI.

Copyright (c) 2026 Jason Ardon (W3ndees)
Licensed under the MIT License - see LICENSE in this repository.
"""
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import threading

import pyudev
import websockets

CONFIG_PATH = "/etc/belabox-hotplug/config.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("belabox-hotplug")


def load_config():
    if not os.path.exists(CONFIG_PATH):
        log.error(f"No config found at {CONFIG_PATH}. Run bootstrap_token.py first.")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        return json.load(f)


class BelaUIClient:
    """
    Thin wrapper around the belaUI websocket protocol.
    Keeps a cache of the last-seen config/pipelines/status messages so we
    know what settings to reuse when we issue our own start command.
    """

    def __init__(self, ws_url: str, auth_token: str):
        self.ws_url = ws_url
        self.auth_token = auth_token
        self.ws = None
        self.authenticated = asyncio.Event()

        self.last_config = {}
        self.last_pipelines = {}
        self.is_streaming = False
        # belaUI's current list of audio source names (e.g. "HDMI",
        # the connected USB device's product name, "No audio"), broadcast
        # in 'status' messages and
        # refreshed by belaUI itself on USB audio hotplug. Needed so
        # start_stream() can send the *correct* asrc for whichever source is
        # now active - see pick_asrc().
        self.available_asrcs = []
        # Gets one item pushed onto it on every successful authentication -
        # the first connection AND every reconnect after belaUI restarts or
        # drops the connection. A physically-connected source doesn't emit
        # any USB/HDMI event when belaUI itself goes away and comes back, so
        # this is what lets the daemon notice "belaUI is back but not
        # streaming, and something is still plugged in" instead of only
        # reconciling once at its own startup. See reconcile_on_reconnect().
        self.reconnect_queue = asyncio.Queue()

    async def connect_and_listen(self):
        """Runs forever: connects, authenticates, and processes incoming
        messages, reconnecting automatically if the connection drops."""
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.authenticated.clear()
                    await ws.send(json.dumps({"auth": {"token": self.auth_token}}))
                    log.info("Connected to belaUI, authenticating...")

                    async for raw in ws:
                        await self._handle_message(json.loads(raw))

            except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
                log.warning(f"belaUI connection lost/unavailable ({e}), retrying in 5s...")
                self.ws = None
                self.authenticated.clear()
                await asyncio.sleep(5)

    async def _handle_message(self, msg: dict):
        if "auth" in msg:
            auth = msg["auth"]
            if auth.get("success") is True:
                log.info("Authenticated with belaUI")
                self.authenticated.set()
                self.reconnect_queue.put_nowait(True)
            else:
                log.error(f"belaUI auth failed: {auth}. Re-run bootstrap_token.py")

        if "config" in msg:
            self.last_config = msg["config"]

        if "pipelines" in msg:
            self.last_pipelines = msg["pipelines"]
            log.info(f"Got {len(self.last_pipelines)} pipelines from belaUI")

        if "status" in msg:
            status = msg["status"]
            if "is_streaming" in status:
                self.is_streaming = status["is_streaming"]
                log.info(f"Stream state update: is_streaming={self.is_streaming}")
            if "asrcs" in status:
                self.available_asrcs = status["asrcs"]

    async def wait_ready(self, timeout=15):
        try:
            await asyncio.wait_for(self.authenticated.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.warning("Timed out waiting for belaUI authentication")

    async def stop_stream(self):
        if not self.ws:
            log.warning("Can't stop stream, not connected to belaUI")
            return
        log.info("Sending stop to belaUI")
        await self.ws.send(json.dumps({"stop": 0}))

    async def start_stream(self, pipeline: str, asrc: str = None):
        if not self.ws:
            log.warning("Can't start stream, not connected to belaUI")
            return

        # Reuse the last known settings (bitrate, SRT/SRTLA config, etc.)
        # and only override the pipeline field (and asrc, if given - needed
        # when switching from a source with one audio device to a source
        # with a different one, see pick_asrc()).
        cfg = dict(self.last_config)
        cfg["pipeline"] = pipeline
        if asrc is not None:
            cfg["asrc"] = asrc

        log.info(f"Sending start to belaUI with pipeline={pipeline}"
                 f"{f', asrc={asrc}' if asrc is not None else ''}")
        await self.ws.send(json.dumps({"start": cfg}))


# ---------------------------------------------------------------------------
# USB source detection
# ---------------------------------------------------------------------------

def is_external_usb_video_device(device) -> bool:
    """True if this video4linux device hangs off a USB device (as opposed to
    the onboard HDMI-in, which is a platform device with no USB ancestor).
    Matches any USB capture card/action-cam/dongle, not one specific
    vendor/product."""
    return device.find_parent(subsystem="usb", device_type="usb_device") is not None


def node_supports_capture(device_node: str) -> bool:
    """Some USB capture chips expose 2+ /dev/videoN nodes for one physical
    device (e.g. a real capture stream node plus a metadata-only node that
    errors out on format queries). This is the cheapest reliable way found
    to tell them apart: try to query the current format and see if the
    driver accepts it."""
    try:
        result = subprocess.run(
            ["v4l2-ctl", "-d", device_node, "--get-fmt-video"],
            capture_output=True, timeout=2,
        )
        return result.returncode == 0
    except Exception:
        return False


def find_working_usb_node():
    """Scans every external USB video4linux device currently present,
    grouped by physical USB device, and returns the device node of the
    first one that actually supports capture (not just the first node that
    happens to exist). Returns None if no external USB source is usable
    right now."""
    context = pyudev.Context()
    groups = {}
    for device in context.list_devices(subsystem="video4linux"):
        if not is_external_usb_video_device(device):
            continue
        usb_parent = device.find_parent(subsystem="usb", device_type="usb_device")
        groups.setdefault(usb_parent.sys_path, []).append(device.device_node)

    for usb_path, nodes in groups.items():
        for node in sorted(nodes):
            if node_supports_capture(node):
                return node
    return None


def udev_watcher_thread(loop: asyncio.AbstractEventLoop, event_queue: asyncio.Queue):
    """Runs in a background thread (pyudev's observer is not asyncio-native)
    and pushes add/remove events for any external USB video4linux device
    into the asyncio queue. Doesn't care which node fired - handle_events
    rescans all candidates once settled, so it naturally copes with a
    device that shows up as several /dev/videoN nodes at once."""
    context = pyudev.Context()
    monitor = pyudev.Monitor.from_netlink(context)
    monitor.filter_by(subsystem="video4linux")

    def device_event(device):
        action = device.action  # 'add' or 'remove'
        if action not in ("add", "remove"):
            return
        if not is_external_usb_video_device(device):
            return  # this is the onboard HDMI-in node, handled separately

        log.info(f"udev event: {action} for {device.device_node} (external USB video device)")
        loop.call_soon_threadsafe(event_queue.put_nowait, ("usb", action))

    observer = pyudev.MonitorObserver(monitor, callback=device_event)
    observer.start()
    log.info("udev watcher started (watching for any external USB video4linux device)")

    # Keep the thread alive; the observer runs its own internal thread too,
    # but we block here so this function (and thus this thread) doesn't exit.
    threading.Event().wait()


# ---------------------------------------------------------------------------
# HDMI source detection
# ---------------------------------------------------------------------------

def find_hdmi_device():
    """Locates the onboard HDMI-in device node by walking up from each
    video4linux device to find one bound to the rk_hdmirx platform driver,
    rather than assuming it's always /dev/video0."""
    context = pyudev.Context()
    for device in context.list_devices(subsystem="video4linux"):
        parent = device.find_parent(subsystem="platform")
        if parent is not None and parent.driver == "rk_hdmirx":
            return device.device_node
    return None


def get_hdmi_power_present(hdmi_device: str) -> bool:
    """Polls the rk_hdmirx driver's read-only `power_present` V4L2 control,
    which reflects whether the connected source is actually driving
    power/signal on the HDMI line right now. This was verified against this
    board's hardware to be reliable where the /sys/class/hdmirx/hdmirx/status
    sysfs file was NOT: that file stayed stuck on "connected" for over a
    minute after physically unplugging the cable, while power_present
    correctly read 0 immediately and flipped back to 1 once a live signal
    was actually present again."""
    try:
        result = subprocess.run(
            ["v4l2-ctl", "-d", hdmi_device, "--get-ctrl=power_present"],
            capture_output=True, text=True, timeout=2,
        )
        if result.returncode != 0:
            return False
        value = result.stdout.strip().split(":")[-1].strip()
        return value == "1"
    except Exception as e:
        log.warning(f"Failed to read power_present from {hdmi_device}: {e}")
        return False


async def poll_hdmi(hdmi_device: str, poll_interval: float, event_queue: asyncio.Queue,
                     initial_present: bool):
    """Background asyncio task: polls power_present at a fixed interval and
    pushes an 'add'/'remove' event whenever it changes state. Takes the
    already-known startup state so it doesn't log/emit a spurious
    transition for state that was already established before this task
    started."""
    last_present = initial_present
    while True:
        present = await asyncio.to_thread(get_hdmi_power_present, hdmi_device)
        if present != last_present:
            action = "add" if present else "remove"
            log.info(f"HDMI signal {'detected' if present else 'lost'} on {hdmi_device}")
            await event_queue.put(("hdmi", action))
            last_present = present
        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Format probing + pipeline matching
# ---------------------------------------------------------------------------

def probe_source_resolution(device_node: str):
    """Runs `v4l2-ctl --list-formats-ext` on the given node and returns the
    (width, height, fps) of its first listed format. On this hardware both
    the USB card and the HDMI-in report their best/current format first
    (USB: highest native resolution; HDMI: the live-negotiated signal
    format), so this doesn't need special-casing per source type."""
    try:
        result = subprocess.run(
            ["v4l2-ctl", "-d", device_node, "--list-formats-ext"],
            capture_output=True, text=True, timeout=3,
        )
    except Exception as e:
        log.warning(f"Failed to probe formats on {device_node}: {e}")
        return None, None, None

    size_match = re.search(r"Size:\s*Discrete\s*(\d+)x(\d+)", result.stdout)
    if not size_match:
        return None, None, None
    width, height = int(size_match.group(1)), int(size_match.group(2))

    fps_match = re.search(r"Discrete\s*[\d.]+s\s*\(([\d.]+)\s*fps\)", result.stdout)
    fps = float(fps_match.group(1)) if fps_match else None

    return width, height, fps


def parse_pipeline_resolution(name: str):
    """Extracts (height, fps) from a pipeline name like
    'rk3588/h265_usb_mjpeg_1080p30' -> (1080, 30.0). Returns (None, None)
    for bare pipelines with no resolution suffix, e.g. 'rk3588/h265_hdmi'."""
    m = re.search(r"(\d{3,4})p(\d+(?:\.\d+)?)?", name)
    if not m:
        return None, None
    height = int(m.group(1))
    fps = float(m.group(2)) if m.group(2) else None
    return height, fps


def pick_pipeline(source_type: str, device_node: str, pipelines: dict):
    """
    Picks the belaUI pipeline that best matches the given active source.

    `pipelines` is the {id: {name, asrc, acodec}} dict cached from belaUI's
    'pipelines' websocket message (BelaUIClient.last_pipelines). Matching is
    keyword-based on source type ('_usb_' vs '_hdmi_' in the pipeline name,
    e.g. "rk3588/h265_usb_mjpeg_1080p30" / "rk3588/h265_hdmi_1080p30") plus
    closest resolution - this is a simple heuristic for the MVP, not a
    format-accurate match (e.g. it won't distinguish MJPEG-only USB cards
    from H264 UVC action cams by codec, only by the "_usb_"/"_hdmi_" keyword
    belacoder's own pipeline names happen to use).

    Returns (pipeline_id, pipeline_name), or (None, None) if nothing usable
    was found at all (e.g. belaUI hasn't sent its pipeline list yet).
    """
    if not pipelines:
        log.warning("No pipeline list cached from belaUI yet, can't pick a pipeline")
        return None, None

    width, height, fps = probe_source_resolution(device_node)
    if height is None:
        log.warning(f"Could not determine resolution for {device_node}; "
                    f"matching on source type only")

    keyword = "_usb_" if source_type == "usb" else "_hdmi_"
    candidates = {pid: p for pid, p in pipelines.items() if keyword in p["name"]}

    if not candidates:
        log.warning(f"No pipelines matched keyword '{keyword}' for {source_type} source; "
                    f"falling back to closest-resolution match across ALL pipelines "
                    f"rather than failing silently")
        candidates = pipelines

    if height is None:
        pid, p = next(iter(candidates.items()))
        log.warning(f"No resolution to match on either; falling back to first "
                    f"available candidate pipeline: {p['name']}")
        return pid, p["name"]

    def distance(item):
        _, p = item
        p_height, p_fps = parse_pipeline_resolution(p["name"])
        if p_height is None:
            return (float("inf"), float("inf"))  # rank bare/no-resolution pipelines last
        fps_diff = abs((p_fps or 30.0) - (fps or 30.0))
        return (abs(p_height - height), fps_diff)

    best_id, best = min(candidates.items(), key=distance)
    log.info(f"Matched {source_type} source ({width}x{height}@{fps}) "
             f"to pipeline '{best['name']}'")
    return best_id, best["name"]


# belaUI's fixed onboard audio source names (see its own audioSrcAliases /
# addAudioCardById in belaUI.js) - anything else in the asrcs list is a
# hotplugged external device (e.g. a USB action cam's own audio interface).
_ONBOARD_ASRC_NAMES = {"HDMI", "Analog in", "No audio", "Pipeline default"}


def pick_asrc(source_type: str, available_asrcs: list):
    """
    Picks the belaUI audio source name ("asrc") to send in the start
    command for the given active source type.

    This exists because belaUI's own "start" handler will hang indefinitely
    probing for the *previous* asrc if it's stale (verified on this board:
    switching from HDMI to a USB action cam while blindly reusing the old
    cached "asrc": "HDMI" made belaUI's startStream() call
    `asrcProbe("HDMI")` and just never return, since no HDMI audio device
    exists anymore - the stream silently never actually started even though
    belaUI already reported is_streaming=True).

    `available_asrcs` is belaUI's current list of audio source names (from
    its 'status' message, BelaUIClient.available_asrcs) - it's refreshed by
    belaUI itself whenever a USB audio device is plugged/unplugged.

    Returns an asrc name, or None if no confident choice could be made (in
    which case the caller should fall back to "No audio" rather than send
    nothing and risk reusing a stale value).
    """
    if source_type == "hdmi":
        if "HDMI" in available_asrcs:
            return "HDMI"
        log.warning("'HDMI' not in belaUI's current asrcs list; "
                    f"available: {available_asrcs}")
        return None

    # source_type == "usb": look for whichever asrc isn't one of the fixed
    # onboard names - that's the hotplugged device's own audio interface.
    external = [a for a in available_asrcs if a not in _ONBOARD_ASRC_NAMES]
    if len(external) == 1:
        return external[0]
    if len(external) > 1:
        log.warning(f"Multiple external audio sources present ({external}); "
                    f"picking '{external[0]}' - ambiguous with more than one "
                    f"USB audio device connected at once")
        return external[0]

    log.warning(f"No external audio source found in belaUI's asrcs list "
                f"({available_asrcs}) for the active USB video source; "
                f"falling back to 'No audio' rather than risk hanging on a "
                f"stale asrc")
    return "No audio" if "No audio" in available_asrcs else None


# ---------------------------------------------------------------------------
# Source arbitration
# ---------------------------------------------------------------------------

class SourceState:
    def __init__(self, hdmi_device: str):
        self.hdmi_device = hdmi_device
        self.hdmi_present = False
        self.usb_node = None
        self.active_source = None  # None, "hdmi", or "usb"


async def evaluate_and_apply(state: SourceState, client: BelaUIClient, priority: str):
    """
    Decides which source (if any) should be streaming and only issues a
    start/stop to belaUI if that decision actually changed - this runs
    after every USB or HDMI event, so it needs to be a no-op when e.g. the
    USB metadata node disappears but the real capture node (and thus the
    USB source as a whole) is still present.
    """
    if state.hdmi_present and state.usb_node:
        chosen = priority
        log.info(f"Both HDMI and USB sources present; priority='{priority}' wins")
    elif state.hdmi_present:
        chosen = "hdmi"
    elif state.usb_node:
        chosen = "usb"
    else:
        chosen = None

    if chosen == state.active_source:
        return

    # belaUI's own "start" websocket handler is a no-op whenever it already
    # considers itself streaming (see belaUI.js: `if (isStreaming ...) return;`)
    # - including while it's internally erroring/auto-retrying the *previous*
    # pipeline after the old source disappeared (verified on this board: losing
    # HDMI mid-stream makes belacoder throw repeated "gstreamer error from
    # v4l2src0/alsasrc0", and belaUI just keeps relaunching the same stale
    # HDMI pipeline in a retry loop of its own, completely ignoring a "start"
    # sent with a new pipeline). So switching from one already-active source
    # to another must explicitly stop and wait for confirmation first - just
    # sending "start" with the new pipeline is silently ignored otherwise.
    if client.is_streaming:
        log.info(f"Stopping current stream before switching "
                  f"({state.active_source} -> {chosen})")
        await client.stop_stream()
        if not await wait_for_stream_stopped(client):
            log.warning("belaUI did not confirm the stream stopped in time; "
                        "proceeding to start anyway")

    if chosen is None:
        state.active_source = None
        return

    device_node = state.hdmi_device if chosen == "hdmi" else state.usb_node
    pipeline_id, pipeline_name = pick_pipeline(chosen, device_node, client.last_pipelines)
    if pipeline_id is None:
        log.error(f"No usable pipeline found for {chosen} source at {device_node}, "
                  f"not starting stream")
        state.active_source = None
        return

    asrc = pick_asrc(chosen, client.available_asrcs)

    log.info(f"Switching active source to {chosen} ({device_node}), "
             f"pipeline={pipeline_name}, asrc={asrc}")
    await client.start_stream(pipeline_id, asrc=asrc)
    state.active_source = chosen


async def wait_for_stream_stopped(client: BelaUIClient, timeout: float = 10.0,
                                    poll_interval: float = 0.2) -> bool:
    """Polls BelaUIClient.is_streaming (updated from belaUI's 'status'
    messages) until it goes False, since belaUI ignores "start" while it
    still considers itself streaming."""
    elapsed = 0.0
    while client.is_streaming and elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    return not client.is_streaming


async def wait_for_pipelines(client: BelaUIClient, timeout: float = 5.0,
                               poll_interval: float = 0.2) -> bool:
    """Waits for belaUI's pipeline list to arrive after (re)connecting,
    since it's sent as a follow-up message after auth succeeds, not
    atomically with it."""
    elapsed = 0.0
    while not client.last_pipelines and elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    return bool(client.last_pipelines)


async def reconcile_now(state: SourceState, client: BelaUIClient, priority: str,
                          label: str = "Reconciling"):
    """Rescans both sources from scratch and applies whatever the result
    should be. Used both at daemon startup and on every belaUI reconnect
    (see reconcile_on_reconnect) - state.active_source is deliberately
    invalidated first so evaluate_and_apply() can't skip the decision as
    "unchanged" based on stale memory of what it last set, when what
    actually matters is whether belaUI itself is currently streaming."""
    log.info(f"{label}: rescanning USB/HDMI sources...")
    state.active_source = "unknown"
    state.usb_node = await asyncio.to_thread(find_working_usb_node)
    state.hdmi_present = await asyncio.to_thread(get_hdmi_power_present, state.hdmi_device)
    await evaluate_and_apply(state, client, priority)


async def reconcile_on_reconnect(client: BelaUIClient, state: SourceState, priority: str):
    """Runs once per successful (re)connection to belaUI *after* the very
    first one (which the daemon's startup sequence in main() already
    consumes and handles directly). A physically-connected source produces
    no USB/HDMI event when belaUI itself restarts or drops the connection,
    so without this, a belaUI crash/restart could leave the stream off
    indefinitely even though nothing about the actual video source
    changed."""
    while True:
        await client.reconnect_queue.get()
        if not await wait_for_pipelines(client):
            log.warning("Reconnected to belaUI but never got a pipeline list; "
                        "skipping reconcile")
            continue
        await reconcile_now(state, client, priority, label="Reconnected to belaUI")


async def handle_events(event_queue: asyncio.Queue, client: BelaUIClient,
                          state: SourceState, settle_delay: float, priority: str):
    while True:
        source_type, action = await event_queue.get()

        if action == "add":
            log.info(f"Waiting {settle_delay}s for {source_type} source to settle...")
            await asyncio.sleep(settle_delay)

        if source_type == "usb":
            state.usb_node = await asyncio.to_thread(find_working_usb_node)
            if action == "add" and state.usb_node is None:
                log.warning("USB add event fired, but no working capture node found "
                            "among the candidates")
        elif source_type == "hdmi":
            state.hdmi_present = await asyncio.to_thread(get_hdmi_power_present, state.hdmi_device)

        await evaluate_and_apply(state, client, priority)


async def main():
    config = load_config()

    ws_url = config.get("belaui_ws_url", "ws://127.0.0.1")
    auth_token = config["auth_token"]
    settle_delay = config.get("settle_delay_seconds", 2.0)
    hdmi_poll_interval = config.get("hdmi_poll_interval_seconds", 1.0)

    # Which source wins if both HDMI and USB are active at once. HDMI
    # defaults to winning here on the assumption that a dedicated
    # camera/camcorder plugged into the onboard HDMI-in is the primary,
    # higher-quality path, and a USB capture dongle/action-cam is more often
    # a secondary/backup source. Flip "source_priority" to "usb" in
    # config.json to invert this if that assumption doesn't hold for a
    # given setup.
    priority = config.get("source_priority", "hdmi")
    if priority not in ("hdmi", "usb"):
        log.warning(f"Invalid source_priority '{priority}' in config, defaulting to 'hdmi'")
        priority = "hdmi"

    hdmi_device = config.get("hdmi_device") or find_hdmi_device()
    if not hdmi_device:
        log.error("Could not find the onboard HDMI-in device (rk_hdmirx driver). "
                  "Set 'hdmi_device' explicitly in config.json if autodetection is wrong.")
        sys.exit(1)
    log.info(f"Using {hdmi_device} as the onboard HDMI-in device")

    client = BelaUIClient(ws_url, auth_token)
    loop = asyncio.get_event_loop()
    event_queue = asyncio.Queue()
    state = SourceState(hdmi_device)

    # Connect first and wait for belaUI's initial 'pipelines' message to
    # arrive before evaluating any state, otherwise a source that's already
    # plugged in at startup would race pick_pipeline() against an empty
    # pipeline cache and fail to start.
    connect_task = asyncio.create_task(client.connect_and_listen())
    await client.wait_ready()
    await wait_for_pipelines(client)

    # Consume the reconnect-queue item that corresponds to this same first
    # connection ourselves, and reconcile synchronously here - before the
    # watcher thread below is started. find_working_usb_node() creates its
    # own pyudev.Context(), and doing that at the exact same moment the
    # watcher thread is initializing its own Context()/Monitor was found
    # (via testing on this board) to intermittently deadlock somewhere in
    # the underlying libudev bindings. Once the watcher thread is already up
    # and just blocked listening for events, creating additional
    # short-lived Contexts alongside it is fine - which is why every
    # *subsequent* reconnect (belaUI restarting/crashing later, handled by
    # reconcile_on_reconnect below) doesn't need this same care.
    await client.reconnect_queue.get()
    await reconcile_now(state, client, priority, label="Startup")

    # udev watching happens in a background thread since pyudev's observer
    # isn't asyncio-native; it pushes events into our asyncio queue safely.
    watcher_thread = threading.Thread(
        target=udev_watcher_thread,
        args=(loop, event_queue),
        daemon=True,
    )
    watcher_thread.start()

    await asyncio.gather(
        connect_task,
        poll_hdmi(hdmi_device, hdmi_poll_interval, event_queue, state.hdmi_present),
        handle_events(event_queue, client, state, settle_delay, priority),
        reconcile_on_reconnect(client, state, priority),
    )


if __name__ == "__main__":
    asyncio.run(main())
