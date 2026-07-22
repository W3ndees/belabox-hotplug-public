# belabox-hotplug

Watches for video sources coming and going - any external USB capture
card/action-cam/dongle, AND the onboard HDMI-in port - and tells belaUI to
stop/restart the stream accordingly, picking a matching pipeline
automatically instead of one hardcoded pipeline.

Built for rk3588 BELABOX-based streaming rigs (tested on a Radxa ROCK 5B+),
so you don't have to manually stop/restart the stream every time you swap
cameras or a cable comes loose.

Also included: `fan-lock.sh` / `fan-lock.service`, a tiny standalone utility
that pins the board's fan to a fixed cooling state at boot instead of
letting the kernel's thermal governor ramp it up and down. Unrelated to the
hotplug daemon otherwise - use either piece independently.

## How detection works

**USB sources** (capture dongles, action cams like DJI/GoPro/Insta360,
webcams, etc.) are detected via real udev add/remove events. Any external
USB video4linux device triggers a rescan - there's no vendor/product ID
allowlist, so any USB capture source works out of the box. Some USB capture
chips expose more than one `/dev/videoN` node for the same physical device
(e.g. a real capture stream plus a metadata-only node); on each rescan every
candidate node is tried with `v4l2-ctl --get-fmt-video` and the first one
that actually supports capture is used.

**The onboard HDMI-in** (`rk_hdmirx` driver) is different: its `/dev/videoX`
node always exists, cable or no cable, so udev add/remove events don't apply
to it. Instead, the daemon polls the driver's read-only `power_present` V4L2
control (`v4l2-ctl -d <node> --get-ctrl=power_present`), which reflects
whether the connected source is actually driving power/signal on the HDMI
line right now. This was verified against real hardware:
`/sys/class/hdmirx/hdmirx/status` looked like an obvious candidate but is
**not** reliable - it stayed stuck on `"connected"` for over a minute after
physically unplugging the cable. `power_present` correctly read `0`
immediately and flipped back to `1` as soon as a real signal reappeared.

**Pipeline selection**: once a source is confirmed active, its supported
resolution is probed with `v4l2-ctl --list-formats-ext` and matched against
belaUI's live pipeline list (from its `pipelines` websocket message) by
source-type keyword (`_usb_` vs `_hdmi_` in the pipeline name, e.g.
`rk3588/h265_usb_mjpeg_1080p30` / `rk3588/h265_hdmi_1080p30`) and closest
resolution. Note: belaUI's `pipeline` field in the `start` command is
actually the **sha1 hash of the pipeline's name** (e.g.
`sha1("rk3588/h265_usb_mjpeg_1080p30")`), not the readable string - the
daemon looks this up from the cached pipeline list itself, so you never need
to compute or configure this by hand.

If no pipeline matches the source-type keyword at all, the daemon logs a
warning and falls back to the closest resolution match across ALL known
pipelines rather than failing silently.

**Priority if both are connected at once**: HDMI wins by default. The
reasoning: a dedicated camera/camcorder plugged into the onboard HDMI-in is
assumed to be the primary, higher-quality path, with a USB capture
dongle/action-cam as more of a secondary/backup source. This is easy to
flip - set `"source_priority": "usb"` in config.json to invert it. Whenever
both sources are present, the daemon logs which one won and why.

**Recovering from a belaUI restart/crash**: a physically-connected source
produces no USB/HDMI event when belaUI itself goes away and comes back (a
software update, a crash, a manual restart), so the daemon re-checks both
sources from scratch on every (re)connection to belaUI, not just its own
startup - otherwise a source could stay plugged in the whole time and the
stream would just stay off until some unrelated device event happened to
come along. Look for `Reconnected to belaUI: rescanning USB/HDMI sources...`
in the logs to confirm this fired.

**What's not handled**: changing a camera's resolution/fps *without*
unplugging it (e.g. via the camera's own menu) isn't detected - neither USB
nor HDMI monitoring notices an in-place format change on an
already-connected source, only connect/disconnect. The daemon keeps running
the old pipeline, which will start erroring since its caps no longer match
the real source, until the device is unplugged/replugged or the daemon is
restarted.

## 1. Copy files to the board

```bash
# "belabox" below is a placeholder - use your board's actual hostname/IP,
# or an SSH config alias if you've set one up
scp -r belabox-hotplug belabox:~/belabox-hotplug
```

## 2. Install on the board (SSH in first: `ssh belabox`)

```bash
sudo apt update
sudo apt install -y python3-pip
sudo pip3 install websockets pyudev

sudo mkdir -p /opt/belabox-hotplug
sudo cp ~/belabox-hotplug/belabox_hotplug.py /opt/belabox-hotplug/
sudo cp ~/belabox-hotplug/bootstrap_token.py /opt/belabox-hotplug/
sudo cp ~/belabox-hotplug/LICENSE /opt/belabox-hotplug/
```

(`--break-system-packages` is only needed on newer pip/Ubuntu releases with
PEP 668 externally-managed environments - not required on Ubuntu 22.04.)

## 3. Get an auth token (one-time, run manually)

```bash
cd /opt/belabox-hotplug
sudo python3 bootstrap_token.py YOUR_BELAUI_PASSWORD
```

This creates `/etc/belabox-hotplug/config.json` with your saved token. No
further editing is required for a typical setup - USB and HDMI sources are
both auto-detected.

**Security note**: passing the password as a command-line argument means it
will briefly be visible to anything else on the box that can read the
process list (`ps aux`), and it'll land in your shell history unless you
prefix the command with a space (bash: `HISTCONTROL=ignorespace`, or just
edit it out of `~/.bash_history` afterward). This only matters for the one
command above - after that, only the derived token is stored on disk, never
the plaintext password.

## 4. Config reference

```json
{
  "auth_token": "...",
  "belaui_ws_url": "ws://127.0.0.1",
  "hdmi_device": null,
  "hdmi_poll_interval_seconds": 1.0,
  "settle_delay_seconds": 2.0,
  "source_priority": "hdmi"
}
```

- `hdmi_device`: leave as `null` to auto-detect the `rk_hdmirx` device node.
  Set explicitly (e.g. `"/dev/video0"`) only if autodetection picks the
  wrong node.
- `hdmi_poll_interval_seconds`: how often to check `power_present`.
- `settle_delay_seconds`: how long to wait after a source appears before
  probing its format and starting the stream, to let it finish
  initializing.
- `source_priority`: `"hdmi"` or `"usb"` - which source wins if both are
  connected at once.

There is no `capture_vendor_id`/`capture_product_id` - any external USB
video capture device is detected automatically.

## 5. Install and start the service

```bash
sudo cp ~/belabox-hotplug/belabox-hotplug.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable belabox-hotplug
sudo systemctl start belabox-hotplug
```

## 6. Watch it work

```bash
sudo journalctl -u belabox-hotplug -f
```

- Nothing plugged into USB or HDMI: no stream starts.
- Plug in a USB capture source: it's detected, the correct pipeline is
  picked based on its resolution, and streaming starts.
- Unplug it: streaming stops.
- Plug a source into the onboard HDMI port: detected via `power_present`
  (not just device existence), matching pipeline picked, streaming starts.
- Unplug HDMI: streaming stops.
- Both connected at once: the `source_priority` rule is applied and logged.

## Troubleshooting

- **No udev events showing up for a USB device**: confirm it actually shows
  up under `/sys/bus/usb/devices/*/idVendor` - if it doesn't enumerate as a
  USB device at all, this daemon can't see it either.
- **HDMI never seems to detect a source**: check
  `v4l2-ctl -d /dev/video0 --get-ctrl=power_present` manually. If it's stuck
  at `0`, the issue is at the hardware/cable/source level (verified on this
  board: the source device itself must be powered on and actively
  outputting - a cable plugged into an unpowered source will not flip this).
- **"Can't stop/start stream, not connected to belaUI"**: check belaUI's
  actual service name with `systemctl list-units | grep -i bela` and fix
  the `After=`/`Wants=` lines in the `.service` file if it's not literally
  called `belaUI.service`.
- **Auth fails on every reconnect**: the token may have been invalidated
  (e.g. password changed in belaUI). Re-run `bootstrap_token.py`.
- **Pipeline picked seems wrong**: check the journalctl log line
  `Matched <type> source (<w>x<h>@<fps>) to pipeline '<name>'` - matching is
  keyword + closest-resolution only, not a full format match.

## fan-lock

A small, independent utility - pins the fan to a fixed cooling state at
boot rather than letting the kernel's thermal governor ramp it up/down.

```bash
sudo cp fan-lock.sh /usr/local/bin/fan-lock.sh
sudo chmod 755 /usr/local/bin/fan-lock.sh
sudo cp fan-lock.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fan-lock
```

`cur_state` in `fan-lock.sh` (currently `2`) is board/cooling-device
specific - check `/sys/class/thermal/cooling_device0/` on your own hardware
before assuming this value applies.

## License

MIT - see [LICENSE](LICENSE). Copyright (c) 2026 Jason Ardon (W3ndees).
