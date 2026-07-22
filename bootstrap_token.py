#!/usr/bin/env python3
"""
Run this ONCE to get a persistent auth token from belaUI, the same way
the browser does when you check "remember me" on the login page.

Usage:
    python3 bootstrap_token.py <belaui_password>

This connects to belaUI over its local websocket, authenticates with the
password, and saves the returned auth_token to /etc/belabox-hotplug/config.json.
The daemon (belabox_hotplug.py) reads that token on every boot instead of
ever needing the plaintext password.

Copyright (c) 2026 Jason Ardon (W3ndees)
Licensed under the MIT License - see LICENSE in this repository.
"""
import asyncio
import json
import sys
import os

CONFIG_DIR = "/etc/belabox-hotplug"
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")

try:
    import websockets
except ImportError:
    print("Missing dependency. Install it with:")
    print("  sudo pip3 install websockets pyudev --break-system-packages")
    sys.exit(1)


async def get_token(password: str) -> str:
    uri = "ws://127.0.0.1"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"auth": {"password": password, "persistent_token": True}}))

        # Wait for the auth response specifically; belaUI may send other
        # messages (config, pipelines, status) around the same time.
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if "auth" in msg:
                auth = msg["auth"]
                if auth.get("success") is True and auth.get("auth_token"):
                    return auth["auth_token"]
                else:
                    raise RuntimeError(f"Authentication failed: {auth}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 bootstrap_token.py <belaui_password>")
        sys.exit(1)

    password = sys.argv[1]

    token = asyncio.run(get_token(password))
    print(f"Got auth token: {token}")

    os.makedirs(CONFIG_DIR, exist_ok=True)

    config = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            config = json.load(f)

    config["auth_token"] = token
    config.setdefault("belaui_ws_url", "ws://127.0.0.1")
    config.setdefault("hdmi_device", None)  # None = autodetect the rk_hdmirx node
    config.setdefault("hdmi_poll_interval_seconds", 1.0)
    config.setdefault("settle_delay_seconds", 2.0)
    config.setdefault("source_priority", "hdmi")  # "hdmi" or "usb", see README

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

    os.chmod(CONFIG_PATH, 0o600)  # token is a secret, lock it down

    print(f"Saved config to {CONFIG_PATH}")
    print("No further editing needed for typical setups: USB capture sources and the")
    print("onboard HDMI-in are both auto-detected. See README.md if you need to override")
    print("hdmi_device or source_priority.")


if __name__ == "__main__":
    main()
