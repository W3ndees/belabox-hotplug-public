#!/bin/bash
# fan-lock.sh: keeps the board's fan pinned to a fixed cooling state instead
# of letting the kernel's thermal governor ramp it up/down.
#
# Copyright (c) 2026 Jason Ardon (W3ndees)
# Licensed under the MIT License - see /opt/fan-lock/LICENSE.
while true; do
    echo 2 > /sys/class/thermal/cooling_device0/cur_state
    sleep 10
done
