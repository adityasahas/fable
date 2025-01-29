#!/bin/bash

mkdir -p /var/run/dbus
dbus-daemon --system --fork --address=unix:path=/var/run/dbus/system_bus_socket

source /opt/conda/etc/profile.d/conda.sh
conda activate fable_env

python -m uvicorn main:app --host 0.0.0.0 --port $PORT