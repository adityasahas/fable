#!/bin/bash

# Create the specific directory structure D-Bus needs
mkdir -p /var/run/dbus
chmod 755 /var/run/dbus
chown messagebus:messagebus /var/run/dbus

# Create directory for PID file
mkdir -p /var/run/dbus
touch /var/run/dbus/pid
chown messagebus:messagebus /var/run/dbus/pid

# Start D-Bus with explicit socket path and PID file
dbus-daemon --system --fork --address=unix:path=/var/run/dbus/system_bus_socket --pid-file=/var/run/dbus/pid

# Give D-Bus a moment to start
sleep 2

source /opt/conda/etc/profile.d/conda.sh
conda activate fable_env

exec python -m uvicorn main:app --host 0.0.0.0 --port $PORT