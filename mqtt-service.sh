#!/bin/bash

PID_FILE="/opt/android-project/MQTTServer/mqtt.pid"
LOG_FILE="/opt/android-project/MQTTServer/mqtt_server.log"

start() {
    if [ -f "$PID_FILE" ]; then
        echo "Service is already running."
        exit 1
    fi
    
    echo "Starting MQTT Service..."
    source venv/bin/activate
    nohup python3 resgiter-android.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Service started with PID $(cat $PID_FILE)"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Service is not running."
        exit 1
    fi
    
    echo "Stopping MQTT Service..."
    kill $(cat "$PID_FILE")
    rm "$PID_FILE"
    echo "Service stopped"
}

status() {
    if [ -f "$PID_FILE" ]; then
        echo "Service is running with PID $(cat $PID_FILE)"
    else
        echo "Service is not running"
    fi
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 2
        start
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0
