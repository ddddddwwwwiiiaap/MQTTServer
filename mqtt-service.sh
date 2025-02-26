#!/bin/bash

# Direktori utama aplikasi
APP_DIR="/opt/android-project/MQTTServer"
VENV_DIR="$APP_DIR/venv"
PID_FILE="$APP_DIR/mqtt.pid"
LOG_FILE="$APP_DIR/mqtt_server.log"
PYTHON_SCRIPT="$APP_DIR/main.py"

# Memastikan direktori log ada
ensure_log_directory() {
    if [ ! -d "$(dirname "$LOG_FILE")" ]; then
        mkdir -p "$(dirname "$LOG_FILE")"
    fi
}

# Memeriksa virtual environment
check_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        echo "Virtual environment not found at $VENV_DIR"
        exit 1
    fi
}

# Memeriksa file Python utama
check_script() {
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo "Python script not found at $PYTHON_SCRIPT"
        exit 1
    fi
}

start() {
    check_venv
    check_script
    ensure_log_directory
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Service is already running with PID $PID"
            exit 1
        else
            echo "Removing stale PID file"
            rm "$PID_FILE"
        fi
    fi
    
    echo "Starting MQTT Service..."
    source "$VENV_DIR/bin/activate"
    cd "$APP_DIR"  # Pindah ke direktori aplikasi
    
    # Set PYTHONPATH untuk menemukan modul lokal
    export PYTHONPATH="$APP_DIR:$PYTHONPATH"
    
    # Gunakan python -u untuk unbuffered output
    nohup python3 -u "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    echo "Service started with PID $PID"
    
    # Tunggu dan periksa proses
    sleep 2
    if ! ps -p $PID > /dev/null; then
        echo "Service failed to start. Last 20 lines of log:"
        tail -n 20 "$LOG_FILE"
        rm "$PID_FILE" 2>/dev/null
        exit 1
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Service is not running (PID file not found)"
        exit 1
    fi
    
    PID=$(cat "$PID_FILE")
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "Process not found. Cleaning up PID file."
        rm "$PID_FILE"
        exit 1
    fi
    
    echo "Stopping MQTT Service (PID: $PID)..."
    kill $PID
    
    # Tunggu proses berhenti
    TIMEOUT=10
    while ps -p $PID > /dev/null 2>&1; do
        if [ $TIMEOUT -le 0 ]; then
            echo "Service did not stop gracefully. Forcing stop..."
            kill -9 $PID
            break
        fi
        TIMEOUT=$((TIMEOUT-1))
        sleep 1
    done
    
    rm "$PID_FILE"
    echo "Service stopped"
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Service is running with PID $PID"
            return 0
        else
            echo "Service is not running (stale PID file)"
            rm "$PID_FILE"
            return 1
        fi
    else
        echo "Service is not running"
        return 1
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