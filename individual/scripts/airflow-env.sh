#!/usr/bin/env bash

trap 'kill $(jobs -p)' EXIT

echo "Setting up environment"
if ! command -v python &> /dev/null; then
    source ~/uni/engeneering-data/.venv/bin/activate
    if ! command -v python &> /dev/null; then
        echo "python not found, exiting"
        exit 1
    fi
fi

export AIRFLOW_HOME=$"~/uni/engeneering-data/airflow/"

if ! command -v airflow &> /dev/null; then
    echo "Failed to find airflow in PATH"
    exit 1
fi

airflow scheduler &> /dev/null &
SCHEDULER_PID="$!"
echo "Scheduler started with PID: ${SCHEDULER_PID}"

airflow webserver --port 8080 &> /dev/null &
WEBSERVER_PID="$!"
echo "Webserver started with PID: ${WEBSERVER_PID}"

sleep infinity &
wait
