#!/usr/bin/env bash

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


exec "$@"
