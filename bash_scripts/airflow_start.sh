#!/bin/bash

# webserver
tmux new -d -s airflow-webserver
sleep 1.5
tmux send-keys -t airflow-webserver.0 "airflow_env" ENTER
tmux send-keys -t airflow-webserver.0 "airflow webserver" ENTER

tmux new -d -s airflow-scheduler
sleep 1.5
tmux send-keys -t airflow-scheduler.0 "airflow_env" ENTER
tmux send-keys -t airflow-scheduler.0 "airflow scheduler" ENTER
