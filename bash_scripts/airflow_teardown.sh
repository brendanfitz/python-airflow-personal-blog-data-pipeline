#!/bin/bash
tmux send-keys -t airflow-webserver.0 C-c
tmux kill-session -t airflow-webserver

tmux send-keys -t airflow-scheduler.0 C-c
tmux kill-session -t airflow-scheduler
