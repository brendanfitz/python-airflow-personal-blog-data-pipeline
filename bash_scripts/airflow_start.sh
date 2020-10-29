#!/bin/bash

echo -n $'Starting airflow webserver...'
WEBSERVERNAME=airflow-webserver
tmux new -d -s $WEBSERVERNAME
sleep 1.5
tmux send-keys -t $WEBSERVERNAME.0 "airflow_env" ENTER
tmux send-keys -t $WEBSERVERNAME.0 "airflow webserver" ENTER
echo complete
echo $'TMUX SESSION NAME: '$WEBSERVERNAME$'\n'

echo -n $'Starting airflow scheduler...'
SCHEDULERNAME=airflow-scheduler
tmux new -d -s $SCHEDULERNAME
sleep 1.5
tmux send-keys -t $SCHEDULERNAME.0 "airflow_env" ENTER
tmux send-keys -t $SCHEDULERNAME.0 "airflow scheduler" ENTER
echo complete
echo $'TMUX SESSION NAME: '$SCHEDULERNAME$'\n'
