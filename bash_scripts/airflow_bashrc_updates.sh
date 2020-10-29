#!/bin/bash

echo "export AIRFLOW_HOME=/home/brendan/Github/python-airflow-personal-blog-data-pipeline/airflow" >> ~/.bashrc
echo 'alias airflow_env="cd $AIRFLOW_HOME/.. && source ./venv/bin/activate"' >> ~/.bashrc
