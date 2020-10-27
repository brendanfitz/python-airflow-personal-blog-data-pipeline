#!/bin/bash

# add this to bashrc
# export AIRFLOW_HOME=/home/brendan/Github/python-airflow-personal-blog-data-pipeline/airflow

sudo apt-get update
sudo apt-get install build-essential

pip install \
 apache-airflow[postgres,aws]==1.10.12 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"

sudo apt-get install -y --no-install-recommends \
        freetds-bin \
        krb5-user \
        ldap-utils \
#        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc

wget http://mirrors.kernel.org/ubuntu/pool/main/libf/libffi/libffi6_3.2.1-8_amd64.deb
sudo apt-get install ./libffi6_3.2.1-8_amd64.deb
rm ./libffi6_3.2.1-8_amd64.deb
