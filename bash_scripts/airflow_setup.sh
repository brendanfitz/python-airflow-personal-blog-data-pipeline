#!/bin/bash

echo $'Did you run airflow_bashrc_updates.sh and then re-source your bashrc?\nEnter "yes" or "no":'

read answer

if [ $answer == 'yes' ]
then
  sudo apt-get update
  sudo apt-get install build-essential
  
  pip install \
   apache-airflow[postgres,aws]==1.10.12 \
   --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"
  
  sudo apt-get install -y --no-install-recommends \
          freetds-bin \
          krb5-user \
          ldap-utils \
  #       libffi6 \
          libsasl2-2 \
          libsasl2-modules \
          libssl1.1 \
          locales  \
          lsb-release \
          sasl2-bin \
          sqlite3 \
          unixodbc
  
  # correction for errors installing libffi6 on ubuntu 20.04
  wget http://mirrors.kernel.org/ubuntu/pool/main/libf/libffi/libffi6_3.2.1-8_amd64.deb
  sudo apt-get install ./libffi6_3.2.1-8_amd64.deb
  rm ./libffi6_3.2.1-8_amd64.deb
  echo $'\nInstallation Complete\n'
else
  echo $'\nPlease do so\n'
fi
