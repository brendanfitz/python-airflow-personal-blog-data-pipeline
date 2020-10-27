#!/bin/bash

# install postgres
sudo apt-get install postgresql postgresql-contrib

# create user
sudo -u postgres createuser --superuser brendan

# launch postgres to change password (use \password brendan)
sudo -u postgres psql

# created blog database
sudo -u postgres createdb brendan
sudo -u brendan createdb blog

# create visuals schema in blog data (enter CREATE SCHEMA visuals;)
psql -d blog -U brendan -W
