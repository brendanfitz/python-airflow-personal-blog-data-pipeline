#!/bin/bash
sudo apt-get install awscli
aws --version
aws configure
aws s3 cp s3://metis-projects/stock_index_data/dowjones.json .
