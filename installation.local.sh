#!/bin/bash
set -e # Any subsequent commands which fail will cause the shell script to exit immediately

#----- Install Python3.6 -----#
sudo apt-get update
sudo apt-get install python3.6
#----- Install Python3.6 -----#

#----- Install Postgresql9.6 -----#
sudo add-apt-repository "deb https://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main"
sudo apt-get update
sudo apt-get install postgresql-9.6 postgresql-contrib
#----- Install Postgresql9.6 -----#

#----- Install Redis Script -----#
sudo apt update
sudo apt install redis-server
#----- Install Redis Script -----#

#----- Install Python Dependencies -----#
pip install -r requirements.txt
pip3 install --extra-index-url http://3.90.97.142:9000/simple/ --trusted-host 3.90.97.142 taiyo-utils
#----- Install Python Dependencies -----#