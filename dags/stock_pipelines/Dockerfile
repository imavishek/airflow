FROM ubuntu:18.04

COPY . /app/

WORKDIR /app

RUN apt-get update
RUN apt-get --yes install python3.6
RUN apt-get --yes install python3-pip

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
RUN pip3 install --extra-index-url http://3.90.97.142:9000/simple/ --trusted-host 3.90.97.142 taiyo-utils

ENTRYPOINT [ "python3" , "from pipeline_abstractions import *;"]
