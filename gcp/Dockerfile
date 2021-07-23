FROM google/cloud-sdk:349.0.0-slim

RUN apt-get update

COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt

COPY ./schema/ /tmp/
COPY ./src/ /tmp/
COPY ./start_streaming.sh /tmp/
