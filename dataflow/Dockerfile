FROM google/cloud-sdk:412.0.0-slim

RUN apt-get update

COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip
RUN pip3 install -r /tmp/requirements.txt

COPY ./schema/ /tmp/
COPY ./taxirides_realtime.py /tmp/
COPY ./start_streaming.sh /tmp/
