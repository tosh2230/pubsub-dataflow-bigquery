#!/bin/bash

SUBSCRIPTION="taxirides_realtime"
DATASET="beam_streaming"

gcloud pubsub subscriptions create ${SUBSCRIPTION} \
    --topic=projects/pubsub-public-data/topics/taxirides-realtime

bq --location=US mk -d \
    --description ${DATASET} \
    ${DATASET}
