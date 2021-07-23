#!/bin/bash

SUBSCRIPTION="taxirides_realtime"
DATASET="beam_streaming"

python3 /tmp/src/taxirides-realtime.py \
    --project=${GCP_PROJECT} \
    --input_subscription="projects/${GCP_PROJECT}/subscriptions/${SUBSCRIPTION}" \
    --output_dataset=${DATASET} \
    --output_table=${SUBSCRIPTION} \
    --output_error_table=${SUBSCRIPTION}_error \
    --region='us-central1' \
    --runner DataflowRunner
