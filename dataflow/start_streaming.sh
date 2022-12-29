#!/bin/bash

SUBSCRIPTION="taxirides_realtime"
DATASET="beam_streaming"

python3 /tmp/taxirides-realtime.py \
    --project=${GCP_PROJECT} \
    --input_subscription="projects/${GCP_PROJECT}/subscriptions/${SUBSCRIPTION}" \
    --output_dataset=${DATASET} \
    --output_table=${SUBSCRIPTION} \
    --output_error_table=${SUBSCRIPTION}_error \
    --runner DataflowRunner \
    --enable_streaming_engine \
    --region='us-central1' \
    --worker_machine_type='n1-standard-2' \
    --num_workers=1 \
    --max_num_workers=1
