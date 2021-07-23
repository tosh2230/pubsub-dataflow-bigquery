#!/bin/bash

SUBSCRIPTION="taxirides_realtime"
DATASET="beam_streaming"

# https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#setting-other-cloud-dataflow-pipeline-options
python3 /tmp/taxirides-realtime.py \
    --project=${GCP_PROJECT} \
    --input_subscription="projects/${GCP_PROJECT}/subscriptions/${SUBSCRIPTION}" \
    --output_dataset=${DATASET} \
    --output_table=${SUBSCRIPTION} \
    --output_error_table=${SUBSCRIPTION}_error \
    --runner DataflowRunner \
    --enable_streaming_engine \
    --region='us-central1' \
    --autoscaling_algorithm=NONE \
    --num_workers=1 \
    --max_num_workers=1
