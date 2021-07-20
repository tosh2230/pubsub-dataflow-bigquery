import os
import json

import boto3
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials

AWS_SSM_PARAM_NAME = os.environ.get('AWS_SSM_PARAM_NAME')
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_PUBSUB_TOPIC_ID = os.environ.get('GCP_PUBSUB_TOPIC_ID')
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

def get_parameters(names):
    ssm = boto3.client('ssm', region_name=AWS_REGION)
    response = ssm.get_parameters(Names=names, WithDecryption=True)
    return response['Parameters'][0]['Value']

def make_publisher(params):
    service_account_key = json.loads(params)
    credentials = Credentials.from_service_account_info(
        service_account_key, scopes=SCOPES
    )
    return pubsub_v1.PublisherClient(credentials=credentials)

def main(event, context) -> None:
    params = get_parameters(AWS_SSM_PARAM_NAME)
    publisher = make_publisher(params)

    topic_path = publisher.topic_path(GCP_PROJECT_ID, GCP_PUBSUB_TOPIC_ID)
    body = event

    future = publisher.publish(topic_path, body)
    return future.result()

def lambda_handler(event, context):
    return main(event, context)
