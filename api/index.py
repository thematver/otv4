import json
import os
import uuid

import boto3
import yandexcloud
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

boto_session = None
storage_client = None
url_ymq_queue = None


def get_boto_session():
    global boto_session
    if boto_session is not None:
        return boto_session

    yc_sdk = yandexcloud.SDK()
    channel = yc_sdk._channels.channel("lockbox-payload")
    lockbox = PayloadServiceStub(channel)
    response = lockbox.Get(GetPayloadRequest(secret_id=os.environ['SECRET_ID']))

    access_key = None
    secret_key = None
    for entry in response.entries:
        if entry.key == 'aws_access_key_id':
            access_key = entry.text_value
        elif entry.key == 'aws_secret_access_key':
            secret_key = entry.text_value
    if access_key is None or secret_key is None:
        raise Exception("secrets required")
    print("Key id: " + access_key)

    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session


def get_ymq_queue():
    global ymq_queue
    if ymq_queue is not None:
        return ymq_queue

    ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    ).Queue(os.environ['YMQ_QUEUE_URL'])
    return ymq_queue

def get_url_ymq_queue():
    global url_ymq_queue
    if url_ymq_queue is not None:
        return url_ymq_queue

    url_ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    ).Queue(os.environ['URL_YMQ_QUEUE_URL'])
    return url_ymq_queue


def get_storage_client():
    global storage_client
    if storage_client is not None:
        return storage_client

    storage_client = get_boto_session().client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1'
    )
    return storage_client

# API handler

def create_task(user_id, source, output_format):
    task_id = str(uuid.uuid4())
    get_ymq_queue().send_message(MessageBody=json.dumps({'task_id': task_id, "user_id": user_id, "source": source, "output_format": output_format}))
    return {
        'task_id': task_id
    }

def create_url_task(user_id, url):
    task_id = str(uuid.uuid4())
    get_url_ymq_queue().send_message(MessageBody=json.dumps({'task_id': task_id, "user_id": user_id, "url": url}))
    return {
        'task_id': task_id
    }


def handle_api(event, context):
    action = event['action']
    if action == 'convert':
        return create_task(event['user_id'], event['source'], event['output_format'])
    elif action == 'youtube':
        return create_url_task(event['user_id'], event['url'])
    else:
        return {"error": "unknown action: " + action}