import json
import os
import subprocess

import boto3
import requests
import yandexcloud
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

boto_session = None
storage_client = None
in_ymq_queue = None
out_ymq_queue = None

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

    # initialize boto session
    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session


def get_in_ymq_queue():
    global in_ymq_queue
    if in_ymq_queue is not None:
        return in_ymq_queue

    in_ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    ).Queue(os.environ['IN_YMQ_QUEUE_URL'])
    return in_ymq_queue


def get_output_ymq_queue():
    global out_ymq_queue
    if out_ymq_queue is not None:
        return out_ymq_queue

    out_ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    ).Queue(os.environ['OUT_YMQ_QUEUE_URL'])
    return out_ymq_queue


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


def download_from_ya_disk(src, dst):
    download_url = 'https://storage.yandexcloud.net/converse-cloud/' + \
                   str(src)
    download_response = requests.get(download_url)
    with open(dst, 'wb') as video_file:
        video_file.write(download_response.content)


def upload_and_presign(file_path, object_name):
    client = get_storage_client()
    bucket = os.environ['S3_BUCKET']
    client.upload_file(file_path, bucket, object_name)
    return client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': object_name}, ExpiresIn=3600)


def send_success(user_id, path):
    get_output_ymq_queue().send_message(MessageBody=json.dumps({'user_id': user_id, "path": path}))


def handle_process_event(event, context):
    for message in event['messages']:
        task_json = json.loads(message['details']['message']['body'])
        task_src = task_json['source']
        task_id = task_json['task_id']
        user_id = task_json['user_id']
        output_format = task_json['output_format']
        download_from_ya_disk(task_src, f"/tmp/{task_id}.mp4")
        subprocess.run(['ffmpeg', '-i', f"/tmp/{task_id}.mp4", '-r', '10', '-s', '320x240', f"/tmp/{task_id}.{output_format}"])
        result_download_url = upload_and_presign(f"/tmp/{task_id}.{output_format}", f"{task_id}_out.{output_format}")
        send_success(user_id, result_download_url)
    return "OK"