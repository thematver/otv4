import json
import os
from aiogram import *
import asyncio
import yandexcloud
from aiogram.enums import ParseMode
import boto3
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub


token = ""
boto_session = None
out_ymq_queue = None

async def send_message(user_id, path):
    operator = Bot(token)
    await operator.send_message(user_id, f"Готово! Файл доступен в течение часа <a href=\"{path}\">по ссылке</a>.", parse_mode=ParseMode.HTML)

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
    global token
    for entry in response.entries:
        if entry.key == 'aws_access_key_id':
            access_key = entry.text_value
        elif entry.key == 'aws_secret_access_key':
            secret_key = entry.text_value
        if entry.key == "bot_token":
            token = entry.text_value
    if access_key is None or secret_key is None:
        raise Exception("secrets required")
    print("Key id: " + access_key)

    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session


def handle(event, context):
    get_boto_session()
    for message in event['messages']:
        task_json = json.loads(message['details']['message']['body'])
        path = task_json['path']
        user_id = task_json['user_id']
        asyncio.run(send_message(user_id=user_id, path=path))