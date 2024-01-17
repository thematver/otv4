import asyncio
import logging
import os

import requests
import sys
from aiogram import *
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton


import boto3


TOKEN = os.environ['TOKEN']
S3_BUCKET_NAME = "converse-cloud"
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
CREATE_TASK_FUNCTION_URL = os.environ['CREATE_TASK_FUNCTION_URL']
CREATE_URL_TASK_FUNCTION_URL = os.environ['CREATE_URL_TASK_FUNCTION_URL']

dp = Dispatcher()
session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY)

s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    region_name="ru-central1"
)

bot = None
form_router = Router()

formats = {
    "MP4": ["MOV", "GIF"],
    "MOV": ["MP4"],
    "WAV": ["MP3"],
    "MP3": ["WAV"]
}

class Form(StatesGroup):
    document = State()
    format = State()

@form_router.message(CommandStart())
async def command_start(message: Message, state: FSMContext) -> None:
    await state.set_state(Form.document)
    await message.answer(
        "Привет! Отправьте свой документ.",
        reply_markup=ReplyKeyboardRemove(),
    )

@form_router.message(F.text.casefold() == "отменить")
async def command_start(message: Message, state: FSMContext) -> None:
    await state.set_state(Form.document)
    await message.answer(
        "Состояние сброшено. В следующий раз просто отправь файл, больше писать /start нет необходимости.",
        reply_markup=ReplyKeyboardRemove(),
    )

@form_router.message(Form.document)
async def process_document(message: Message, state: FSMContext) -> None:
    if message.document or message.audio:
        file_format = ""
        if message.document:
            await state.update_data(document=message.document)
            file_format = message.document.file_name.split(".")[1].strip().upper()
        elif message.audio:
            await state.update_data(document=message.audio)
            file_format = message.audio.file_name.split(".")[1].strip().upper()

        await state.set_state(Form.format)

        if not file_format in formats:
            await state.set_state(Form.document)
            await message.answer("Такой формат пока не поддерживаем :( \nПопробуйте отправить другой файл?\n\nДоступные форматы: MOV, MP4,"
                                 "WAV, GIF")
            return

        available_formats =  formats[file_format.strip().upper()]
        await message.answer(
            f"Отлично! В какой формат конвертируем?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text=format) for format in available_formats],
                    [KeyboardButton(text="Отменить")]
                ],
                resize_keyboard=True,
            ),
        )
    elif message.text and 'youtube' in message.text:
        await state.clear()
        (message.chat.id, message.text)
    else:
        await message.answer("В вашем сообщении не прикреплен файл :(")

@form_router.message(Form.format)
async def process_format(message: Message, state: FSMContext) -> None:
    data = await state.update_data(format=message.text)
    await state.clear()

    document = data["document"]
    document_format = data["format"].strip().lower()

    file_id = document.file_id
    file = await bot.get_file(file_id)
    file_path = file.file_path
    await bot.download_file(file_path, file_id)
    with open(file_id, 'rb') as f:
        s3.upload_fileobj(f, S3_BUCKET_NAME, file_id)
    os.remove(file_id)

    create_task(file_id, message.chat.id, document_format)
    await message.reply(f"Принято! Конвертируем файл в {document_format}.",         reply_markup=ReplyKeyboardRemove())


def create_task(file_id, user_id, document_format) -> None:
    requests.post(CREATE_TASK_FUNCTION_URL, json={
        "action": "convert",
        "source": file_id,
        "user_id": user_id,
        "output_format": document_format
    })

def create_url_task(user_id, url) -> None:
    requests.post(CREATE_URL_TASK_FUNCTION_URL, json={
        "action": "youtube",
        "url": url,
        "user_id": user_id,
    })


async def main() -> None:
    dp.include_router(form_router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    bot = Bot(TOKEN)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
