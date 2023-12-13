import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold

TOKEN = getenv("BOT_TOKEN")

dp = Dispatcher()

@dp.message()
async def echo_handler(message: types.Message) -> None:
    try:
        if message.document:
            await message.reply("Принято! Конвертируем.")
    except TypeError:
        await message.answer("Nice try!")


async def main() -> None:
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())