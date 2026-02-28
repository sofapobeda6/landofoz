import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from .db import db
from .yandex_gpt import yandex_gpt
from .config import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

async def send_telegram_message(chat_id: int, text: str):
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text
        )
        logger.info(f"Message sent to {chat_id}")
    except Exception as e:
        logger.error(f"Send message error: {e}")

async def process_message(chat_id: int, text: str):
    logger.info(f"Processing message: {text}")
    try:
        if text == '/start':
            response = (
                "Привет! Я бот для статистики видео.\n\n"
                "Примеры вопросов:\n"
                "Сколько всего видео?\n"
                "Сколько видео у креатора 123?\n"
                "Сколько видео с просмотрами > 100000?\n"
                "На сколько выросли просмотры 28 ноября 2025?"
            )
            logger.info("/start command processed")
        else:
            ai_response = await yandex_gpt.process_question(text)
            logger.info(f"AI response received")
            if ai_response.get('sql'):
                try:
                    result = await db.execute_query(ai_response['sql'])
                    response = f"{result}"
                except Exception as e:
                    logger.error(f"SQL execution error: {e}")
                    response = "Ошибка при выполнении запроса к базе данных."
            else:
                response = ai_response.get('message', 'Не удалось обработать вопрос. Попробуйте переформулировать.')
        await send_telegram_message(chat_id, response)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await send_telegram_message(chat_id, "Произошла ошибка. Пожалуйста, попробуйте позже.")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await process_message(message.chat.id, "/start")

@dp.message()
async def handle_message(message: Message):
    await process_message(message.chat.id, message.text)

async def on_startup():
    logger.info("BOT STARTING UP")
    try:
        await db.connect()
        logger.info("Database connected")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

async def on_shutdown():
    logger.info("BOT SHUTTING DOWN")
    await db.close()
    logger.info("Database connection closed")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())