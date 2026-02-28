import os
from dotenv import load_dotenv

load_dotenv() #загрузка переменных

class Config:
    #db
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'video_stats')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    #yandex cloud
    YC_ADDR = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    YC_MODEL = "yandexgpt-lite"
    YC_FOLDER_ID = os.getenv('YC_FOLDER_ID')
    YC_API_KEY = os.getenv('YC_API_KEY')
    #tg
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    #logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

config = Config()