import aiohttp
import asyncio
import logging
import json
import re
from .config import config
from .prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

class YandexGPT:
    def __init__(self):
        self.api_url = config.YC_ADDR
        self.folder_id = config.YC_FOLDER_ID
        self.api_key = config.YC_API_KEY
        self.model = config.YC_MODEL

    async def process_question(self, question: str) -> dict:
        if not self.api_key or not self.folder_id:
            logger.error("Yandex GPT credentials not set")
            return {
                'sql': "SELECT COUNT(*) FROM videos;",
                'message': "Ошибка конфигурации AI"
            }
        user_message = f"Вопрос пользователя: {question}"
        data = {
            'modelUri': f"gpt://{self.folder_id}/{self.model}",
            'completionOptions': {
                'stream': False,
                'temperature': 0.1,
                'maxTokens': 500
            },
            'messages': [
                {'role': 'system', 'text': SYSTEM_PROMPT},
                {'role': 'user', 'text': user_message}
            ]
        }
        headers = {
            'Authorization': f'Api-Key {self.api_key}',
            'Content-Type': 'application/json'
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, headers=headers, json=data, timeout=10) as response:
                    if response.status == 200:
                        result = await response.json()
                        text = result['result']['alternatives'][0]['message']['text']
                        return self._parse_response(text, question)
                    else:
                        error_text = await response.text()
                        logger.error(f"Yandex GPT error: {response.status} - {error_text}")
                        return self._fallback_response(question)
        except asyncio.TimeoutError:
            logger.error("Yandex GPT timeout")
            return self._fallback_response(question)
        except Exception as e:
            logger.error(f"Yandex GPT error: {e}")
            return self._fallback_response(question)

    def _parse_response(self, text: str, original_question: str) -> dict:
        text = re.sub(r',\s*}', '}', text) 
        text = re.sub(r',\s*]', ']', text) 
        json_match = re.search(r'\{.*\}', text, re.DOTALL) #ищем json
        if json_match:            
            try:                
                result = json.loads(json_match.group())
                if 'sql' not in result:
                    result['sql'] = None
                if 'message' not in result:
                    result['message'] = None
                if result.get('sql'):
                    result['sql'] = self._clean_sql(result['sql'])
                return result
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON: {text}")
        return {
            'sql': None,
            'message': "Не удалось обработать вопрос. Попробуйте переформулировать."
        }

    def _clean_sql(self, sql: str) -> str:
        sql = re.sub(r'```sql\n?|```\n?', '', sql)
        sql = re.sub(r'SQL:\s*', '', sql, flags=re.IGNORECASE)
        sql = ' '.join(sql.split())
        if not sql.endswith(';'):
            sql += ';'
        return sql

yandex_gpt = YandexGPT()