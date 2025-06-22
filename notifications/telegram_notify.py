# Copyright 2025 Thiago Luis de Lima
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import httpx
from dotenv import load_dotenv
import asyncio

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

async def enviar_telegram(mensagem: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("[Telegram] BOT_TOKEN ou CHAT_ID não configurados.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": mensagem,
        "parse_mode": "HTML"
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, data=payload)
            response.raise_for_status()
            print("[Telegram] Mensagem enviada com sucesso.")
        except httpx.HTTPStatusError as e:
            print(f"[Telegram] Erro HTTP: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            print(f"[Telegram] Erro de conexão: {e}")
