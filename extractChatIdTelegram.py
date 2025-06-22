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
import asyncio
from dotenv import load_dotenv
from telegram import Bot
from telegram.error import TelegramError
import asyncio

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")

async def main():
    if not TOKEN:
        print("BOT_TOKEN não definido no .env.")
        return

    bot = Bot(token=TOKEN)

    try:
        updates = await bot.get_updates()
        for update in reversed(updates):
            if update.message and update.message.chat.type in ['supergroup']:
                chat_id = update.message.chat.id
                print(f"chat_id do grupo: {chat_id}")

                # Atualiza o .env com o chat_id
                with open(".env", "r") as file:
                    lines = file.readlines()

                with open(".env", "w") as file:
                    updated = False
                    for line in lines:
                        if line.startswith("CHAT_ID="):
                            file.write(f"CHAT_ID={chat_id}\n")
                            updated = True
                        else:
                            file.write(line)
                    if not updated:
                        file.write(f"CHAT_ID={chat_id}\n")
                break
            elif update.message and update.message.chat.type in ['group']:
                chat_id = update.message.chat.id
                print(f"chat_id do grupo: {chat_id}")

                # Atualiza o .env com o chat_id
                with open(".env", "r") as file:
                    lines = file.readlines()

                with open(".env", "w") as file:
                    updated = False
                    for line in lines:
                        if line.startswith("CHAT_ID="):
                            file.write(f"CHAT_ID={chat_id}\n")
                            updated = True
                        else:
                            file.write(line)
                    if not updated:
                        file.write(f"CHAT_ID={chat_id}\n")
                break
            else:
             print("Nenhuma mensagem de grupo encontrada nas atualizações.")
    except TelegramError as e:
        print(f"Erro ao acessar Telegram API: {e}")

# Executar a função async
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
