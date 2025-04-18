
import os
from telegram import Bot
import asyncio
from telegram.error import TelegramError
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

async def enviar_telegram(mensagem):
    if not BOT_TOKEN or not CHAT_ID:
        print("[Telegram] BOT_TOKEN ou CHAT_ID não configurados.")
        return

    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.send_message(chat_id=CHAT_ID, text=mensagem)
        print("[Telegram] Mensagem enviada com sucesso.")
    except TelegramError as e:
        print(f"[Telegram] Erro ao enviar mensagem: {e}")