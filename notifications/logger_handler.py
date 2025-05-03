
import logging
import os
from dotenv import load_dotenv
from .notifier import notificar

load_dotenv()
LEVELS = os.getenv("NOTIFY_LEVELS", "ERROR,CRITICAL").split(",")

class NotificacaoHandler(logging.Handler):
    def emit(self, record):
        try:
            if record.levelname in LEVELS:
                mensagem = self.format(record)
                notificar(mensagem)
        except Exception as e:
            print(f"[Notificação] Falha ao enviar notificação: {e}")
