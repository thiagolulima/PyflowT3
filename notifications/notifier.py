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
from .telegram_notify import enviar_telegram
from .email import enviar_email
import sys

load_dotenv()
CANAIS_DEFAULT = os.getenv("NOTIFY_CHANNELS",",").split(",")

def notificar(mensagem: str, canais: list = None):
    canais = canais or CANAIS_DEFAULT

    if 'telegram' in canais:
        from .telegram_notify import enviar_telegram
        try:
            asyncio.run(enviar_telegram(mensagem))
        except RuntimeError as e:
            if "Event loop is closed" not in str(e):
                raise

    if 'email' in canais:
        from .email import enviar_email
        try:
            asyncio.run(enviar_email(mensagem))  # Corrigido a ordem da chamada
        except RuntimeError as e:
            if "Event loop is closed" not in str(e):
                raise

if sys.platform.startswith('win') and sys.version_info < (3, 9):
    # Evita o erro "Event loop is closed" ao encerrar no Windows
    import asyncio.proactor_events

    def silence_event_loop_closed(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RuntimeError as e:
                if str(e) != 'Event loop is closed':
                    raise
        return wrapper

    asyncio.proactor_events._ProactorBasePipeTransport.__del__ = silence_event_loop_closed(
        asyncio.proactor_events._ProactorBasePipeTransport.__del__
    )