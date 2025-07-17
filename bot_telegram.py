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

import httpx
import requests
import threading
import os
import sys
import subprocess
import sqlite3
import logging
import time
from dotenv import load_dotenv
from executaWorkflow import executar_etl

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Desativa logs excessivos
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_PATH = os.getenv("DB_PATH", "agendador.db")

class TelegramBot:
    def __init__(self, stop_event):
        self.stop_event = stop_event
        self.session = requests.Session()
        self.offset = None
        self.fluxo_map = {}  # Dicionário para mapear callbacks para fluxos
        
    def buscar_fluxos(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT distinct arquivo FROM agendamentos WHERE Status = 'Ativo' ORDER BY arquivo")
        fluxos = [row[0] for row in cursor.fetchall()]
        conn.close()
        return fluxos

    def buscar_fluxos_por_nome(self, filtro: str):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT arquivo 
            FROM agendamentos 
            WHERE Status = 'Ativo' AND arquivo LIKE ?
            ORDER BY arquivo
        """, (f"%{filtro}%",))
        fluxos = [row[0] for row in cursor.fetchall()]
        conn.close()
        return fluxos

    def executar_fluxo(self, caminho):
        # Validação do tipo do caminho
        if isinstance(caminho, int):
            caminho = str(caminho)
        elif not isinstance(caminho, (str, bytes, os.PathLike)):
            return f"❌ Tipo de caminho inválido: {type(caminho)}"
        
        if not os.path.exists(caminho):
            return f"❌ Caminho não encontrado: {caminho}"
            
        python_exec = sys.executable
        if python_exec.lower().endswith("pythonservice.exe"):
            python_exec = os.path.join(os.path.dirname(python_exec), "python.exe")
            
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute(""" 
                SELECT id, projeto, local_run, arquivo, ferramenta_etl, timeout_execucao
                FROM agendamentos 
                WHERE arquivo = ? 
                ORDER BY id LIMIT 1
            """, (caminho,))
            row = cursor.fetchone()
            conn.close()

            if not row:
                return f"❌ Nenhum agendamento encontrado para o arquivo: {caminho}"

            id_agendamento, projeto, local_run, arquivo, ferramenta_etl, timeout_execucao = row

            if ferramenta_etl.upper() == 'PENTAHO':
                cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo, projeto or " ", local_run or " ", str(timeout_execucao)]
            elif ferramenta_etl.upper() == 'APACHE_HOP':
                cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo, projeto or " ", local_run or " ", str(timeout_execucao)]
            elif ferramenta_etl.upper() == 'TERMINAL':
                cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo, projeto or " ", local_run or " ", str(timeout_execucao)]
            else:
                return f"❌ Ferramenta ETL desconhecida: {ferramenta_etl}"

            processo = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True
            )
            stdout, stderr = processo.communicate()

            if processo.returncode == 0:
                return stdout if stdout else "⚙️ Agenda executada com sucesso."
            else:
                return f"❌ Erro ao executar a agenda:\n\nCódigo de saída: {processo.returncode}\n\nSTDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"

        except Exception as e:
            return f"❌ Erro ao executar a agenda: {str(e)}"

    def obter_mensagens(self):
        try:
            params = {"timeout": 30}
            if self.offset:
                params["offset"] = self.offset
                
            response = self.session.get(
                f"{API_URL}/getUpdates",
                params=params,
                timeout=35
            )
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning("[Telegram] Tempo limite ao buscar mensagens.")
            return {}
        except Exception as e:
            logger.error(f"[Telegram] Erro inesperado: {e}")
            return {}

    def enviar_resposta(self, chat_id, texto, reply_markup=None):
        payload = {"chat_id": chat_id, "text": texto}
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            response = self.session.post(f"{API_URL}/sendMessage", json=payload)
            response.raise_for_status()
            logger.info(f"[BOT] Mensagem enviada para chat_id {chat_id}")
        except Exception as e:
            logger.error(f"[BOT] Erro ao enviar mensagem: {e}")

    def responder_callback(self, callback_query_id, texto=None):
        payload = {"callback_query_id": callback_query_id}
        if texto:
            payload["text"] = texto
            
        try:
            response = self.session.post(f"{API_URL}/answerCallbackQuery", json=payload)
            response.raise_for_status()
            logger.info("[BOT] Callback respondido")
        except Exception as e:
            logger.error(f"[BOT] Erro ao responder callback: {e}")

    def run(self):
        logger.info("Bot do Telegram iniciado")

        while not self.stop_event.is_set():
            try:
                dados = self.obter_mensagens()
                for resultado in dados.get("result", []):
                    self.offset = resultado["update_id"] + 1

                    mensagem = resultado.get("message")
                    if mensagem:
                        texto = mensagem.get("text", "")
                        chat_id = mensagem.get("chat", {}).get("id")

                        if not texto or chat_id != CHAT_ID:
                            continue

                        if texto == "/agendas":
                            fluxos = self.buscar_fluxos()
                            self.fluxo_map = {f"EXEC:{i}": caminho for i, caminho in enumerate(fluxos)}
                            botoes = [[{"text": os.path.basename(caminho), "callback_data": f"EXEC:{i}"}] for i, caminho in enumerate(fluxos)]
                            reply_markup = {"inline_keyboard": botoes}
                            self.enviar_resposta(chat_id, "Escolha uma agenda para executar:", reply_markup)

                        elif texto.startswith("/buscar "):
                            termo = texto.replace("/buscar", "", 1).strip()
                            if not termo:
                                self.enviar_resposta(chat_id, "❌ Use o comando no formato: `/buscar nome_da_agenda`", reply_markup=None)
                                continue

                            fluxos = self.buscar_fluxos_por_nome(termo)
                            if not fluxos:
                                self.enviar_resposta(chat_id, f"🔍 Nenhuma agenda encontrada contendo: `{termo}`", reply_markup=None)
                                continue

                            self.fluxo_map = {f"EXEC:{i}": caminho for i, caminho in enumerate(fluxos)}
                            botoes = [[{"text": os.path.basename(caminho), "callback_data": f"EXEC:{i}"}] for i, caminho in enumerate(fluxos)]
                            reply_markup = {"inline_keyboard": botoes}
                            self.enviar_resposta(chat_id, f"🔍 Resultados da busca por: `{termo}`", reply_markup)

                    callback = resultado.get("callback_query")
                    if callback:
                        dados = callback.get("data")
                        callback_id = callback.get("id")
                        chat_id = callback.get("message", {}).get("chat", {}).get("id")

                        if dados and dados.startswith("EXEC:") and chat_id == CHAT_ID:
                            caminho = self.fluxo_map.get(dados)
                            self.responder_callback(callback_id)
                            if caminho:
                                self.enviar_resposta(chat_id, f"⏳ Executando agenda:\n`{os.path.basename(caminho)}`")
                                output = self.executar_fluxo(caminho)
                                output = output if output else "⚠️ Nenhuma saída retornada."
                                self.enviar_resposta(chat_id, f"✅ Resultado:\n{output[:4000]}")
                            else:
                                self.enviar_resposta(chat_id, "❌ Caminho não encontrado.")
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Erro no loop principal: {e}")
                time.sleep(5)  # Prevenção contra crash loops

def run_bot(stop_event):
    bot = TelegramBot(stop_event)
    bot.run()

if __name__ == '__main__':
    stop_event = threading.Event()
    run_bot(stop_event)