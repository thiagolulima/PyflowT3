import httpx
import asyncio
import os
import sys
import subprocess
import sqlite3
import logging
from dotenv import load_dotenv
from executaWorkflow import executar_etl

# Desativa logs excessivos
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_PATH = os.getenv("DB_PATH", "agendador.db")

fluxo_map = {}

def buscar_fluxos():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT distinct arquivo FROM agendamentos WHERE Status = 'Ativo' ORDER BY arquivo")
    fluxos = [row[0] for row in cursor.fetchall()]
    conn.close()
    return fluxos

def buscar_fluxos_por_nome(filtro: str):
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

def executar_fluxo(caminho):
    if not os.path.exists(caminho):
        return f"❌ Caminho não encontrado: {caminho}"
    python_exec = sys.executable
    if python_exec.lower().endswith("pythonservice.exe"):
        python_exec = os.path.join(os.path.dirname(python_exec), "python.exe")
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(""" 
            SELECT id, projeto, local_run, arquivo, ferramenta_etl 
            FROM agendamentos 
            WHERE arquivo = ? 
            ORDER BY id LIMIT 1
        """, (caminho,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return f"❌ Nenhum agendamento encontrado para o arquivo: {caminho}"

        id_agendamento, projeto, local_run, arquivo, ferramenta_etl = row

        if ferramenta_etl.upper() == 'PENTAHO':
            cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo]
        elif ferramenta_etl.upper() == 'APACHE_HOP':
            cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo, projeto or "", local_run or ""]
        elif ferramenta_etl.upper() == 'TERMINAL':
            cmd = [python_exec, 'executaWorkflow.py', str(id_agendamento), arquivo]
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

async def obter_mensagens(offset=None):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
            resp = await client.get(f"{API_URL}/getUpdates", params={"offset": offset, "timeout": 30})
            return resp.json()
    except httpx.ReadTimeout:
        logging.warning("[Telegram] Tempo limite ao buscar mensagens.")
        return {}
    except Exception as e:
        logging.error(f"[Telegram] Erro inesperado: {e}")
        return {}

async def enviar_resposta(chat_id, texto, reply_markup=None):
    payload = {"chat_id": chat_id, "text": texto}
    if reply_markup:
        payload["reply_markup"] = reply_markup

    async with httpx.AsyncClient() as client:
        await client.post(f"{API_URL}/sendMessage", json=payload)
    logging.info(f"[BOT] Mensagem enviada para chat_id {chat_id}")

async def responder_callback(callback_query_id, texto=None):
    payload = {"callback_query_id": callback_query_id}
    if texto:
        payload["text"] = texto
    async with httpx.AsyncClient() as client:
        await client.post(f"{API_URL}/answerCallbackQuery", json=payload)
    logging.info("[BOT] Callback respondido")

async def loop_bot(stop_event: asyncio.Event = None):
    global fluxo_map
    offset = None

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    logging.info("Bot do Telegram iniciado")

    while True:
        if stop_event and stop_event.is_set():
            logging.info("Stop event recebido. Encerrando loop do bot.")
            break

        dados = await obter_mensagens(offset)
        for resultado in dados.get("result", []):
            offset = resultado["update_id"] + 1

            mensagem = resultado.get("message")
            if mensagem:
                texto = mensagem.get("text", "")
                chat_id = mensagem.get("chat", {}).get("id")

                if not texto or chat_id != CHAT_ID:
                    continue

                if texto == "/agendas":
                    fluxos = buscar_fluxos()
                    fluxo_map = {f"EXEC:{i}": caminho for i, caminho in enumerate(fluxos)}
                    botoes = [[{"text": os.path.basename(caminho), "callback_data": f"EXEC:{i}"}] for i, caminho in enumerate(fluxos)]
                    reply_markup = {"inline_keyboard": botoes}
                    await enviar_resposta(chat_id, "Escolha uma agenda para executar:", reply_markup)

                elif texto.startswith("/buscar "):
                    termo = texto.replace("/buscar", "", 1).strip()
                    if not termo:
                        await enviar_resposta(chat_id, "❌ Use o comando no formato: `/buscar nome_da_agenda`", reply_markup=None)
                        continue

                    fluxos = buscar_fluxos_por_nome(termo)
                    if not fluxos:
                        await enviar_resposta(chat_id, f"🔍 Nenhuma agenda encontrada contendo: `{termo}`", reply_markup=None)
                        continue

                    fluxo_map = {f"EXEC:{i}": caminho for i, caminho in enumerate(fluxos)}
                    botoes = [[{"text": os.path.basename(caminho), "callback_data": f"EXEC:{i}"}] for i, caminho in enumerate(fluxos)]
                    reply_markup = {"inline_keyboard": botoes}
                    await enviar_resposta(chat_id, f"🔍 Resultados da busca por: `{termo}`", reply_markup)

            callback = resultado.get("callback_query")
            if callback:
                dados = callback.get("data")
                callback_id = callback.get("id")
                chat_id = callback.get("message", {}).get("chat", {}).get("id")

                if dados and dados.startswith("EXEC:") and chat_id == CHAT_ID:
                    caminho = fluxo_map.get(dados)
                    await responder_callback(callback_id)
                    if caminho:
                        await enviar_resposta(chat_id, f"⏳ Executando agenda:\n`{os.path.basename(caminho)}`")
                        output = executar_fluxo(caminho)
                        output = output if output else "⚠️ Nenhuma saída retornada."
                        await enviar_resposta(chat_id, f"✅ Resultado:\n{output[:4000]}")
                    else:
                        await enviar_resposta(chat_id, "❌ Caminho não encontrado.")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(loop_bot())
