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
import subprocess
import time
import logging
from pathlib import Path
import sys
import platform
import time
import datetime
import sqlite3
from notifications.notifier import notificar
from dotenv import load_dotenv

load_dotenv()
# Configuração do diretório de trabalho
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SERVICE_DIR)

#banco de dados
DB_PATH = os.path.join(SERVICE_DIR, "agendador.db")

# Configuração avançada de logging
# Gera o nome do arquivo de log com a data atual
def get_daily_log_path():
    log_dir = os.path.join(SERVICE_DIR, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    data_atual = datetime.datetime.now().strftime("%d%m%Y")
    return os.path.join(log_dir, f"agendador{data_atual}.log")

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(get_daily_log_path(), encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

def determinar_sistema_operacional():
    """Detecta o sistema operacional e retorna configurações específicas"""
    sistema = platform.system().lower()
    if sistema == 'windows':
        return {
            'pentaho_kitchen':os.getenv("PENTAHO_JOB", r'C:\data-integration\Kitchen.bat'),
            'pentaho_pan':os.getenv("PENTAHO_TRANSFORMATION", r'C:\data-integration\Pan.bat')  ,
            'hop_run': os.getenv("APACHE_HOP", r'C:\Apache-hop\hop-run.bat'),
            'shell': True
        }
    else:  # Linux/Unix
        return {
            'pentaho_kitchen': os.getenv("PENTAHO_JOB", '/opt/data-integration/kitchen.sh'),
            'pentaho_pan': os.getenv("PENTAHO_TRANSFORMATION", '/opt/data-integration/Pan.sh'),
            'hop_run': os.getenv("APACHE_HOP", '/opt/hop/hop-run.sh') ,
            'shell': False
        }

config_os = determinar_sistema_operacional()

def executar_etl(id,arquivo_path, projeto_hop=None, local_run_hop=None, timeout=1800):
    """
    Executa jobs/transformações do Pentaho PDI, Apache Hop ou comandos genéricos de terminal

    Args:
        arquivo_path (str): Caminho completo para o arquivo (.kjb, .ktr, .hwf, .hpl, .bat, .sh, etc)
        projeto_hop (str, optional): Nome do projeto Hop (apenas para Apache Hop)
        local_run_hop (str, optional): Nome do local_run (apenas para Apache Hop)
        timeout (int): Tempo máximo de execução em segundos

    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        arquivo_path = os.path.abspath(os.path.normpath(arquivo_path))
        ext = os.path.splitext(arquivo_path)[1].lower()

        if not os.path.exists(arquivo_path):
            logger.error(f"Arquivo não encontrado: {arquivo_path}")
            notificar(f"Arquivo não encontrado: {arquivo_path}")
            return False

        logger.info(f"Iniciando execução do arquivo: {arquivo_path}")

        if ext in ('.kjb', '.ktr'):
            return executar_job_pentaho(id,arquivo_path, timeout)

        elif ext in ('.hwf', '.hpl'):
            return executar_hop(id,arquivo_path, projeto_hop, local_run_hop, timeout)

        elif ext in ('.bat', '.cmd', '.sh', '.ps1', '.py', ''):
            return executar_comando_terminal(
                id,
                comando=arquivo_path,
                cwd=os.path.dirname(arquivo_path),
                nome_arquivo=arquivo_path,
                ferramenta="TERMINAL",
                timeout=timeout
            )

        else:
            logger.error(f"Extensão de arquivo não suportada: {ext}")
            return False

    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}", exc_info=True)
        return False
    
def atualizar_execucao_no_banco(id_agendamento, duracao_execucao, ultima_execucao):
    """Atualiza a duração e data/hora da última execução do agendamento"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE agendamentos
            SET duracao_execucao = ?, ultima_execucao = ?
            WHERE id = ?
        """, (duracao_execucao, ultima_execucao, id_agendamento))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[ERRO] Falha ao atualizar execução no banco: {str(e)}")

def executar_job_pentaho(id, job_path, timeout):
    """Executa um job ou transformação do Pentaho PDI e monitora erros"""
    try:
        kitchen_path = config_os['pentaho_kitchen']
        pan_path = config_os['pentaho_pan']
        pentaho_dir = os.path.dirname(kitchen_path)

        logger.info(f"Executando job Pentaho: {job_path} Timeout: {timeout}")

        env = os.environ.copy()
        env.update({
            'PENTAHO_DI_JAVA_OPTIONS': '-Xms1024m -Xmx2048m',
            'KETTLE_HOME': pentaho_dir,
            'KETTLE_JNDI_ROOT': os.path.join(pentaho_dir, 'simple-jndi')
        })

        arquivo = os.path.abspath(os.path.normpath(job_path))
        extensao = Path(arquivo).suffix.lower()

        if extensao == '.ktr':
            comando = f'"{pan_path}" /file:"{arquivo}"'
        else:
            comando = f'"{kitchen_path}" /file:"{arquivo}"'

        logger.debug(f"Comando Pentaho: {comando}")
        #notificar(f"Comando Pentaho: {comando}", canais=['telegram'])

        startupinfo = None
        if config_os['shell']:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        linhas_erro = []

        processo = subprocess.Popen(
            comando,
            cwd=pentaho_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env,
            startupinfo=startupinfo,
            shell=config_os['shell']
        )

        start_time = time.time()

        with open(get_daily_log_path(), 'a', encoding='utf-8') as output_file:
            for linha in processo.stdout:
                output_file.write(f"[PID {processo.pid}] {linha}")
                output_file.flush()

                if "ERROR" in linha.upper():
                    linhas_erro.append(linha.strip())

        processo.wait(timeout=timeout)

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if linhas_erro:
            msg = (
                f"[Pentaho] ⚠️ Erros detectados na execução do arquivo:\n"
                f"📄 Arquivo: {os.path.basename(job_path)}\n\n"
                f"🧾 Erros:\n" + "\n".join(linhas_erro[-5:])  # mostra os últimos 5 erros para evitar overflow
            )
            logger.error(msg)
            notificar(msg)

        if processo.returncode == 0 and not linhas_erro:
            logger.info("[Pentaho] Execução concluída com sucesso")
            return True
        else:
            logger.error(f"[Pentaho] Processo finalizado com código {processo.returncode}")
            return False

    except subprocess.TimeoutExpired:
        msg = "[Pentaho] Timeout excedido - processo finalizado à força"
        logger.error(msg)
        notificar(msg)
        return False

    except Exception as e:
        logger.error(f"Erro inesperado na execução do Pentaho: {str(e)}", exc_info=True)
        notificar(f"Erro inesperado na execução do Pentaho: {str(e)}")
        return False

def executar_hop(id, arquivo_hop, projeto, local_run, timeout):
    """Executa um job/transformação do Apache Hop e monitora erros"""
    try:
        hop_run_path = config_os['hop_run']
        hop_dir = os.path.dirname(hop_run_path)

        logger.info(f"Executando arquivo Hop: {arquivo_hop} Timeout: {timeout}")
        logger.info(f"Projeto: {projeto}, Local Run: {local_run}")

        comando = [
            hop_run_path,
            '--file', arquivo_hop,
            '--project', projeto,
            '--runconfig', local_run,
            '--level', 'Basic'
        ]

        logger.debug(f"Comando Hop: {' '.join(comando)}")

        erro_detectado = False
        linha_erro = ""

        processo = subprocess.Popen(
            comando,
            cwd=hop_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            shell=config_os['shell']
        )

        start_time = time.time()

        with open(get_daily_log_path(), 'a', encoding='utf-8') as output_file:
            for linha in processo.stdout:
                output_file.write(f"[PID {processo.pid}] {linha}")
                output_file.flush()

                if "ERROR" in linha.upper():
                    erro_detectado = True
                    linha_erro = linha.strip()

        processo.wait(timeout=timeout)

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if erro_detectado:
            msg = (
                f"[HOP] Erro detectado na execução do arquivo:\n"
                f"📄 Arquivo: {os.path.basename(arquivo_hop)}\n"
                f"🧾 Linha: {linha_erro}"
            )
            logger.error(msg)
            notificar(msg)

        if processo.returncode == 0 and not erro_detectado:
            logger.info("[HOP] Execução concluída com sucesso")
            return True
        else:
            logger.error(f"[HOP] Processo finalizado com código {processo.returncode}")
            return False

    except subprocess.TimeoutExpired:
        msg = "[HOP] Timeout excedido - processo finalizado à força"
        logger.error(msg)
        notificar(msg)
        return False

    except Exception as e:
        logger.error(f"Erro inesperado na execução do Hop: {str(e)}", exc_info=True)
        notificar(f"Erro inesperado na execução do Hop: {str(e)}")
        return False
    
def executar_comando_terminal(id,comando, cwd, nome_arquivo, ferramenta="TERMINAL", timeout=1800):
    """Executa um comando genérico no terminal, monitora o log e envia notificações em caso de erro"""
    try:
        logger.info(f"[{ferramenta}] Executando comando: {' '.join(comando)} Timeout {timeout}")
        
        erro_detectado = False
        linhas_erro = []

        processo = subprocess.Popen(
            comando,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            shell=config_os.get('shell', False)
        )

        start_time = time.time()

        with open(get_daily_log_path(), 'a', encoding='utf-8') as output_file:
            for linha in processo.stdout:
                output_file.write(f"[PID {processo.pid}] {linha}")
                output_file.flush()

                if any(p in linha.upper() for p in ["ERROR", "EXCEPTION", "FATAL"]):
                    erro_detectado = True
                    linhas_erro.append(linha.strip())

        processo.wait(timeout=timeout)

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if erro_detectado:
            msg = (
                f"[{ferramenta}] Erro detectado na execução do arquivo:\n"
                f"📄 Arquivo: {os.path.basename(nome_arquivo)}\n"
                f"🧾 Erros:\n" + '\n'.join(linhas_erro)
            )
            logger.error(msg)
            notificar(msg)

        if processo.returncode == 0 and not erro_detectado:
            logger.info(f"[PID {processo.pid}][{ferramenta}] Execução concluída com sucesso")
            return True
        else:
            logger.error(f"[PID {processo.pid}][{ferramenta}] Processo finalizado com código {processo.returncode}")
            return False

    except subprocess.TimeoutExpired:
        msg = f"[{ferramenta}] Timeout excedido - processo finalizado à força"
        logger.error(msg)
        notificar(msg)
        return False

    except Exception as e:
        logger.error(f"[{ferramenta}] Erro inesperado: {str(e)}", exc_info=True)
        notificar(f"[{ferramenta}] Erro inesperado: {str(e)}")
        return False

def monitorar_processo(processo, timeout):
    """Monitora o processo e gerencia timeouts"""
    start_time = time.time()
    
    try:
        while True:
            if processo.poll() is not None:
                logger.info(f"Processo finalizado com código: {processo.returncode}")
                return processo.returncode == 0

            if (time.time() - start_time) > timeout:
                logger.error(f"Timeout de {timeout} segundos excedido")
                notificar(f"Timeout de {timeout} segundos excedido")
                processo.kill()
                return False

            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário")
        processo.kill()
        return False
    except Exception as e:
        logger.error(f"Erro no monitoramento: {str(e)}")
        notificar(f"Erro no monitoramento: {str(e)}")
        processo.kill()
        return False

if __name__ == '__main__':
    logger.info("==== Início da Execução ETL ====")

    if len(sys.argv) < 3:
        logger.error("Uso: python script.py <id> <arquivo> [projeto_hop] [local_run_hop] [timeout]")
        sys.exit(1)

    id_execucao = sys.argv[1]
    arquivo = sys.argv[2]

    projeto = sys.argv[3] if len(sys.argv) > 3 else None
    local_run = sys.argv[4] if len(sys.argv) > 4 else None

    # Captura o timeout como inteiro, se informado
    try:
        timeout = int(sys.argv[5]) if len(sys.argv) > 5 else 3600
    except ValueError:
        timeout = 3600

    success = executar_etl(
        id_execucao,
        arquivo_path=arquivo,
        projeto_hop=projeto,
        local_run_hop=local_run,
        timeout=timeout
    )

    if success:
        logger.info("Execução concluída com sucesso!")
        sys.exit(0)
    else:
        logger.error("Falha na execução")
        sys.exit(1)