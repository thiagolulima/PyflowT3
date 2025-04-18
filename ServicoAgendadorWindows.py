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

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import time
import sqlite3
import subprocess
import datetime
import locale
import threading
import multiprocessing
import logging
from pathlib import Path
import ctypes
import unicodedata
from dotenv import load_dotenv
from notifications.notifier import notificar

# Configuração do diretório de trabalho
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SERVICE_DIR)

# Configuração para evitar problemas com DLLs
ctypes.windll.kernel32.SetDllDirectoryW(None)

# Definição de comandos para executar na linha de comando
load_dotenv()
APACHE_HOP = os.getenv("APACHE_HOP", r'C:\Apache-hop\hop-run.bat')
PENTAHO_JOB = os.getenv("PENTAHO_JOB", r'C:\data-integration\Kitchen.bat') 
PENTAHO_TRANSFORMATION = os.getenv("PENTAHO_TRANSFORMATION", r'C:\data-integration\Pan.bat')  

# Configurações do aplicativo
DB_PATH = os.path.join(SERVICE_DIR, "agendador.db")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SERVICE_DIR, 'agendador_service.log')),
        logging.StreamHandler()
    ]
)

# Gera o nome do arquivo de log com a data atual
def get_daily_log_path():
    log_dir = os.path.join(SERVICE_DIR, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    data_atual = datetime.datetime.now().strftime("%d%m%Y")
    return os.path.join(log_dir, f"agendador{data_atual}.log")

# Configuração do locale para Português Brasil no Windows
try:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
    except locale.Error:
        pass

DIAS_SEMANA_MAP = {
    'mon': 'seg', 'tue': 'ter', 'wed': 'qua', 'thu': 'qui', 'fri': 'sex', 'sat': 'sab', 'sun': 'dom'
}
def remover_acentuacao(texto):
    # Normaliza o texto para a forma NFD (decomposição de caracteres)
    texto_normalizado = unicodedata.normalize('NFD', texto)
    
    # Filtra apenas caracteres que não são diacríticos (acentos)
    texto_sem_acento = ''.join(
        c for c in texto_normalizado 
        if not unicodedata.combining(c)
    )
    
    return texto_sem_acento
def log_event(mensagem):
    """Registra mensagens no log diário e no Event Viewer"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{timestamp} - {mensagem}"
    
    try:
        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            log_file.write(log_line + "\n")
    except Exception as e:
        logging.error(f"Erro ao escrever no log: {str(e)}")
        notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")
    
    try:
        servicemanager.LogInfoMsg(log_line)
    except Exception as e:
        logging.error(f"Erro ao registrar no Event Viewer: {str(e)}")
        notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")

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
        log_event(f"[ERRO] Falha ao atualizar execução no banco: {str(e)}")

def executar_pentaho(id, arquivo_kjb, timeout=3600):
    """Executa jobs/transformações do Pentaho com tratamento especial para serviço Windows"""
    try:
        arquivo = os.path.abspath(os.path.normpath(arquivo_kjb))
        extensao = Path(arquivo).suffix.lower()

        if extensao == '.ktr':
            comando = f'"{PENTAHO_TRANSFORMATION}" /file:"{arquivo}"'
            diretorio = os.path.dirname(PENTAHO_TRANSFORMATION)
        else:
            comando = f'"{PENTAHO_JOB}" /file:"{arquivo}"'
            diretorio = os.path.dirname(PENTAHO_JOB)

        log_event(f"[PENTAHO] Iniciando execução do arquivo: {arquivo}")
        log_event(f"[PENTAHO] Comando completo: {comando}")
        log_event(f"[PENTAHO] Diretório de trabalho: {diretorio}")

        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        env = os.environ.copy()
        env.update({
            'PENTAHO_DI_JAVA_OPTIONS': '-Xms1024m -Xmx2048m',
            'KETTLE_HOME': diretorio,
            'KETTLE_JNDI_ROOT': os.path.join(diretorio, 'simple-jndi'),
            'TEMP': os.environ.get('TEMP', r'C:\Temp'),
            'TMP': os.environ.get('TMP', r'C:\Temp')
        })

        os.makedirs(env['TEMP'], exist_ok=True)

        log_event(f"[PENTAHO] Variáveis de ambiente configuradas")

        linhas_erro = []

        processo = subprocess.Popen(
            comando,
            cwd=diretorio,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env,
            startupinfo=startupinfo,
            shell=True  # Necessário no Windows como serviço
        )

        start_time = time.time()
        karaf_initialized = False
        karaf_timeout = 300  # 5 minutos para o Karaf

        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            while True:
                output = processo.stdout.readline()
                if output == '' and processo.poll() is not None:
                    break

                if output:
                    log_file.write(output)
                    log_file.flush()

                    if "ERROR" in output.upper():
                        linhas_erro.append(output.strip())

                    if not karaf_initialized and "OSGI Service Port" in output:
                        karaf_initialized = True
                        log_event("[PENTAHO] Karaf inicializado com sucesso")

                    if not karaf_initialized and (time.time() - start_time) > karaf_timeout:
                        raise subprocess.TimeoutExpired(comando, karaf_timeout)

                    if (time.time() - start_time) > timeout:
                        raise subprocess.TimeoutExpired(comando, timeout)

        return_code = processo.wait()

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if linhas_erro:
            msg = (
                f"[Pentaho] ⚠️ Erros detectados na execução do arquivo:\n"
                f"📄 Arquivo: {os.path.basename(arquivo)}\n\n"
                f"🧾 Erros:\n" + "\n".join(linhas_erro[-5:])  # últimos 5 erros
            )
            log_event(msg)
            notificar(msg)

        if return_code == 0 and not linhas_erro:
            log_event("[PENTAHO] Executado com sucesso")
        else:
            log_event(f"[PENTAHO] Erro (Código: {return_code})")
            notificar(f"[PENTAHO] Erro (Código: {return_code})")

            if not karaf_initialized:
                log_event("[PENTAHO] Falha na inicialização do Karaf")
                notificar("[PENTAHO] Falha na inicialização do Karaf")

        return return_code

    except subprocess.TimeoutExpired as e:
        processo.kill()
        if not karaf_initialized:
            msg = "[PENTAHO] Timeout na inicialização do Karaf"
        else:
            msg = "[PENTAHO] Timeout na execução do job"

        log_event(msg)
        notificar(msg)
        return 1

    except Exception as e:
        log_event(f"[PENTAHO] Erro crítico: {str(e)}")
        notificar(f"[PyFlowT3] Erro crítico: {str(e)}")
        raise

def executar_hop(id,arquivo, projeto, ambiente, timeout=1800):
    """Executa workflows/pipelines do Apache Hop"""
    try:
        arquivo = os.path.abspath(os.path.normpath(arquivo))
        comando = [
            APACHE_HOP,
            '-j', projeto,
            '-r', ambiente,
            '-f', arquivo
        ]

        log_event(f"[HOP] Executando: {' '.join(comando)}")
        log_event(f"[HOP] Diretório: {os.path.dirname(APACHE_HOP)}")

        erros_detectados = []

        processo = subprocess.Popen(
            comando,
            cwd=os.path.dirname(APACHE_HOP),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            shell=True  # Necessário para execução como serviço no Windows
        )

        start_time = time.time()

        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            while True:
                linha = processo.stdout.readline()
                if linha == '' and processo.poll() is not None:
                    break

                if linha:
                    log_file.write(linha)
                    log_file.flush()

                    if any(p in linha.upper() for p in ['ERROR', 'EXCEPTION', 'FATAL']):
                        erros_detectados.append(linha.strip())

                if time.time() - start_time > timeout:
                    raise subprocess.TimeoutExpired(comando, timeout)

        processo.wait()

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if processo.returncode == 0 and not erros_detectados:
            log_event("[HOP] Executado com sucesso")
        else:
            log_event(f"[HOP] Erro (Código: {processo.returncode})")
            if erros_detectados:
                msg = (
                    f"[HOP] ⚠️ Erros detectados no arquivo:\n"
                    f"📄 Arquivo: {os.path.basename(arquivo)}\n\n"
                    f"🧾 Linhas de erro:\n" + "\n".join(erros_detectados[-5:])  # últimos 5 erros
                )
                log_event(msg)
                notificar(msg)
            else:
                notificar(f"[HOP] Erro (Código: {processo.returncode})")

        return processo.returncode

    except subprocess.TimeoutExpired:
        processo.kill()
        msg = "[HOP] Timeout excedido - processo terminado"
        log_event(msg)
        notificar(msg)
        return 1

    except Exception as e:
        msg = f"[HOP] Erro inesperado: {str(e)}"
        log_event(msg)
        notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")
        raise
    
def executar_comando_terminal(id,comando, timeout=1800, descricao="Comando genérico"):
    """
    Executa um comando ou script no terminal (por exemplo .bat, .cmd, .sh, python, etc.)
    
    Args:
        comando (str): Comando completo a ser executado.
        timeout (int): Tempo máximo de execução em segundos.
        descricao (str): Texto descritivo para logs e notificações.
    
    Returns:
        int: Código de retorno do processo.
    """
    try:
        log_event(f"[CMD] Iniciando: {descricao}")
        log_event(f"[CMD] Comando: {comando}")

        erros_detectados = []

        processo = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            shell=True  # Necessário para .bat e comandos do terminal
        )

        start_time = time.time()

        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            while True:
                linha = processo.stdout.readline()
                if linha == '' and processo.poll() is not None:
                    break

                if linha:
                    log_file.write(linha)
                    log_file.flush()

                    if any(p in linha.upper() for p in ['ERROR', 'EXCEPTION', 'FATAL']):
                        erros_detectados.append(linha.strip())

                if time.time() - start_time > timeout:
                    raise subprocess.TimeoutExpired(comando, timeout)

        processo.wait()

        end_time = time.time()
        duracao = round((end_time - start_time) / 60, 2)  # em minutos
        ultima_execucao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        atualizar_execucao_no_banco(id, duracao, ultima_execucao)

        if processo.returncode == 0 and not erros_detectados:
            log_event(f"[CMD] Finalizado com sucesso: {descricao}")
        else:
            msg = f"[CMD] Erro ao executar: {descricao} (Código: {processo.returncode})"
            if erros_detectados:
                msg += "\n🧾 Linhas com erro:\n" + "\n".join(erros_detectados[-5:])
            log_event(msg)
            notificar(msg)

        return processo.returncode

    except subprocess.TimeoutExpired:
        processo.kill()
        msg = f"[CMD] Timeout excedido na execução de: {descricao}"
        log_event(msg)
        notificar(msg)
        return 1

    except Exception as e:
        msg = f"[CMD] Erro inesperado ao executar '{descricao}': {str(e)}"
        log_event(msg)
        notificar(msg)
        raise

def obter_dia_semana_ptbr():
    """Retorna o dia da semana em português"""
    dia_eng = datetime.datetime.now().strftime("%a").lower()[:3]
    return DIAS_SEMANA_MAP.get(dia_eng, dia_eng)

class AgendadorHopService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AgendadorHopService"
    _svc_display_name_ = "Agendador de Workflows e Pepilines ETL pyflowt3"
    _svc_description_ = "Serviço para agendamento e execução de workflows e pipelines ETL"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_event = threading.Event()
        self.timeout = 30000  # 30 segundos
        self.main_thread = None
        socket.setdefaulttimeout(60)
        self.criar_banco_dados()
        self.verificar_ambiente()

    def criar_banco_dados(self):
        """Cria o banco de dados e tabela se não existirem"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agendamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                arquivo TEXT NOT NULL,
                projeto TEXT NULL,
                local_run TEXT NULL,
                horario TEXT,
                intervalo INTEGER,
                dias_semana TEXT,
                dias_mes TEXT,
                hora_inicio TEXT,
                hora_fim TEXT,
                status TEXT NOT NULL DEFAULT 'Ativo',
                ferramenta_etl TEXT,
                ultima_execucao DATETIME,
                duracao_execucao REAL                
                )
            """)
        
        cursor.execute("PRAGMA table_info(agendamentos)")
        colunas = [info[1] for info in cursor.fetchall()]
        
        if 'ultima_execucao' not in colunas:
            cursor.execute("ALTER TABLE agendamentos ADD COLUMN ultima_execucao DATETIME")
            conn.commit()
            conn.close()

        if 'duracao_execucao' not in colunas:
            cursor.execute("ALTER TABLE agendamentos ADD COLUMN duracao_execucao REAL")
            conn.commit()
            conn.close()

    def verificar_ambiente(self):
        """Verifica requisitos do ambiente antes de iniciar"""
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Banco de dados não encontrado: {DB_PATH}")
        
        log_event("Verificando ambiente...")
        log_event(f"Diretório do serviço: {SERVICE_DIR}")
        log_event(f"Python: {sys.version}")

    def SvcStop(self):
        """Para o serviço de forma controlada"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        log_event("Recebido comando para parar o serviço")
        self.stop_event.set()
        win32event.SetEvent(self.hWaitStop)
        
        if self.main_thread and self.main_thread.is_alive():
            self.main_thread.join(timeout=10.0)
            
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)
        log_event("Serviço parado com sucesso")

    def SvcDoRun(self):
        """Método principal de execução do serviço"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        log_event(f"Serviço iniciado. Versão: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        self.main_thread = threading.Thread(
            target=self._main_loop,
            name="MainServiceLoop",
            daemon=True
        )
        self.main_thread.start()
        
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

    def _main_loop(self):
        """Loop principal de verificação de agendamentos"""
        log_event("Iniciando loop principal de verificação")
        
        while not self.stop_event.is_set():
            try:
                self._verificar_agendamentos()
                
                # Espera 60 segundos ou até receber sinal de parada
                for _ in range(60):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
                    
            except Exception as e:
                log_event(f"Erro no loop principal: {str(e)}")
                notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")
                if not self.stop_event.is_set():
                    time.sleep(10)
        
        log_event("Loop principal finalizado")

    def _verificar_agendamentos(self):
        """Verifica agendamentos no banco de dados"""
        if self.stop_event.is_set():
            return
            
        try:
            agora = datetime.datetime.now()
            dia_semana = obter_dia_semana_ptbr()
            dia_mes = str(agora.day)
            hora_atual = agora.strftime("%H:%M")
            minuto_atual = agora.minute
            
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT arquivo, horario, intervalo, dias_semana, 
                           dias_mes, hora_inicio, hora_fim, projeto, 
                           local_run, ferramenta_etl,id
                    FROM agendamentos
                    WHERE status = 'Ativo'
                """)
                
                for agendamento in cursor:
                    if self.stop_event.is_set():
                        break
                    self._processar_agendamento(agendamento, hora_atual, dia_semana, dia_mes, agora, minuto_atual)
                    
        except Exception as e:
            log_event(f"Erro ao verificar agendamentos: {str(e)}")
            notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")

    def _processar_agendamento(self, agendamento, hora_atual, dia_semana, dia_mes, agora, minuto_atual):
        """Processa um agendamento individual com todas as condições combinadas"""
        (arquivo, horario, intervalo, dias_semana_ag, dias_mes_ag, 
        hora_inicio, hora_fim, projeto, local_run, ferramenta_etl,id) = agendamento
        
        # Inicializa como False - só deve executar se TODAS as condições aplicáveis forem atendidas
        deve_executar = False
        
        # Pré-processamento dos dados
        intervalo = int(intervalo) if intervalo and str(intervalo).isdigit() else 0
        dias_semana_list = [d.strip().lower() for d in dias_semana_ag.split(",")] if dias_semana_ag else []
        dias_mes_list = [d.strip() for d in dias_mes_ag.split(",")] if dias_mes_ag else []
        
        # 1. Verificação de horário fixo (tem precedência sobre tudo)
        if horario and horario.strip():
            if horario == hora_atual:
                deve_executar = True
            else:
                return False  # Horário fixo não coincide - ignora todas outras condições
        
        # 2. Verificação de dias da semana (se especificado)
        if dias_semana_list:
            if remover_acentuacao(dia_semana.lower()) not in dias_semana_list:
                #log_event(f"Dias da semana | hoje {dia_semana.lower()} | Agendado: {dias_semana_list}")
                return False
        
        # 3. Verificação de dias do mês (se especificado)
        if dias_mes_list:
            if str(dia_mes) not in dias_mes_list:
                return False
        
        # 4. Verificação de janela de horário e intervalo
        if hora_inicio and hora_fim:
            if not (hora_inicio <= hora_atual <= hora_fim):
                return False
            
            # Se tem intervalo, verifica o minuto exato
            if intervalo > 0:
                hora_min = hora_inicio.split(':')
                hora_min_ref = int(hora_min[0]) * 60 + int(hora_min[1])
                hora_min_atual = agora.hour * 60 + agora.minute
                delta_minutos = hora_min_atual - hora_min_ref
                
                if delta_minutos >= 0 and delta_minutos % intervalo == 0:
                    deve_executar = True
        elif intervalo > 0:
            # Verifica apenas o intervalo sem janela de horário
            if minuto_atual % intervalo == 0:
                deve_executar = True
        else:
            # Caso não tenha nem intervalo nem janela de horário
            deve_executar = True
        
        # Execução se todas as condições foram atendidas
        if deve_executar and not self.stop_event.is_set():
            log_event(f"Agendamento cumpre condições para execução: {arquivo}")
            
            try:
                if ferramenta_etl == 'PENTAHO':
                    processo = multiprocessing.Process(
                        target=executar_pentaho,
                        args=(id, arquivo,),
                        kwargs={'timeout': 7200},  # 2 horas para jobs complexos
                        name=f"Pentaho_{Path(arquivo).name}"
                    )
                elif ferramenta_etl == 'APACHE_HOP':
                    processo = multiprocessing.Process(
                        target=executar_hop,
                        args=(id,arquivo, projeto, local_run),
                        name=f"Hop_{Path(arquivo).name}"
                    )
                else:
                    processo = multiprocessing.Process(
                        target=executar_comando_terminal,
                        args=(id, arquivo,),
                        kwargs={
                            'timeout': 1800,
                            'descricao': f"Execução terminal: {Path(arquivo).name}"
                        },
                        name=f"Terminal_{Path(arquivo).name}"
                    )

                processo.daemon = True
                processo.start()
                log_event(f"Processo iniciado (PID: {processo.pid})")
                
            except Exception as e:
                log_event(f"Falha ao iniciar processo: {str(e)}")
                notificar(f"[PyFlowT3] Erro ao executar o workflow: {str(e)}")
        
        return deve_executar

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(AgendadorHopService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(AgendadorHopService)