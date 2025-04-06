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
    
    try:
        servicemanager.LogInfoMsg(log_line)
    except Exception as e:
        logging.error(f"Erro ao registrar no Event Viewer: {str(e)}")

def executar_pentaho(arquivo_kjb, timeout=3600):
    """Executa jobs/transformações do Pentaho com tratamento especial para serviço Windows"""
    try:
        arquivo = os.path.abspath(os.path.normpath(arquivo_kjb))
        extensao = Path(arquivo).suffix.lower()
        
        # Definir comando baseado no tipo de arquivo
        if extensao == '.ktr':
            comando = f'{PENTAHO_TRANSFORMATION} /file:"{arquivo}"'
            diretorio = os.path.dirname(PENTAHO_TRANSFORMATION)
        else:
            comando = f'{PENTAHO_JOB} /file:"{arquivo}"'
            diretorio = os.path.dirname(PENTAHO_JOB)

        log_event(f"[PENTAHO] Iniciando execução do arquivo: {arquivo}")
        log_event(f"[PENTAHO] Comando completo: {comando}")
        log_event(f"[PENTAHO] Diretório de trabalho: {diretorio}")
        
        # Configuração especial para serviço Windows
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        
        # Variáveis de ambiente críticas para o Pentaho
        env = os.environ.copy()
        env.update({
            'PENTAHO_DI_JAVA_OPTIONS': '-Xms1024m -Xmx2048m',
            'KETTLE_HOME': diretorio,
            'KETTLE_JNDI_ROOT': os.path.join(diretorio, 'simple-jndi'),
            'TEMP': os.environ.get('TEMP', r'C:\Temp'),
            'TMP': os.environ.get('TMP', r'C:\Temp')
        })

        # Garantir que o diretório TEMP exista
        os.makedirs(env['TEMP'], exist_ok=True)

        log_event(f"[PENTAHO] Variáveis de ambiente configuradas")
        
        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            # Executar em um shell para garantir o contexto correto
            processo = subprocess.Popen(
                comando,
                cwd=diretorio,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env,
                startupinfo=startupinfo,
                shell=True  # Crucial para funcionar como serviço
            )
            
            start_time = time.time()
            karaf_initialized = False
            karaf_timeout = 300  # 5 minutos para inicializar o Karaf
            
            try:
                # Processar saída em tempo real
                while True:
                    output = processo.stdout.readline()
                    if output == '' and processo.poll() is not None:
                        break
                        
                    if output:
                        log_file.write(output)
                        log_file.flush()
                        
                        # Verificar se o Karaf foi inicializado
                        if not karaf_initialized and "OSGI Service Port" in output:
                            karaf_initialized = True
                            log_event("[PENTAHO] Karaf inicializado com sucesso")
                            
                        # Verificar timeout específico para inicialização do Karaf
                        if not karaf_initialized and (time.time() - start_time) > karaf_timeout:
                            raise subprocess.TimeoutExpired(comando, karaf_timeout, output, None)
                            
                        # Verificar timeout geral
                        if (time.time() - start_time) > timeout:
                            raise subprocess.TimeoutExpired(comando, timeout, output, None)
                
                # Capturar erros restantes
                errors = processo.stderr.read()
                if errors:
                    log_file.write("\nERROS:\n" + errors)
                    log_file.flush()
                
                return_code = processo.wait()
                
                if return_code == 0:
                    log_event("[PENTAHO] Executado com sucesso")
                else:
                    log_event(f"[PENTAHO] Erro (Código: {return_code})")
                    if not karaf_initialized:
                        log_event("[PENTAHO] Falha na inicialização do Karaf")
                
                return return_code
                
            except subprocess.TimeoutExpired as e:
                processo.kill()
                if not karaf_initialized:
                    log_event("[PENTAHO] Timeout na inicialização do Karaf")
                else:
                    log_event("[PENTAHO] Timeout na execução do job")
                return 1
                
    except Exception as e:
        log_event(f"[PENTAHO] Erro crítico: {str(e)}")
        raise

def executar_hop(arquivo, projeto, ambiente, timeout=1800):
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
        
        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            processo = subprocess.Popen(
                comando,
                cwd=os.path.dirname(APACHE_HOP),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            
            try:
                for linha in processo.stdout:
                    log_file.write(linha)
                    log_file.flush()
                
                erros = processo.stderr.read()
                if erros:
                    log_file.write("\nERROS:\n" + erros)
                    log_file.flush()
                
                processo.wait(timeout=timeout)
                
                if processo.returncode == 0:
                    log_event("[HOP] Executado com sucesso")
                else:
                    log_event(f"[HOP] Erro (Código: {processo.returncode})")
                
                return processo.returncode
                
            except subprocess.TimeoutExpired:
                processo.kill()
                log_event("[HOP] Timeout excedido - processo terminado")
                return 1
                
    except Exception as e:
        log_event(f"[HOP] Erro inesperado: {str(e)}")
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
        self.verificar_ambiente()

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
                           local_run, ferramenta_etl
                    FROM agendamentos
                    WHERE status = 'Ativo'
                """)
                
                for agendamento in cursor:
                    if self.stop_event.is_set():
                        break
                    self._processar_agendamento(agendamento, hora_atual, dia_semana, dia_mes, agora, minuto_atual)
                    
        except Exception as e:
            log_event(f"Erro ao verificar agendamentos: {str(e)}")

    def _processar_agendamento(self, agendamento, hora_atual, dia_semana, dia_mes, agora, minuto_atual):
        """Processa um agendamento individual com todas as condições combinadas"""
        (arquivo, horario, intervalo, dias_semana_ag, dias_mes_ag, 
        hora_inicio, hora_fim, projeto, local_run, ferramenta_etl) = agendamento
        
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
                        args=(arquivo,),
                        kwargs={'timeout': 7200},  # 2 horas para jobs complexos
                        name=f"Pentaho_{Path(arquivo).name}"
                    )
                else:
                    processo = multiprocessing.Process(
                        target=executar_hop,
                        args=(arquivo, projeto, local_run),
                        name=f"Hop_{Path(arquivo).name}"
                    )
                
                processo.daemon = True
                processo.start()
                log_event(f"Processo iniciado (PID: {processo.pid})")
                
            except Exception as e:
                log_event(f"Falha ao iniciar processo: {str(e)}")
        
        return deve_executar

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(AgendadorHopService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(AgendadorHopService)