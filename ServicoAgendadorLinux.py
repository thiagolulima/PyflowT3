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

#!/usr/bin/env python3
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
import signal
import unicodedata

# Configuração do diretório de trabalho
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SERVICE_DIR)

# Definição de comandos para executar na linha de comando (ajuste para Linux)
APACHE_HOP = '/opt/Apache-hop/hop-run.sh'
PENTAHO_JOB = '/opt/data-integration/kitchen.sh'
PENTAHO_TRANSFORMATION = '/opt/data-integration/pan.sh'

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

# Configuração do locale para Português Brasil no Linux
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'pt_BR')
    except locale.Error:
        pass

DIAS_SEMANA_MAP = {
    'mon': 'seg', 'tue': 'ter', 'wed': 'qua', 'thu': 'qui', 'fri': 'sex', 'sat': 'sab', 'sun': 'dom'
}

def remover_acentuacao(texto):
    texto_normalizado = unicodedata.normalize('NFD', texto)
    texto_sem_acento = ''.join(
        c for c in texto_normalizado 
        if not unicodedata.combining(c)
    )
    return texto_sem_acento

def log_event(mensagem):
    """Registra mensagens no log diário"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{timestamp} - {mensagem}"
    
    try:
        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            log_file.write(log_line + "\n")
    except Exception as e:
        logging.error(f"Erro ao escrever no log: {str(e)}")
    logging.info(log_line)

def executar_pentaho(arquivo_kjb, timeout=3600):
    """Executa jobs/transformações do Pentaho no Linux"""
    try:
        arquivo = os.path.abspath(os.path.normpath(arquivo_kjb))
        extensao = Path(arquivo).suffix.lower()
        
        if extensao == '.ktr':
            comando = [PENTAHO_TRANSFORMATION, f'/file="{arquivo}"']
            diretorio = os.path.dirname(PENTAHO_TRANSFORMATION)
        else:
            comando = [PENTAHO_JOB, f'/file="{arquivo}"']
            diretorio = os.path.dirname(PENTAHO_JOB)

        log_event(f"[PENTAHO] Iniciando execução do arquivo: {arquivo}")
        
        # Variáveis de ambiente para o Pentaho no Linux
        env = os.environ.copy()
        env.update({
            'PENTAHO_DI_JAVA_OPTIONS': '-Xms1024m -Xmx2048m',
            'KETTLE_HOME': diretorio,
            'KETTLE_JNDI_ROOT': os.path.join(diretorio, 'simple-jndi')
        })

        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            processo = subprocess.Popen(
                comando,
                cwd=diretorio,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                env=env
            )
            
            try:
                processo.wait(timeout=timeout)
                if processo.returncode == 0:
                    log_event("[PENTAHO] Executado com sucesso")
                else:
                    log_event(f"[PENTAHO] Erro (Código: {processo.returncode})")
                return processo.returncode
                
            except subprocess.TimeoutExpired:
                processo.kill()
                log_event("[PENTAHO] Timeout excedido - processo terminado")
                return 1
                
    except Exception as e:
        log_event(f"[PENTAHO] Erro crítico: {str(e)}")
        raise

def executar_hop(arquivo, projeto, ambiente, timeout=1800):
    """Executa workflows/pipelines do Apache Hop no Linux"""
    try:
        arquivo = os.path.abspath(os.path.normpath(arquivo))
        comando = [
            APACHE_HOP,
            '-j', projeto,
            '-r', ambiente,
            '-f', arquivo
        ]
        
        log_event(f"[HOP] Executando: {' '.join(comando)}")
        
        with open(get_daily_log_path(), "a", encoding='utf-8') as log_file:
            processo = subprocess.Popen(
                comando,
                cwd=os.path.dirname(APACHE_HOP),
                stdout=log_file,
                stderr=subprocess.STDOUT
            )
            
            try:
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

class AgendadorHopService:
    def __init__(self):
        self.stop_event = threading.Event()
        self.timeout = 30  # 30 segundos
        self.main_thread = None
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        self.verificar_ambiente()

    def handle_signal(self, signum, frame):
        """Manipula sinais de término"""
        log_event(f"Recebido sinal {signum}, parando serviço...")
        self.stop()

    def verificar_ambiente(self):
        """Verifica requisitos do ambiente antes de iniciar"""
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Banco de dados não encontrado: {DB_PATH}")
        
        log_event("Verificando ambiente...")
        log_event(f"Diretório do serviço: {SERVICE_DIR}")
        log_event(f"Python: {sys.version}")

    def stop(self):
        """Para o serviço de forma controlada"""
        log_event("Recebido comando para parar o serviço")
        self.stop_event.set()
        
        if self.main_thread and self.main_thread.is_alive():
            self.main_thread.join(timeout=10.0)
            
        log_event("Serviço parado com sucesso")

    def run(self):
        """Método principal de execução do serviço"""
        log_event(f"Serviço iniciado. Versão: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        self.main_thread = threading.Thread(
            target=self._main_loop,
            name="MainServiceLoop",
            daemon=True
        )
        self.main_thread.start()
        self.main_thread.join()

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
        
        deve_executar = False
        
        intervalo = int(intervalo) if intervalo and str(intervalo).isdigit() else 0
        dias_semana_list = [d.strip().lower() for d in dias_semana_ag.split(",")] if dias_semana_ag else []
        dias_mes_list = [d.strip() for d in dias_mes_ag.split(",")] if dias_mes_ag else []
        
        if horario and horario.strip():
            if horario == hora_atual:
                deve_executar = True
            else:
                return False
        
        if dias_semana_list:
            if remover_acentuacao(dia_semana.lower()) not in dias_semana_list:
                return False
        
        if dias_mes_list:
            if str(dia_mes) not in dias_mes_list:
                return False
        
        if hora_inicio and hora_fim:
            if not (hora_inicio <= hora_atual <= hora_fim):
                return False
            
            if intervalo > 0:
                hora_min = hora_inicio.split(':')
                hora_min_ref = int(hora_min[0]) * 60 + int(hora_min[1])
                hora_min_atual = agora.hour * 60 + agora.minute
                delta_minutos = hora_min_atual - hora_min_ref
                
                if delta_minutos >= 0 and delta_minutos % intervalo == 0:
                    deve_executar = True
        elif intervalo > 0:
            if minuto_atual % intervalo == 0:
                deve_executar = True
        else:
            deve_executar = True
        
        if deve_executar and not self.stop_event.is_set():
            log_event(f"Agendamento cumpre condições para execução: {arquivo}")
            
            try:
                if ferramenta_etl == 'PENTAHO':
                    processo = multiprocessing.Process(
                        target=executar_pentaho,
                        args=(arquivo,),
                        kwargs={'timeout': 7200},
                        name=f"Pentaho_{Path(arquivo).name}"
                    )
                else:
                    processo = multiprocessing.Process(
                        target=executar_hop,
                        args=(arquivo, projeto, local_run),
                        name=f"Hop_{Path(arquivo).name}"
                    )
                
                processo.start()
                log_event(f"Processo iniciado (PID: {processo.pid})")
                
            except Exception as e:
                log_event(f"Falha ao iniciar processo: {str(e)}")
        
        return deve_executar

if __name__ == '__main__':
    service = AgendadorHopService()
    
    try:
        log_event("Iniciando serviço AgendadorHopService")
        service.run()
    except Exception as e:
        log_event(f"Erro fatal: {str(e)}")
        sys.exit(1)