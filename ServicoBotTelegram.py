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
import threading
import sys
import os
import time
import logging
import requests
from pathlib import Path
from datetime import datetime

def setup_service_logger():
    """Configura o logger com rotação diária e criação de pasta de logs"""
    # Cria o diretório de logs se não existir
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Formato da data para o nome do arquivo (DDMMYYYY)
    date_str = datetime.now().strftime("%d%m%Y")
    log_filename = f"telegram_service{date_str}.log"
    log_path = log_dir / log_filename
    
    # Configuração do logger principal
    logger = logging.getLogger("TelegramBotService")
    logger.setLevel(logging.INFO)
    
    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Configura formatação
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler para arquivo (rotação diária)
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

class TelegramBotService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TelegramPyflowt3Bot"
    _svc_display_name_ = "Bot Telegram Agendador Pyflowt3"
    _svc_description_ = "Serviço que executa comandos do Telegram para workflows e pipelines pyflowt3."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_event = threading.Event()
        self.bot_thread = None
        
        # Configura logger diário
        self.logger = setup_service_logger()
        
        # Configura diretório de trabalho
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(self.script_dir)
        sys.path.insert(0, self.script_dir)
        
        self.logger.info("Serviço inicializado com sucesso")

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.logger.info("Recebido comando de parada do serviço")
        self.stop_event.set()
        
        if self.bot_thread and self.bot_thread.is_alive():
            self.logger.info("Aguardando finalização da thread do bot...")
            self.bot_thread.join(timeout=10.0)
            
        win32event.SetEvent(self.hWaitStop)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)
        self.logger.info("Serviço parado com sucesso")

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.logger.info("Serviço iniciado e em execução")
        
        try:
            from bot_telegram import run_bot
            self.bot_thread = threading.Thread(
                target=run_bot,
                args=(self.stop_event,),
                name="TelegramBotThread",
                daemon=True
            )
            self.bot_thread.start()
            
            # Loop principal de verificação
            while not self.stop_event.is_set():
                if not self.bot_thread.is_alive():
                    self.logger.error("Thread do bot parou inesperadamente! Reiniciando...")
                    self.bot_thread = threading.Thread(
                        target=run_bot,
                        args=(self.stop_event,),
                        name="TelegramBotThread_Restarted",
                        daemon=True
                    )
                    self.bot_thread.start()
                
                time.sleep(5)
                
        except Exception as e:
            self.logger.critical(f"Erro crítico no serviço: {str(e)}", exc_info=True)
            raise
        finally:
            self.logger.info("Serviço finalizando")
            self.ReportServiceStatus(win32service.SERVICE_STOPPED)

if __name__ == '__main__':
    # Configura logger temporário para a inicialização
    temp_logger = setup_service_logger()
    
    try:
        if len(sys.argv) == 1:
            temp_logger.info("Iniciando serviço no modo de despacho...")
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(TelegramBotService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            temp_logger.info(f"Processando comando de linha: {sys.argv}")
            win32serviceutil.HandleCommandLine(TelegramBotService)
    except Exception as e:
        temp_logger.critical(f"Falha na inicialização do serviço: {str(e)}", exc_info=True)
        raise