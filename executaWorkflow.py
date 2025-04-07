import os
import subprocess
import time
import logging
from pathlib import Path
import sys
import platform
import time
import datetime
from dotenv import load_dotenv

load_dotenv()
# Configuração do diretório de trabalho
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SERVICE_DIR)

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

def executar_etl(arquivo_path, projeto_hop=None, local_run_hop=None, timeout=1800):
    """
    Executa jobs/transformações do Pentaho PDI ou Apache Hop
    
    Args:
        arquivo_path (str): Caminho completo para o arquivo (.kjb, .hwf, .hpl)
        projeto_hop (str, optional): Nome do projeto Hop (apenas para Apache Hop)
        local_run_hop (str, optional): Caminho do local_run (apenas para Apache Hop)
        timeout (int): Tempo máximo de execução em segundos
    
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        arquivo_path = os.path.abspath(os.path.normpath(arquivo_path))
        ext = os.path.splitext(arquivo_path)[1].lower()
        
        if not os.path.exists(arquivo_path):
            logger.error(f"Arquivo não encontrado: {arquivo_path}")
            return False

        logger.info(f"Iniciando execução do arquivo: {arquivo_path}")

        if ext in ('.kjb','.ktr'):
            return executar_job_pentaho(arquivo_path, timeout)
        elif ext in ('.hwf', '.hpl'):
            return executar_hop(arquivo_path, projeto_hop, local_run_hop, timeout)
        else:
            logger.error(f"Extensão de arquivo não suportada: {ext}")
            return False

    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}", exc_info=True)
        return False

def executar_job_pentaho(job_path, timeout):
    """Executa um job do Pentaho PDI"""
    try:
        kitchen_path = config_os['pentaho_kitchen']
        pan_path = config_os['pentaho_pan']
        pentaho_dir = os.path.dirname(kitchen_path)
        
        logger.info(f"Executando job Pentaho: {job_path}")

        env = os.environ.copy()
        env.update({
            'PENTAHO_DI_JAVA_OPTIONS': '-Xms1024m -Xmx2048m',
            'KETTLE_HOME': pentaho_dir,
            'KETTLE_JNDI_ROOT': os.path.join(pentaho_dir, 'simple-jndi')
        })

        comando = f'"{kitchen_path}" /file:"{job_path}"'
        arquivo = os.path.abspath(os.path.normpath(job_path))
        extensao = Path(arquivo).suffix.lower()
        
        # Definir comando baseado no tipo de arquivo
        if extensao == '.ktr':
            comando = f'{pan_path} /file:"{arquivo}"'
        else:
            comando = f'{kitchen_path} /file:"{arquivo}"'

        logger.debug(f"Comando Pentaho: {comando}")

        startupinfo = None
        if config_os['shell']:  # Windows
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        with open(get_daily_log_path(), 'a', encoding='utf-8') as output_file:
            processo = subprocess.Popen(
                comando,
                cwd=pentaho_dir,
                stdout=output_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env,
                startupinfo=startupinfo,
                shell=config_os['shell']
            )

            return monitorar_processo(processo, timeout)

    except Exception as e:
        logger.error(f"Erro na execução do Pentaho: {str(e)}", exc_info=True)
        return False

def executar_hop(arquivo_hop, projeto, local_run, timeout):
    """Executa um job/transformação do Apache Hop"""
    try:
        hop_run_path = config_os['hop_run']
        hop_dir = os.path.dirname(hop_run_path)
        
        logger.info(f"Executando arquivo Hop: {arquivo_hop}")
        logger.info(f"Projeto: {projeto}, Local Run: {local_run}")

        # Construir comando Hop
        comando = [
            hop_run_path,
            '--file', arquivo_hop,
            '--project', projeto,
            '--runconfig', local_run,
            '--level', 'Basic'
        ]

        logger.debug(f"Comando Hop: {' '.join(comando)}")

        with open(get_daily_log_path(), 'a', encoding='utf-8') as output_file:
            processo = subprocess.Popen(
                comando,
                cwd=hop_dir,
                stdout=output_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                shell=config_os['shell']
            )

            return monitorar_processo(processo, timeout)

    except Exception as e:
        logger.error(f"Erro na execução do Hop: {str(e)}", exc_info=True)
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
                processo.kill()
                return False

            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário")
        processo.kill()
        return False
    except Exception as e:
        logger.error(f"Erro no monitoramento: {str(e)}")
        processo.kill()
        return False

if __name__ == '__main__':
    logger.info("==== Início da Execução ETL ====")
    
    # Exemplo de uso:
    # Para Pentaho: python script.py caminho/arquivo.kjb
    # Para Hop: python script.py caminho/arquivo.hwf NomeProjeto /caminho/local_run
    
    if len(sys.argv) < 2:
        logger.error("Uso: python script.py <arquivo> [projeto_hop] [local_run_hop]")
        sys.exit(1)
    
    arquivo = sys.argv[1]
    projeto = sys.argv[2] if len(sys.argv) > 2 else None
    local_run = sys.argv[3] if len(sys.argv) > 3 else None
    
    success = executar_etl(
        arquivo_path=arquivo,
        projeto_hop=projeto,
        local_run_hop=local_run,
        timeout=3600  # 1 hora de timeout
    )
    
    if success:
        logger.info("Execução concluída com sucesso!")
        sys.exit(0)
    else:
        logger.error("Falha na execução")
        sys.exit(1)