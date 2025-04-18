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

import sys
import sqlite3
import datetime
import os
import subprocess
from dotenv import load_dotenv

from PyQt6.QtWidgets import (QApplication, QWidget, QVBoxLayout, QLabel, 
                            QPushButton, QTextEdit, QTableWidget, 
                            QTableWidgetItem, QLineEdit, QDateEdit,QCheckBox,QMessageBox)
from PyQt6.QtCore import QDate, Qt , QTimer
from PyQt6.QtGui import QPixmap , QIcon ,QTextCursor
from executaWorkflow import executar_etl
from PyQt6.QtWidgets import QHBoxLayout
hbox_atualizacao = QHBoxLayout()

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "agendador.db")

class AgendadorGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        self.setWindowTitle("PyFlowT3 - Monitoramento do Agendador de Workflows e Pipelines")
        self.setGeometry(100, 100, 1000, 700)
        self.setWindowIcon(QIcon('pyflowt3.ico'))  # ou .png, .jpg
        
        layout = QVBoxLayout()
        
        # Seção de Agendamentos
        self.label_agendamentos = QLabel("Agendamentos ativos:")
        layout.addWidget(self.label_agendamentos)
        
        self.pesquisa_agendamentos = QLineEdit()
        self.pesquisa_agendamentos.setPlaceholderText("Pesquisar agendamentos...")
        self.pesquisa_agendamentos.textChanged.connect(self.carregar_agendamentos)
        layout.addWidget(self.pesquisa_agendamentos)
        
        self.tabela_agendamentos = QTableWidget()
        self.tabela_agendamentos.setSortingEnabled(True)
        layout.addWidget(self.tabela_agendamentos)
        
        # Botão de executar agora
        self.btn_executar = QPushButton("Executar agora")
        self.btn_executar.clicked.connect(self.executa_workflow)
        hbox_atualizacao.addWidget(self.btn_executar)

        # Botão de Atualização
        self.btn_atualizar = QPushButton("Atualizar Dados")
        self.btn_atualizar.clicked.connect(self.atualizar_tudo)
        hbox_atualizacao.addWidget(self.btn_atualizar)

        self.auto_update_logs = QCheckBox("Atualizar logs a cada 5s")
        self.auto_update_logs.stateChanged.connect(self.toggle_auto_update_logs)
        hbox_atualizacao.addWidget(self.auto_update_logs)

        layout.addLayout(hbox_atualizacao)

        # Timer para atualização automática
        self.log_timer = QTimer()
        self.log_timer.setInterval(5000)  # 5000ms = 5 segundos
        self.log_timer.timeout.connect(self.carregar_logs)
        
        # Seção de Logs
        self.label_logs = QLabel("Logs do Dia:")
        layout.addWidget(self.label_logs)
        
        self.data_log = QDateEdit()
        self.data_log.setCalendarPopup(True)
        self.data_log.setDate(QDate.currentDate())
        self.data_log.dateChanged.connect(self.carregar_logs)
        layout.addWidget(self.data_log)
        
        self.pesquisa_logs = QLineEdit()
        self.pesquisa_logs.setPlaceholderText("Pesquisar nos logs...")
        self.pesquisa_logs.textChanged.connect(self.carregar_logs)
        layout.addWidget(self.pesquisa_logs)
        
        self.texto_logs = QTextEdit()
        self.texto_logs.setReadOnly(True)
        layout.addWidget(self.texto_logs)

        # Adicionar a logo centralizada abaixo dos logs
        self.logo_label = QLabel()
        self.carregar_logo()
        self.logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.logo_label.setContentsMargins(0, 5, 0, 0)  # Margem apenas no topo
        layout.addWidget(self.logo_label)
        
        self.setLayout(layout)
        self.atualizar_tudo()

    def toggle_auto_update_logs(self, state):
        if state == Qt.CheckState.Checked.value:
            self.log_timer.start()
        else:
            self.log_timer.stop()
    def carregar_logo(self):
        """Carrega a logo da aplicação"""
        try:
            # Tenta carregar em diferentes formatos
            pixmap = QPixmap('pyflowt3.ico')
            if pixmap.isNull():
                pixmap = QPixmap('pyflowt3.png')
            
            if not pixmap.isNull():
                # Redimensionar mantendo proporção
                pixmap = pixmap.scaled(150, 100, 
                                     Qt.AspectRatioMode.KeepAspectRatio,
                                     Qt.TransformationMode.SmoothTransformation)
                self.logo_label.setPixmap(pixmap)
            else:
                # Texto alternativo se a imagem não for encontrada
                self.logo_label.setText("PyFlowT3 Monitor")
                self.logo_label.setStyleSheet("""
                    font-size: 16px; 
                    font-weight: bold;
                    color: #333;
                    padding: 5px;
                """)
        except Exception as e:
            print(f"Erro ao carregar logo: {e}")
    
    def atualizar_tudo(self):
        """Atualiza tanto os agendamentos quanto os logs"""
        self.carregar_agendamentos()
        self.carregar_logs()
    
    def carregar_agendamentos(self):
        try:
            termo_pesquisa = self.pesquisa_agendamentos.text().strip().lower()
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Consulta mais segura com parâmetros
            cursor.execute("""
                SELECT id, arquivo, projeto, local_run, ultima_execucao,duracao_execucao,horario, intervalo, dias_semana, dias_mes, hora_inicio, hora_fim,ferramenta_etl 
                FROM agendamentos where Status = 'Ativo'
                ORDER BY horario
            """)
            
            agendamentos = cursor.fetchall()
            conn.close()
            
            # Filtragem
            filtrados = [row for row in agendamentos 
                        if termo_pesquisa in " ".join(map(str, row)).lower()]
            
            # Configuração da tabela
            self.tabela_agendamentos.setRowCount(len(filtrados))
            self.tabela_agendamentos.setColumnCount(13)
            self.tabela_agendamentos.setHorizontalHeaderLabels(
                ["ID","Arquivo", "Projeto" , "Local_run", "Última Execução","Duração","Horário", "Intervalo", "Dias Semana", "Dias Mês", "Hora Início", "Hora Fim","Execução"])
            
            # Preenchimento dos dados
            for i, row in enumerate(filtrados):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    item.setFlags(item.flags() )  # Tornar não editável
                    self.tabela_agendamentos.setItem(i, j, item)
                    
            # Ajustar tamanho das colunas
            self.tabela_agendamentos.resizeColumnsToContents()
            
        except Exception as e:
            self.label_agendamentos.setText(f"Erro ao carregar agendamentos: {str(e)}")
    
    def carregar_logs(self): 
        try:
            data_selecionada = self.data_log.date().toString("ddMMyyyy")
            log_path = f"logs/agendador{data_selecionada}.log"
            
            if not os.path.exists(log_path):
                self.texto_logs.setPlainText(f"Arquivo de log não encontrado para {data_selecionada}")
                return
            
            with open(log_path, "r", encoding="utf-8", errors="replace") as file:
                logs = file.readlines()
            
            termo_pesquisa = self.pesquisa_logs.text().strip().lower()
            logs_filtrados = [linha for linha in logs if termo_pesquisa in linha.lower()]
            
            self.texto_logs.setPlainText("".join(logs_filtrados))
            
            # Rolar para o final
            self.texto_logs.moveCursor(QTextCursor.MoveOperation.End)

        except PermissionError:
            self.texto_logs.setPlainText("Sem permissão para ler o arquivo de log")
        except Exception as e:
            self.texto_logs.setPlainText(f"Erro ao carregar logs: {str(e)}")

    def executa_workflow(self):
            """Executa workflows ou pipelines selecionados"""
            linha_selecionada = self.tabela_agendamentos.currentRow()
            if linha_selecionada == -1:
                QMessageBox.warning(self, "Seleção", "Por favor, selecione um agendamento que deseja executar.")
                return
            ferramenta_etl = self.tabela_agendamentos.item(linha_selecionada, 12).text()
            projeto = self.tabela_agendamentos.item(linha_selecionada, 2).text()
            arquivo = self.tabela_agendamentos.item(linha_selecionada, 1).text()
            local = self.tabela_agendamentos.item(linha_selecionada, 3).text()
            id = self.tabela_agendamentos.item(linha_selecionada, 0).text()
            
            resposta = QMessageBox.question(
                self, "Confirmar Execução", 
                f"Tem certeza que deseja executar o agendamento: '{arquivo}'?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if resposta == QMessageBox.StandardButton.Yes:
                if ferramenta_etl == 'PENTAHO':
                    subprocess.Popen([sys.executable, 'executaWorkflow.py', id, arquivo ]) 
                elif ferramenta_etl == 'APACHE_HOP':
                    subprocess.Popen([sys.executable, 'executaWorkflow.py', id, arquivo , projeto , local])     
                else:
                    subprocess.Popen([sys.executable, 'executaWorkflow.py',id, arquivo])             
                #QMessageBox.information(self, "Sucesso", "Agendamento enviado para execução, acompanhe no monitoramento!")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Configurar estilo visual
    app.setStyle('Fusion')
    
    gui = AgendadorGUI()
    gui.show()
    sys.exit(app.exec())