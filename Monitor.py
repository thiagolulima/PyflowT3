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
from dotenv import load_dotenv

from PyQt6.QtWidgets import (QApplication, QWidget, QVBoxLayout, QLabel, 
                            QPushButton, QTextEdit, QTableWidget, 
                            QTableWidgetItem, QLineEdit, QDateEdit)
from PyQt6.QtCore import QDate, Qt   
from PyQt6.QtGui import QPixmap , QIcon

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
        
        # Botão de Atualização
        self.btn_atualizar = QPushButton("Atualizar Dados")
        self.btn_atualizar.clicked.connect(self.atualizar_tudo)
        layout.addWidget(self.btn_atualizar)
        
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
                SELECT arquivo, horario, intervalo, dias_semana, dias_mes, hora_inicio, hora_fim 
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
            self.tabela_agendamentos.setColumnCount(7)
            self.tabela_agendamentos.setHorizontalHeaderLabels(
                ["Arquivo", "Horário", "Intervalo", "Dias Semana", "Dias Mês", "Hora Início", "Hora Fim"])
            
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
            
            # Verificar se o arquivo existe
            if not os.path.exists(log_path):
                self.texto_logs.setText(f"Arquivo de log não encontrado para {data_selecionada}")
                return
            
            # Ler com o mesmo encoding usado na gravação
            with open(log_path, "r", encoding="utf-8", errors="replace") as file:
                logs = file.readlines()
            
            # Filtrar por termo de pesquisa
            termo_pesquisa = self.pesquisa_logs.text().strip().lower()
            logs_filtrados = [linha for linha in logs if termo_pesquisa in linha.lower()]
            
            # Exibir resultados
            self.texto_logs.setText("".join(logs_filtrados))
            
        except PermissionError:
            self.texto_logs.setText(f"Sem permissão para ler o arquivo de log")
        except Exception as e:
            self.texto_logs.setText(f"Erro ao carregar logs: {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Configurar estilo visual
    app.setStyle('Fusion')
    
    gui = AgendadorGUI()
    gui.show()
    sys.exit(app.exec())