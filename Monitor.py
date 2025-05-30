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
                            QTableWidgetItem, QLineEdit, QDateEdit, QCheckBox, QMessageBox, QHBoxLayout, QProgressBar)
from PyQt6.QtCore import QDate, Qt , QTimer
from PyQt6.QtGui import QPixmap , QIcon , QTextCursor
from executaWorkflow import executar_etl

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "agendador.db")

class AgendadorGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle("PyFlowT3 - Monitoramento do Agendador de Workflows e Pipelines")
        self.setGeometry(100, 100, 1000, 700)
        self.setWindowIcon(QIcon('pyflowt3.ico'))

        layout = QVBoxLayout()

        # Seção de Agendamentos
        self.label_agendamentos = QLabel("Agendamentos ativos:")
        layout.addWidget(self.label_agendamentos)

        self.pesquisa_agendamentos = QLineEdit()
        self.pesquisa_agendamentos.setPlaceholderText("Pesquisar agendamentos...")
        self.pesquisa_agendamentos.textChanged.connect(self.carregar_agendamentos)
        #self.pesquisa_agendamentos.returnPressed.connect(self.carregar_agendamentos)
        layout.addWidget(self.pesquisa_agendamentos)

        self.tabela_agendamentos = QTableWidget()
        self.tabela_agendamentos.setSortingEnabled(True)
        layout.addWidget(self.tabela_agendamentos)

        # Linha de botoes
        hbox_atualizacao = QHBoxLayout()

        self.btn_executar = QPushButton("Executar agora")
        self.btn_executar.clicked.connect(self.executa_workflow)
        hbox_atualizacao.addWidget(self.btn_executar)

        self.btn_atualizar = QPushButton("Atualizar Dados")
        self.btn_atualizar.clicked.connect(self.atualizar_tudo)
        hbox_atualizacao.addWidget(self.btn_atualizar)

        self.auto_update_logs = QCheckBox("Atualizar logs a cada 5s")
        self.auto_update_logs.stateChanged.connect(self.toggle_auto_update_logs)
        hbox_atualizacao.addWidget(self.auto_update_logs)

        layout.addLayout(hbox_atualizacao)

        # Timer para atualização automática
        self.log_timer = QTimer()
        self.log_timer.setInterval(5000)
        self.log_timer.timeout.connect(self.carregar_logs)

        # Seção de Logs
        self.label_logs = QLabel("Logs do Dia:")
        layout.addWidget(self.label_logs)

        hbox_log_filtro = QHBoxLayout()

        self.data_log = QDateEdit()
        self.data_log.setCalendarPopup(True)
        self.data_log.setDate(QDate.currentDate())
        hbox_log_filtro.addWidget(self.data_log)

        self.pesquisa_logs = QLineEdit()
        self.pesquisa_logs.setPlaceholderText("Pesquisar nos logs...")
        self.pesquisa_logs.returnPressed.connect(self.carregar_logs)
        hbox_log_filtro.addWidget(self.pesquisa_logs)

        self.btn_filtrar_logs = QPushButton("Filtrar")
        self.btn_filtrar_logs.clicked.connect(self.carregar_logs)
        hbox_log_filtro.addWidget(self.btn_filtrar_logs)

        layout.addLayout(hbox_log_filtro)

        self.loading_bar = QLabel("")
        layout.addWidget(self.loading_bar)

        self.texto_logs = QTextEdit()
        self.texto_logs.setReadOnly(True)
        layout.addWidget(self.texto_logs)

        self.logo_label = QLabel()
        self.carregar_logo()
        self.logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.logo_label.setContentsMargins(0, 5, 0, 0)
        layout.addWidget(self.logo_label)

        self.setLayout(layout)
        self.atualizar_tudo()

    def toggle_auto_update_logs(self, state):
        if state == Qt.CheckState.Checked.value:
            self.log_timer.start()
        else:
            self.log_timer.stop()

    def carregar_logo(self):
        try:
            pixmap = QPixmap('pyflowt3.ico')
            if pixmap.isNull():
                pixmap = QPixmap('pyflowt3.png')
            if not pixmap.isNull():
                pixmap = pixmap.scaled(150, 100, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                self.logo_label.setPixmap(pixmap)
            else:
                self.logo_label.setText("PyFlowT3 Monitor")
                self.logo_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333; padding: 5px;")
        except Exception as e:
            print(f"Erro ao carregar logo: {e}")

    def atualizar_tudo(self):
        self.carregar_agendamentos()
        self.carregar_logs()

    def carregar_agendamentos(self):
        try:
            termo_pesquisa = self.pesquisa_agendamentos.text().strip().lower()
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, arquivo, projeto, local_run, ultima_execucao, duracao_execucao, horario, intervalo, dias_semana, dias_mes, hora_inicio, hora_fim, ferramenta_etl,timeout_execucao
                FROM agendamentos WHERE Status = 'Ativo' ORDER BY horario
            """)
            agendamentos = cursor.fetchall()
            conn.close()

            filtrados = [row for row in agendamentos if termo_pesquisa in " ".join(map(str, row)).lower()]

            # 🔧 Desliga ordenação e limpa a tabela antes de preencher novamente
            self.tabela_agendamentos.setSortingEnabled(False)
            self.tabela_agendamentos.clearContents()
            self.tabela_agendamentos.setRowCount(len(filtrados))
            self.tabela_agendamentos.setColumnCount(14)
            self.tabela_agendamentos.setHorizontalHeaderLabels(
                ["ID", "Arquivo", "Projeto", "Local_run", "Última Execução", "Duração", "Horário", "Intervalo", "Dias Semana", "Dias Mês", "Hora Início", "Hora Fim", "Execução","Timeout"]
            )

            for i, row in enumerate(filtrados):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    item.setFlags(item.flags())
                    self.tabela_agendamentos.setItem(i, j, item)

            self.tabela_agendamentos.resizeColumnsToContents()
            self.tabela_agendamentos.setSortingEnabled(True)  # 🔄 Reativa a ordenação

        except Exception as e:
            self.label_agendamentos.setText(f"Erro ao carregar agendamentos: {str(e)}")


    def carregar_logs(self):
        self.loading_bar.setText("Carregando logs...")
        QApplication.processEvents()

        try:
            data_selecionada = self.data_log.date().toString("ddMMyyyy")
            log_path = f"logs/agendador{data_selecionada}.log"

            if not os.path.exists(log_path):
                self.texto_logs.setPlainText(f"Arquivo de log não encontrado para {data_selecionada}")
                self.loading_bar.setText("")
                return

            with open(log_path, "r", encoding="utf-8", errors="replace") as file:
                logs = file.readlines()

            termo_pesquisa = self.pesquisa_logs.text().strip().lower()
            logs_filtrados = [linha for linha in logs if termo_pesquisa in linha.lower()]
            self.texto_logs.setPlainText("".join(logs_filtrados))
            self.texto_logs.moveCursor(QTextCursor.MoveOperation.End)

        except PermissionError:
            self.texto_logs.setPlainText("Sem permissão para ler o arquivo de log")
        except Exception as e:
            self.texto_logs.setPlainText(f"Erro ao carregar logs: {str(e)}")
        finally:
            self.loading_bar.setText("")

    def executa_workflow(self):
        linha_selecionada = self.tabela_agendamentos.currentRow()
        if linha_selecionada == -1:
            QMessageBox.warning(self, "Seleção", "Por favor, selecione um agendamento que deseja executar.")
            return

        ferramenta_etl = self.tabela_agendamentos.item(linha_selecionada, 12).text()
        projeto = self.tabela_agendamentos.item(linha_selecionada, 2).text()
        arquivo = self.tabela_agendamentos.item(linha_selecionada, 1).text()
        local = self.tabela_agendamentos.item(linha_selecionada, 3).text()
        id = self.tabela_agendamentos.item(linha_selecionada, 0).text()
        timeout = self.tabela_agendamentos.item(linha_selecionada, 13).text()
        if not timeout.isdigit():
            timeout = "1800"

        resposta = QMessageBox.question(
            self, "Confirmar Execução", 
            f"Tem certeza que deseja executar o agendamento: '{arquivo}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if resposta == QMessageBox.StandardButton.Yes:
            args = [sys.executable, 'executaWorkflow.py', id, arquivo]
            if ferramenta_etl == 'APACHE_HOP':
                args += [projeto, local, timeout]
            elif ferramenta_etl == 'PENTAHO' or ferramenta_etl == 'TERMINAL':
                args += [timeout]
            subprocess.Popen(args)              

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    gui = AgendadorGUI()
    gui.show()
    sys.exit(app.exec())
