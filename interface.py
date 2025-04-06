from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QLabel, QPushButton, 
    QTableWidget, QTableWidgetItem, QLineEdit, QFileDialog, QMessageBox, QHBoxLayout, QComboBox, QHeaderView, QAbstractItemView,
    QCheckBox
)
from PyQt6.QtCore import Qt, QRegularExpression
from PyQt6.QtGui import QRegularExpressionValidator
import sqlite3
import sys

DB_PATH = "agendador.db"

class AgendadorGUI(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("PyFlowT3 - Agendador de Workflows e Pipelines")
        self.setGeometry(100, 100, 1000, 600)

        # Variável para armazenar o ID do agendamento sendo editado
        self.agendamento_editando = None

        # Criando o widget central e layout principal
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.layout_principal = QVBoxLayout()
        central_widget.setLayout(self.layout_principal)

        # Frame de busca
        frame_busca = QHBoxLayout()
        self.layout_principal.addLayout(frame_busca)

        frame_busca.addWidget(QLabel("Buscar:"))
        self.entry_busca = QLineEdit()
        self.entry_busca.setPlaceholderText("Digite para buscar...")
        self.entry_busca.textChanged.connect(self.buscar_dinamica)
        frame_busca.addWidget(self.entry_busca)
        self.btn_mostrar_todos = QPushButton("Mostrar Todos")
        self.btn_mostrar_todos.clicked.connect(self.mostrar_todos)
        frame_busca.addWidget(self.btn_mostrar_todos)

        # Layout de grade para os campos de entrada
        self.layout_grid = QGridLayout()
        self.layout_principal.addLayout(self.layout_grid)

        # Linha 1: Arquivo + Input + Botão Selecionar
        self.layout_grid.addWidget(QLabel("Arquivo:*"), 0, 0)
        self.entry_arquivo = QLineEdit()
        self.layout_grid.addWidget(self.entry_arquivo, 0, 1)
        self.btn_selecionar = QPushButton("Selecionar")
        self.btn_selecionar.clicked.connect(self.selecionar_arquivo)
        self.layout_grid.addWidget(self.btn_selecionar, 0, 2)

        
        # Linha 2: Arquivo + Input + Botão Selecionar
        self.layout_grid.addWidget(QLabel("Ferramenta ETL:*"), 1, 0)
        self.entry_etl = QComboBox()
        self.entry_etl.addItems(["APACHE_HOP", "PENTAHO"])
        self.layout_grid.addWidget(self.entry_etl, 1, 1)

        # Linha 3: Projeto
        self.layout_grid.addWidget(QLabel("Projeto HOP:*"), 2, 0)
        self.entry_projeto = QLineEdit()
        self.layout_grid.addWidget(self.entry_projeto, 2, 1, 1, 2)

        # Linha 4: Local RUN HOP
        self.layout_grid.addWidget(QLabel("Local RUN HOP:*"), 3, 0)
        self.entry_local = QLineEdit()
        self.layout_grid.addWidget(self.entry_local, 3, 1, 1, 2)

        # Linha 5: Horário fixo (com máscara HH:MM)
        self.layout_grid.addWidget(QLabel("Horário fixo (HH:MM):"), 4, 0)
        self.entry_horario = QLineEdit()
        self.entry_horario.setPlaceholderText("HH:MM")

        # Configurando validador para formato HH:MM
        regex_horario = QRegularExpression("^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$")
        validator_horario = QRegularExpressionValidator(regex_horario)
        self.entry_horario.setValidator(validator_horario)
        self.layout_grid.addWidget(self.entry_horario, 4, 1, 1, 2)

        # Linha 6: Intervalo em minutos (apenas números inteiros)
        self.layout_grid.addWidget(QLabel("Intervalo (minutos):"), 5, 0)
        self.entry_intervalo = QLineEdit()

        # Configurando validador para números inteiros
        self.entry_intervalo.setValidator(QRegularExpressionValidator(QRegularExpression("[0-9]*")))
        self.layout_grid.addWidget(self.entry_intervalo, 5, 1, 1, 2)

        # Linha 7: Dias da semana
        # Por este código:
        self.layout_grid.addWidget(QLabel("Dias da Semana:"), 6, 0)

        # Cria um widget container para os checkboxes
        self.dias_semana_widget = QWidget()
        self.dias_semana_layout = QHBoxLayout()
        self.dias_semana_widget.setLayout(self.dias_semana_layout)

        checkbox_todos = QCheckBox("Todos")
        checkbox_todos.stateChanged.connect(self.toggle_todos_dias)
        self.dias_semana_layout.insertWidget(0, checkbox_todos)

        # Cria checkboxes para cada dia
        dias = ['seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom']
        self.checkboxes_dias_semana = {}

        for dia in dias:
            checkbox = QCheckBox(dia)
            self.checkboxes_dias_semana[dia] = checkbox
            self.dias_semana_layout.addWidget(checkbox)

        self.layout_grid.addWidget(self.dias_semana_widget, 6, 1, 1, 2)

        # Linha 8: Dias do mês (apenas números inteiros separados por vírgula)
        self.layout_grid.addWidget(QLabel("Dias do Mês (1, 15, 30):"), 7, 0)
        self.entry_dias_mes = QLineEdit()
        # Configurando validador para números e vírgulas
        self.entry_dias_mes.setValidator(QRegularExpressionValidator(QRegularExpression("^[0-9,]*$")))
        self.layout_grid.addWidget(self.entry_dias_mes, 7, 1, 1, 2)

        # Linha 9: Hora Início (com máscara HH:MM)
        self.layout_grid.addWidget(QLabel("Hora Início (HH:MM):"), 8, 0)
        self.entry_hora_inicio = QLineEdit()
        self.entry_hora_inicio.setPlaceholderText("HH:MM")
        self.entry_hora_inicio.setValidator(validator_horario)
        self.layout_grid.addWidget(self.entry_hora_inicio, 8, 1, 1, 2)

        # Linha 10: Hora Fim (com máscara HH:MM)
        self.layout_grid.addWidget(QLabel("Hora Fim (HH:MM):"), 9, 0)
        self.entry_hora_fim = QLineEdit()
        self.entry_hora_fim.setPlaceholderText("HH:MM")
        self.entry_hora_fim.setValidator(validator_horario)
        self.layout_grid.addWidget(self.entry_hora_fim, 9, 1, 1, 2)

        # Linha 11: Status (Ativo Sim/Não)
        self.layout_grid.addWidget(QLabel("Status:"), 10, 0)
        self.combo_status = QComboBox()
        self.combo_status.addItems(["Ativo", "Inativo"])
        self.layout_grid.addWidget(self.combo_status, 10, 1, 1, 2)

        # Botão de salvar/cancelar
        self.btn_salvar = QPushButton("Salvar Agendamento")
        self.btn_salvar.clicked.connect(self.salvar_no_banco)
        self.layout_principal.addWidget(self.btn_salvar)

        self.btn_cancelar = QPushButton("Cancelar Edição")
        self.btn_cancelar.clicked.connect(self.cancelar_edicao)
        self.btn_cancelar.setVisible(False)
        self.layout_principal.addWidget(self.btn_cancelar)

        # Tabela de agendamentos
        self.tabela = QTableWidget()
        self.tabela.setColumnCount(12)
        self.tabela.setHorizontalHeaderLabels([
            "ID", "Arquivo", "Projeto", "Local RUN HOP", "Horário", 
            "Intervalo", "Dias Semana", "Dias Mês", "Hora Início", 
            "Hora Fim", "Status", "ETL"
        ])
        
        # Configurações de seleção (PyQt6)
        self.tabela.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.tabela.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        
        # Ajuste de redimensionamento (PyQt6) - FORMA CORRETA
        header = self.tabela.horizontalHeader()
        ResizeMode = QHeaderView.ResizeMode  # Atalho para os modos de redimensionamento
        
        header.setSectionResizeMode(0, ResizeMode.ResizeToContents)  # ID
        header.setSectionResizeMode(1, ResizeMode.Interactive)       # Arquivo
        header.setSectionResizeMode(2, ResizeMode.ResizeToContents)  # Projeto
        header.setSectionResizeMode(3, ResizeMode.ResizeToContents)   # Local RUN HOP
        header.setSectionResizeMode(4, ResizeMode.ResizeToContents)  # Horário
        header.setSectionResizeMode(5, ResizeMode.ResizeToContents)   # Intervalo
        header.setSectionResizeMode(6, ResizeMode.ResizeToContents)   # Dias Semana
        header.setSectionResizeMode(7, ResizeMode.ResizeToContents)   # Dias Mês
        header.setSectionResizeMode(8, ResizeMode.ResizeToContents)   # Hora Início
        header.setSectionResizeMode(9, ResizeMode.ResizeToContents)   # Hora Fim
        header.setSectionResizeMode(10, ResizeMode.ResizeToContents)  # Status
        header.setSectionResizeMode(11, ResizeMode.ResizeToContents)  # ETL
        
        # Larguras mínimas recomendadas
        self.tabela.setColumnWidth(1, 200)  # Arquivo
        self.tabela.setColumnWidth(2, 150)  # Projeto
        self.tabela.setColumnWidth(3, 200)  # Local RUN HOP
        
        # Melhor visualização
        self.tabela.setWordWrap(True)
        self.tabela.setTextElideMode(Qt.TextElideMode.ElideNone)
        self.tabela.setAlternatingRowColors(True)
        
        self.layout_principal.addWidget(self.tabela)

        # fim tabela

        # Layout horizontal para botões Editar e Excluir
        botoes_layout = QHBoxLayout()
        
        self.btn_editar = QPushButton("Editar")
        self.btn_editar.clicked.connect(self.editar_agendamento)
        botoes_layout.addWidget(self.btn_editar)

        self.btn_excluir = QPushButton("Excluir")
        self.btn_excluir.clicked.connect(self.excluir_agendamento)
        botoes_layout.addWidget(self.btn_excluir)

        self.layout_principal.addLayout(botoes_layout)

        self.criar_banco_dados()
        self.listar_agendamentos()

    def get_dias_semana(self):
        """Retorna os dias selecionados no formato 'seg,ter,qua'"""
        dias_selecionados = [dia for dia, checkbox in self.checkboxes_dias_semana.items() 
                            if checkbox.isChecked()]
        return ','.join(dias_selecionados)

    def set_dias_semana(self, dias_str):
        """Configura os checkboxes a partir de uma string 'seg,ter,qua'"""
        if not dias_str:
            return
            
        dias_selecionados = dias_str.split(',')
        for dia, checkbox in self.checkboxes_dias_semana.items():
            checkbox.setChecked(dia in dias_selecionados)

    def limpar_dias_semana(self):
        """Limpa todos os checkboxes de forma segura"""
        if hasattr(self, 'checkbox_todos'):
            self.checkbox_todos.setChecked(False)
        
        if hasattr(self, 'checkboxes_dias_semana'):
            for checkbox in self.checkboxes_dias_semana.values():
                checkbox.setChecked(False)

    def toggle_todos_dias(self, state):
        for checkbox in self.checkboxes_dias_semana.values():
            checkbox.setChecked(state == 2)  # 2 = Qt.Checked

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
                ferramenta_etl TEXT
                       
            )
        """)
        cursor.execute("PRAGMA table_info(agendamentos)")
        colunas = [info[1] for info in cursor.fetchall()]
    
        if 'status' not in colunas:
            cursor.execute("ALTER TABLE agendamentos ADD COLUMN status TEXT NOT NULL DEFAULT 'Ativo'")
            conn.commit()
            conn.close()

        if 'ferramenta_etl' not in colunas:
            cursor.execute("ALTER TABLE agendamentos ADD COLUMN ferramenta_etl TEXT NOT NULL DEFAULT 'APACHE_HOP'")
            conn.commit()
            conn.close()

    def listar_agendamentos(self, filtro=None):
        """Carrega os agendamentos do banco e exibe na tabela"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if filtro:
            query = """
                SELECT id, arquivo, projeto, local_run, horario, intervalo, 
                       dias_semana, dias_mes, hora_inicio, hora_fim, status,ferramenta_etl
                FROM agendamentos 
                WHERE projeto LIKE ? OR arquivo LIKE ? OR local_run LIKE ?
            """
            cursor.execute(query, (f'%{filtro}%', f'%{filtro}%', f'%{filtro}%'))
        else:
            query = """
                SELECT id, arquivo, projeto, local_run, horario, intervalo, 
                       dias_semana, dias_mes, hora_inicio, hora_fim, status,ferramenta_etl
                FROM agendamentos
            """
            cursor.execute(query)
            
        rows = cursor.fetchall()
        conn.close()

        self.tabela.setRowCount(len(rows))
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                self.tabela.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))

    def buscar_dinamica(self):
        """Realiza a busca dinâmica conforme o usuário digita"""
        texto_busca = self.entry_busca.text().strip()
        if texto_busca:
            self.listar_agendamentos(texto_busca)
        else:
            self.listar_agendamentos()

    def mostrar_todos(self):
        """Mostra todos os agendamentos e limpa a busca"""
        self.entry_busca.clear()
        self.listar_agendamentos()

    def selecionar_arquivo(self):
        """Abre um diálogo para selecionar um arquivo"""
        arquivo, _ = QFileDialog.getOpenFileName(self, "Selecionar Arquivo", "", "Workflows (*.hwf *.hpl *.ktr *.kjb );;Todos os arquivos (*.*)")
        if arquivo:
            self.entry_arquivo.setText(arquivo)

    def limpar_campos(self):
        """Limpa todos os campos de entrada"""
        self.entry_arquivo.clear()
        self.entry_projeto.clear()
        self.entry_local.clear()
        self.entry_horario.clear()
        self.entry_intervalo.clear()
        self.entry_dias_mes.clear()
        self.entry_hora_inicio.clear()
        self.entry_hora_fim.clear()
        self.combo_status.setCurrentIndex(0)  # Define como "Ativo"
        self.entry_etl.setCurrentIndex(0)  # Define como "Ativo"
        self.limpar_dias_semana()
        self.agendamento_editando = None

    def validar_campos(self):
        """Valida os campos obrigatórios e formatos"""
        if self.entry_etl.currentText() == 'PENTAHO':
            campos_obrigatorios = {
            "Arquivo": self.entry_arquivo.text().strip()
        }
        else:
            campos_obrigatorios = {
                "Arquivo": self.entry_arquivo.text().strip(),
                "Projeto": self.entry_projeto.text().strip(),
                "Local RUN HOP": self.entry_local.text().strip()
            }
        # Verifica campos obrigatórios
        for campo, valor in campos_obrigatorios.items():
            if not valor:
                QMessageBox.warning(self, "Campo Obrigatório", f"O campo {campo} é obrigatório!")
                return False
        
        # Verifica formato dos horários
        horarios = {
            "Horário fixo": self.entry_horario.text().strip(),
            "Hora Início": self.entry_hora_inicio.text().strip(),
            "Hora Fim": self.entry_hora_fim.text().strip()
        }
        
        for campo, valor in horarios.items():
            if valor and len(valor) != 5 and ":" not in valor:
                QMessageBox.warning(self, "Formato Inválido", f"O campo {campo} deve estar no formato HH:MM!")
                return False
        
        return True

    def salvar_no_banco(self):
        """Salva os dados no banco de dados"""
        if not self.validar_campos():
            return

        arquivo = self.entry_arquivo.text()

        if self.entry_etl.currentText() == 'PENTAHO':
            projeto = 'PDI'
            local =   'PDI'
        else:
            projeto = self.entry_projeto.text()
            local = self.entry_local.text()
        horario = self.entry_horario.text()
        intervalo = self.entry_intervalo.text()
        dias_semana = self.get_dias_semana()
        dias_mes = self.entry_dias_mes.text()
        hora_inicio = self.entry_hora_inicio.text()
        hora_fim = self.entry_hora_fim.text()
        status = self.combo_status.currentText()
        etl = self.entry_etl.currentText() 

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if self.agendamento_editando:
            # Atualiza o agendamento existente
            cursor.execute("""
                UPDATE agendamentos SET
                    arquivo = ?,
                    projeto = ?,
                    local_run = ?,
                    horario = ?,
                    intervalo = ?,
                    dias_semana = ?,
                    dias_mes = ?,
                    hora_inicio = ?,
                    hora_fim = ?,
                    status = ?,
                    ferramenta_etl = ?
                WHERE id = ?
            """, (arquivo, projeto, local, horario, intervalo, dias_semana, dias_mes, hora_inicio, hora_fim, status, etl, self.agendamento_editando))
            mensagem = "Agendamento atualizado com sucesso!"
        else:
            # Insere um novo agendamento
            cursor.execute("""
                INSERT INTO agendamentos (
                    arquivo, projeto, local_run, horario, intervalo, 
                    dias_semana, dias_mes, hora_inicio, hora_fim, status, ferramenta_etl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (arquivo, projeto, local, horario, intervalo, dias_semana, dias_mes, hora_inicio, hora_fim, status,etl))
            mensagem = "Agendamento salvo com sucesso!"
            
        conn.commit()
        conn.close()

        QMessageBox.information(self, "Sucesso", mensagem)
        self.limpar_campos()
        self.listar_agendamentos()
        self.btn_cancelar.setVisible(False)
        self.btn_salvar.setText("Salvar Agendamento")

    def editar_agendamento(self):
        """Edita um agendamento existente"""
        linha_selecionada = self.tabela.currentRow()
        if linha_selecionada == -1:
            QMessageBox.warning(self, "Seleção", "Por favor, selecione um agendamento para editar.")
            return

        id_agendamento = int(self.tabela.item(linha_selecionada, 0).text())
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT arquivo, projeto, local_run, horario, intervalo, 
                   dias_semana, dias_mes, hora_inicio, hora_fim, status,ferramenta_etl
            FROM agendamentos WHERE id = ?
        """, (id_agendamento,))
        agendamento = cursor.fetchone()
        conn.close()

        if agendamento:
            self.agendamento_editando = id_agendamento
            self.entry_arquivo.setText(agendamento[0] or "")
            self.entry_projeto.setText(agendamento[1] or "")
            self.entry_local.setText(agendamento[2] or "")
            self.entry_horario.setText(agendamento[3] or "")
            self.entry_intervalo.setText(str(agendamento[4]) if agendamento[4] is not None else "") 
            self.entry_dias_mes.setText(agendamento[6] or "")
            self.entry_hora_inicio.setText(agendamento[7] or "")
            self.entry_hora_fim.setText(agendamento[8] or "")

            
            self.set_dias_semana(agendamento[5] or "")

            # Define o status no combobox
            index = self.combo_status.findText(agendamento[9])
            if index >= 0:
                self.combo_status.setCurrentIndex(index)
            
            index_etl = self.entry_etl.findText(agendamento[10])
            if index_etl>= 0:
                self.entry_etl.setCurrentIndex(index_etl)
            
            self.btn_salvar.setText("Atualizar Agendamento")
            self.btn_cancelar.setVisible(True)

    def cancelar_edicao(self):
        """Cancela a edição em andamento"""
        self.limpar_campos()
        self.btn_cancelar.setVisible(False)
        self.btn_salvar.setText("Salvar Agendamento")

    def excluir_agendamento(self):
        """Exclui um agendamento selecionado"""
        linha_selecionada = self.tabela.currentRow()
        if linha_selecionada == -1:
            QMessageBox.warning(self, "Seleção", "Por favor, selecione um agendamento para excluir.")
            return

        id_agendamento = int(self.tabela.item(linha_selecionada, 0).text())
        projeto = self.tabela.item(linha_selecionada, 2).text()

        resposta = QMessageBox.question(
            self, "Confirmar Exclusão", 
            f"Tem certeza que deseja excluir o agendamento do projeto '{projeto}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if resposta == QMessageBox.StandardButton.Yes:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM agendamentos WHERE id = ?", (id_agendamento,))
            conn.commit()
            conn.close()

            QMessageBox.information(self, "Sucesso", "Agendamento excluído com sucesso!")
            self.listar_agendamentos()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AgendadorGUI()
    window.show()
    sys.exit(app.exec())