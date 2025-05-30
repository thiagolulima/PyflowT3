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

import sqlite3

DB_PATH = "agendador.db"

def criar_banco():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agendamentos (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             arquivo TEXT NOT NULL,
             projeto TEXT,
             local_run TEXT,
             horario TEXT,
             intervalo INTEGER,
             dias_semana TEXT,
             dias_mes TEXT,
             hora_inicio TEXT,
             hora_fim TEXT,
             status TEXT NOT NULL DEFAULT 'Ativo',
             ferramenta_etl TEXT,
             ultima_execucao DATETIME,
             duracao_execucao REAL,
             timeout_execucao INTEGER DEFAULT 1800
        )
    """)
    conn.commit()
    conn.close()
    print("✅ Banco criado ou já existente.")

def criar_coluna_se_nao_existir(nome_coluna, tipo_coluna, valor_padrao=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(agendamentos)")
    colunas_existentes = [col[1] for col in cursor.fetchall()]

    if nome_coluna not in colunas_existentes:
        comando = f"ALTER TABLE agendamentos ADD COLUMN {nome_coluna} {tipo_coluna}"
        if valor_padrao is not None:
            comando += f" DEFAULT {valor_padrao}"
        cursor.execute(comando)
        conn.commit()
        print(f"🆕 Coluna adicionada: {nome_coluna}")
    else:
        print(f"ℹ️ Coluna já existe: {nome_coluna}")

    conn.close()

def limpar_banco():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM agendamentos")
    conn.commit()
    conn.close()
    print("⚠️ Tabela agendamentos limpa.")

if __name__ == "__main__":

    criar_banco()
    # Criar colunas adicionais se não existirem
    criar_coluna_se_nao_existir("timeout_execucao", "INTEGER", 1800)
    criar_coluna_se_nao_existir("ultima_execucao", "DATETIME")
    criar_coluna_se_nao_existir("duracao_execucao", "REAL")

    # Limpar banco (opcional - cuidado)
    #limpar_banco()
