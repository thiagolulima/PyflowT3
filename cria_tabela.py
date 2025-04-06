import tkinter as tk
from tkinter import filedialog
import sqlite3


conn = sqlite3.connect("agendador.db")
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
conn.commit()
conn.close()