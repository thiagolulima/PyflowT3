# PyFlowT3

**PyFlowT3** é um agendador de workflows desenvolvido em Python para execução de fluxos do **Pentaho** e **Apache Hop**.  
Ele permite o agendamento, execução e monitoramento de workflows e pipelines de forma prática com interface gráfica e suporte a serviços no Windows.

---

## 📄 Licença

Este projeto está licenciado sob a **Apache License 2.0**.  
Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.

© 2025 Thiago Luis de Lima

---

## ⚙️ Instalação

### ✅ Pré-requisitos

- Python 3.8 instalado  
- Python configurado nas variáveis de ambiente do sistema

### 📦 Instale as dependências

Execute no terminal:

```bash
pip install PyQt6 pywin32 python-dotenv

python-dotenv: Manipulação de arquivos .env com variáveis de ambiente

## 📝 Configuração do .env

        # Configurações do banco de dados
        DB_PATH=agendador.db

        # Caminho para o executável hop-run do Apache Hop
        APACHE_HOP="C:\Apache-hop\hop-run.bat"

        # Caminho para os executáveis do Pentaho (Kitchen e Pan)
        PENTAHO_JOB="C:\data-integration\Kitchen.bat"
        PENTAHO_TRANSFORMATION="C:\data-integration\Pan.bat"    

