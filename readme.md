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

```
## 📝 Configure o arquivo .env

        # Configurações do banco de dados
        DB_PATH=agendador.db

        # Caminho para o executável hop-run do Apache Hop
        APACHE_HOP="C:\Apache-hop\hop-run.bat"

        # Caminho para os executáveis do Pentaho (Kitchen e Pan)
        PENTAHO_JOB="C:\data-integration\Kitchen.bat"
        PENTAHO_TRANSFORMATION="C:\data-integration\Pan.bat"    

## 🧩 Instalação do Serviço (Windows)

        python ServicoAgendadorWindows.py install

## Comandos adicionais:
    ## Remover serviço: 
        python ServicoAgendadorWindows.py remove
    ## Parar serviço:
        python ServicoAgendadorWindows.py stop


## 🚀 Iniciar o Agendador
Você pode iniciar o agendador de duas formas:

* Executando o arquivo iniciaAgendador.bat
* Criando um atalho chamado "Agendador Workflows PyFlowT3" na área de trabalho apontando para esse .bat
(O ícone está na pasta do projeto)

Nesta tela você poderá:
    * Adicionar novos workflows ou pipelines
    * Editar agendas existentes
    * Forçar execuções manuais

## 📊 Monitoramento

Para monitorar as execuções:

* Execute o arquivo iniciaMonitor.bat
* Ou crie um atalho chamado "Monitoramento PyFlowT3" na área de trabalho

Na tela de monitoramento você verá:

* Agendas ativas
* Logs de execução por dia
* Pesquisa e atualização de logs

## 📁 Logs

* Os logs são salvos na pasta logs, com um arquivo por dia
* Verifique as permissões de escrita nessa pasta para garantir o funcionamento adequado
