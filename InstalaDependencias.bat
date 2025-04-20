@echo off
setlocal

:: Obtém o caminho da pasta onde este arquivo .bat está localizado
set "SCRIPT_DIR=%~dp0"

:: Executa o script Python usando o caminho completo
start "" python -m pip install -r "%SCRIPT_DIR%requirements.txt"

endlocal
exit