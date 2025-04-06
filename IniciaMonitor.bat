@echo off
setlocal

:: Obtém o caminho da pasta onde este arquivo .bat está localizado
set "SCRIPT_DIR=%~dp0"

:: Executa o script Python usando o caminho completo
start "" pythonw "%SCRIPT_DIR%Monitor.py"

endlocal
exit