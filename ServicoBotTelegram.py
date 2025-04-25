import win32serviceutil
import win32service
import win32event
import servicemanager
import asyncio
import sys
import os

class TelegramBotService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TelegramPyflowt3Bot"
    _svc_display_name_ = "Bot Telegram Agendador Pyflowt3"
    _svc_description_ = "Serviço que executa comandos do Telegram para workflows e pipelines pyflowt3."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.loop = None
        self.stop_event = asyncio.Event()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        servicemanager.LogInfoMsg("Sinalizando parada do serviço TelegramBotService.")
        if self.loop and self.stop_event:
            self.loop.call_soon_threadsafe(self.stop_event.set)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("Serviço TelegramBotService iniciado.")
        self.main()

    def main(self):
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            sys.path.insert(0, script_dir)

            from bot_telegram import loop_bot

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(loop_bot(self.stop_event))
            servicemanager.LogInfoMsg("Serviço TelegramBotService encerrado normalmente.")
        except Exception as e:
            servicemanager.LogErrorMsg(f"Erro no serviço TelegramBotService: {str(e)}")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(TelegramBotService)
