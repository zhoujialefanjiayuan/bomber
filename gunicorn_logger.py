import gunicorn.glogging
import time


class Logger(gunicorn.glogging.Logger):
    def now(self):
        return time.strftime('%Y-%m-%d %H:%M:%S %z')
