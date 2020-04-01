from bottle import default_app

from bomber.constant_mapping import AppName
from .base import BaseServiceAPI


app = default_app()


class Message(BaseServiceAPI):

    def __init__(self, token=None, version=None):
        super().__init__(token)
        self.version = version

    def default_token(self):
        return app.config['service.message.token']

    def get_base_url(self):
        # if self.version == AppName.DANAMALL.value:
        #     return app.config['service.message.danamall_base_url']
        return app.config['service.message.base_url']
