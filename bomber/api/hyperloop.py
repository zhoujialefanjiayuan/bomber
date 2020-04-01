from bottle import default_app
from .base import BaseServiceAPI

app = default_app()


class Hyperloop(BaseServiceAPI):

    def default_token(self):
        return app.config['service.hyperloop.token']

    def get_base_url(self):
        return app.config['service.hyperloop.base_url']
