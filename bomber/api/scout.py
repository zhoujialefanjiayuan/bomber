from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class Scout(BaseServiceAPI):
    def default_token(self):
        return app.config['service.scout.token']

    def get_base_url(self):
        return app.config['service.scout.base_url']
