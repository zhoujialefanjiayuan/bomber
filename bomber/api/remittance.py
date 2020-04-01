from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class Remittance(BaseServiceAPI):

    def default_token(self):
        return app.config['service.remittance.token']

    def get_base_url(self):
        return app.config['service.remittance.base_url']
