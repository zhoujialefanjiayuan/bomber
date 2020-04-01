from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class Repayment(BaseServiceAPI):
    def default_token(self):
        return app.config['service.repayment.token']

    def get_base_url(self):
        return app.config['service.repayment.base_url']
