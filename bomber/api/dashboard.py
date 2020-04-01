from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class Dashboard(BaseServiceAPI):
    def default_token(self):
        return app.config['service.dashboard.token']

    def get_base_url(self):
        return app.config['service.dashboard.base_url']
