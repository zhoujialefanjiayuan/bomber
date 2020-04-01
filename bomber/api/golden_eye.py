from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class GoldenEye(BaseServiceAPI):
    def default_token(self):
        return app.config['service.golden_eye.token']

    def get_base_url(self):
        return app.config['service.golden_eye.base_url']
