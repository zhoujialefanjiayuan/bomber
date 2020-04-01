from bottle import default_app
from .base import BaseServiceAPI


app = default_app()


class Battlefront(BaseServiceAPI):
    def default_token(self):
        return app.config['service.battlefront.token']

    def get_base_url(self):
        return app.config['service.battlefront.base_url']
