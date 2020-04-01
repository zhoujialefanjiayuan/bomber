import bottle
from bottle import get

from bomber.plugins import ip_whitelist_plugin

app = bottle.default_app()


@get('/', skip=[ip_whitelist_plugin])
def index():
    return {'new': True}
