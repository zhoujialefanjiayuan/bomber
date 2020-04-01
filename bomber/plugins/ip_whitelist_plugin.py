import logging

from bottle import abort

from bomber.models import IpWhitelist
from bomber.utils import request_ip


def ip_whitelist_plugin(callback):
    def wrapper(*args, **kwargs):
        whitelist = list(IpWhitelist.select())
        ip = request_ip()
        if whitelist and ip not in [w.ip for w in whitelist]:
            logging.info('bomber login invalid ip: %s', ip)
            return abort(403, 'Invalid IP')

        return callback(*args, **kwargs)
    return wrapper
