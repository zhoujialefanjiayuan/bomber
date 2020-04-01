import json

import jwt
import inspect
from datetime import datetime

import bottle
import logging
from bottle import request, abort

from bomber.models import Session, Bomber, Role

app = bottle.default_app()


def get_jwt_user():
    secret = app.config['user.secret']
    algorithms = app.config['user.algorithms'].strip(',').split(',')
    try:
        # Authorization: Bearer <token>
        token = request.headers['Authorization'][7:]
        return jwt.decode(token, secret, algorithms=algorithms)
    except Exception:
        return None


class UserPlugin(object):
    def __init__(self, cls, keyword='bomber'):
        self.cls = bottle.load(cls)
        self.keyword = keyword

    def apply(self, callback, route):
        _callback = route['callback']

        # Test if the original callback accepts a 'admin' keyword.
        # Ignore it if it does not need a database handle.
        argspec = inspect.signature(_callback)
        if self.keyword not in argspec.parameters:
            return callback

        def wrapper(*args, **kwargs):
            jwt_user = get_jwt_user()
            if jwt_user is None:
                abort(401, 'Invalid user')

            session = Session.select(
                Session, Bomber, Role
            ).join(Bomber).join(Role).where(
                Session.id == jwt_user.get('session_id'),
            )
            if not session.exists():
                abort(401, 'Invalid user')

            session = session.get()

            if datetime.now() > session.expire_at:
                logging.info('bomber %s expire', session.bomber_id)
                abort(401, 'Account Expired')

            # permission
            callback_permission = getattr(callback, 'permission', None)
            admin_permission = session.bomber.role.permission
            if not admin_permission:
                abort(403, 'Permission Denied')
            if (callback_permission and callback_permission not in
                    json.loads(admin_permission)):
                abort(403, 'Permission Denied')

            # Add the connection handle as a keyword argument.
            kwargs[self.keyword] = session.bomber

            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper
