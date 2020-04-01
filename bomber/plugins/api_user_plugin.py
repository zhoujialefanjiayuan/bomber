import inspect
from datetime import datetime

import bottle
import peewee
from bottle import request, abort


app = bottle.default_app()


class ApiUserPlugin(object):
    def __init__(self, cls, keyword='api_user'):
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
            token, _ = request.auth or (None, None)
            if token is None:
                abort(401, 'Invalid user')

            try:
                user = self.cls.get(self.cls.token == token)
                if (user.expire_at is not None and
                        datetime.now() > user.expire_at):
                    abort(401, 'Invalid user')
            except peewee.DoesNotExist:
                abort(401, 'Invalid user')
                return

            # Add the connection handle as a keyword argument.
            kwargs[self.keyword] = user

            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper
