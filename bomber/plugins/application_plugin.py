import inspect

from bottle import abort

from bomber.models import Application


class ApplicationPlugin(object):
    def __init__(self, keyword='application'):
        self.keyword = keyword

    def apply(self, callback, route):
        _callback = route['callback']

        # Test if the original callback accepts a 'admin' keyword.
        # Ignore it if it does not need a database handle.
        argspec = inspect.signature(_callback)
        if self.keyword not in argspec.parameters:
            return callback

        def wrapper(*args, **kwargs):
            bomber = kwargs.get('bomber')
            if not bomber:
                abort(403, 'permission denied')

            application = Application.filter(
                Application.id == kwargs.pop('app_id')
            ).first()

            if not application:
                abort(400, 'application not found')

            kwargs['application'] = application

            # admin的cycle为0可以查看所有件,还能操作任何未领取的件
            if bomber.role.cycle == 0:
                return callback(*args, **kwargs)

            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper
