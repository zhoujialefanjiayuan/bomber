from datetime import datetime

import bottle
import peewee
from bottle import request

from bomber.models import APIUser

app = bottle.default_app()


def get_api_user():
    """
    :rtype: bomber.models.APIUser
    """
    token, _ = request.auth or (None, None)
    if token is None:
        return

    try:
        user = APIUser.get(APIUser.token == token)
        if user.expire_at is not None and datetime.now() > user.expire_at:
            return
        return user
    except peewee.DoesNotExist:
        return


def get_api_user_or_401():
    user = get_api_user()
    if user is None:
        headers = {'WWW-Authenticate': 'Basic realm="bomber"'}
        raise bottle.HTTPError(401, 'Authorization required', **headers)


def check_api_user():
    get_api_user_or_401()
