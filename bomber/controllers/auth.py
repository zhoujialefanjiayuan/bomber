import hashlib
from datetime import datetime, date
import bcrypt
import bottle
import logging
from bottle import post, abort, get, route, request

from bomber.db import db
from bomber.models import Bomber, Session
from bomber.plugins import ip_whitelist_plugin
from bomber.serializers import bomber_role_serializer
from bomber.validator import login_validator, reset_password_validator

app = bottle.default_app()


@post('/api/v1/login', apply=[ip_whitelist_plugin])
def login():
    form = login_validator(request.json)

    bomber = Bomber.filter(Bomber.username == form['username']).first()
    if not bomber:
        abort(403, 'username password do not match')

    password = form['password'].lower()
    if isinstance(password, str):
        password = bytes(password, 'utf-8')

    user_password = bytes(bomber.password, 'utf-8')

    if len(bomber.password) == 32:
        # 检查是不是 MD5 哈希保存的密码
        is_valid = hashlib.md5(password).hexdigest() == bomber.password
        if is_valid:
            # 如果是，转换成 bcrypt 哈希保存
            bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
            bomber.save()
    else:
        is_valid = bcrypt.checkpw(password, user_password)
        if not is_valid:
            is_valid = bcrypt.checkpw(password.lower(), user_password)
            if is_valid:
                bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
                bomber.save()

    if not is_valid:
        logging.info('%s %s login failed', bomber.name, bomber.id)
        abort(403, 'username password do not match')

    logging.info('%s %s login success', bomber.name, bomber.id)

    session = bomber.logged_in(expire_days=7)

    bomber.last_active_at = datetime.now()
    bomber.save()

    return {
        'jwt': session.jwt_token(),
        'permission': bomber_role_serializer.dump(bomber).data
    }


@get('/api/v1/permission')
def users(bomber):
    return bomber_role_serializer.dump(bomber).data


@get('/api/v1/logout')
def logout(bomber):
    bomber.expire_at = datetime.now()
    bomber.save()
    return bomber_role_serializer.dump(bomber).data


@route('/api/v1/reset-password', method='PATCH')
def reset_password(bomber):
    form = reset_password_validator(request.json)

    password = form['old_password'].lower()
    new_password = form['new_password'].lower()

    if isinstance(password, str):
        password = bytes(password, 'utf-8')

    if isinstance(new_password, str):
        new_password = bytes(new_password, 'utf-8')

    user_password = bytes(bomber.password, 'utf-8')

    if len(bomber.password) == 32:
        # 检查是不是 MD5 哈希保存的密码
        is_valid = hashlib.md5(password).hexdigest() == bomber.password
        if is_valid:
            # 如果是，转换成 bcrypt 哈希保存
            bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
            bomber.save()
    else:
        is_valid = bcrypt.checkpw(password, user_password)
        if not is_valid:
            is_valid = bcrypt.checkpw(password.lower(), user_password)
            if is_valid:
                bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
                bomber.save()

    if not is_valid:
        logging.info('%s %s login failed', bomber.name, bomber.id)
        abort(403, 'password is not correct')

    sessions = Session.filter(
        Session.bomber == bomber.id,
        Session.expire_at > date.today(),
    )

    with db.atomic():
        for session in sessions:
            session.expire_at = date.today()
            session.save()
        bomber.password = bcrypt.hashpw(new_password, bcrypt.gensalt())
        bomber.save()
