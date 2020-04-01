import pytest
from datetime import datetime, timedelta
from ..helper import TestApp, drop_and_create_database

from bomber import models
from bomber.models import (
    ModelBase,
    Session,
    get_ordered_models
)
from bomber.app import init_app


@pytest.fixture(scope='session')
def app():
    return TestApp(init_app())


@pytest.fixture(autouse=True, scope='session')
def db(app):
    """初始化数据库，只会执行一次"""
    drop_and_create_database(app.app.config)

    # 重建表
    ordered_models = get_ordered_models(models)

    for model in ordered_models:
        if model != ModelBase:
            model.create_table()


@pytest.fixture
def session(user, request):
    _session = Session.create(
        user=user,
        ip='1.1.1.1',
        device_name='test device',
        device_no='test device id',
        expire_at=datetime.now() + timedelta(days=8))

    def teardown():
        _session.delete_instance()

    request.addfinalizer(teardown)
    return _session


@pytest.fixture(scope='module')
def ua_header():
    return {
        'User-Agent': 'Android/6.0.1;Xiaomi/Redmi 4A;DanaCepat/1.2.1;'
                      'Device/862115035974347;Android_ID/c58a6c5eb6742de3',
    }
