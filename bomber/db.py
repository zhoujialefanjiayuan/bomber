import bottle
import os
from bottle import hook
from mongoengine import connect
from peewee import MySQLDatabase
from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError, MySQLDatabase

app = bottle.default_app()


class BeginPooledMySQL(RetryOperationalError, PooledMySQLDatabase):
    def begin(self):
        # db api 并没有自动加 begin 语句，所以在此要手动加上
        self.get_conn().begin()


class BeginMySQLDatabase(RetryOperationalError, MySQLDatabase):
    def begin(self):
        # db api 并没有自动加 begin 语句，所以在此要手动加上
        self.get_conn().begin()

# 此处 autocommit 仅表示 peewee 是否在每一句后面自动加一句 commit
# timeout 指连接池里没有可用链接时，等待多少秒
# db = BeginPooledMySQL(
#     None,
#     autocommit=False,
#     max_connections=15,
#     timeout=5,
#     stale_timeout=60)

# !!! 连接池的 _in_use 有问题，暂时不使用

db = BeginMySQLDatabase(None, autocommit=False)
readonly_db = BeginMySQLDatabase(None, autocommit=False)
db_auto_call = BeginMySQLDatabase(None, autocommit=False)
readonly_db_auto_call = BeginMySQLDatabase(None, autocommit=False)


def init_mongo_db(_app):
    return connect(
        _app.config['mongodb.database'],
        host=_app.config['mongodb.host'],
        tz_aware=True,
    )


def init():

    init_mongo_db(app)

    app.config.setdefault('db.read_timeout', 20)
    app.config.setdefault('db.write_timeout', 20)
    db.init(
        app.config['db.database'],
        host=app.config['db.host'],
        user=app.config['db.user'],
        port=int(app.config['db.port']),
        charset=app.config['db.charset'],
        password=app.config['db.password'],
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
        read_timeout=int(app.config['db.read_timeout']),
        write_timeout=int(app.config['db.write_timeout']),
    )

    readonly_db_password = os.environ.get('READONLY_DB_PASSWORD') or \
                           app.config['readonly-db.password']
    readonly_db.init(
        app.config['readonly-db.database'],
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        port=int(app.config['readonly-db.port']),
        charset=app.config['readonly-db.charset'],
        password=readonly_db_password,
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )

    db_auto_call.init(
        'auto_call',
        host=app.config['db.host'],
        user=app.config['db.user'],
        port=int(app.config['db.port']),
        charset=app.config['db.charset'],
        password=app.config['db.password'],
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
        read_timeout=int(app.config['db.read_timeout']),
        write_timeout=int(app.config['db.write_timeout']),
    )

    readonly_db_auto_call.init(
        'auto_call',
        host=app.config['readonly-db.host'],
        user=app.config['readonly-db.user'],
        port=int(app.config['readonly-db.port']),
        charset=app.config['readonly-db.charset'],
        password=readonly_db_password,
        autocommit=True,  # 连接mysql 时是否使用autocommit 模式
    )


@hook('before_request')
def _connect_db():
    db.get_conn()
    readonly_db.get_conn()
    db_auto_call.get_conn()
    readonly_db_auto_call.get_conn()


@hook('after_request')
def _close_db():
    if not db.is_closed():
        db.close()
    if not readonly_db.is_closed():
        readonly_db.close()
    if not db_auto_call.is_closed():
        db_auto_call.close()
    if not readonly_db_auto_call.is_closed():
        readonly_db_auto_call.close()
