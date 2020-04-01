import os
import bottle
import logging

import sys

from bomber import db
from bomber.plugins import packing_plugin
from bomber.plugins import UserPlugin

from bomber.plugins import ApiUserPlugin, ip_whitelist_plugin
from bomber.plugins.application_plugin import ApplicationPlugin
from bomber.plugins.packing_plugin import logging_plugin
from bomber.utils import env_detect
from bomber.error import register_error_handler
from logging.handlers import TimedRotatingFileHandler

os.chdir(os.path.dirname(__file__))
app = application = bottle.default_app()
bottle.BaseRequest.MEMFILE_MAX = 10 * 1024 * 1024


def load_config():
    app.config.load_config('config/base.ini')

    cur_env = env_detect().lower()

    if os.path.exists('config/%s.ini' % cur_env):
        app.config.load_config('config/%s.ini' % cur_env)

    # load config map in k8s
    config_map_path = os.environ.get('APP_CONFIG_MAP_PATH')
    secret_path = os.environ.get('APP_SECRET_PATH')
    for path in (config_map_path, secret_path):
        if not (path and os.path.exists(path)):
            continue

        config_map = {}
        for entry in os.scandir(path):
            if entry.is_file() and not entry.name.startswith('.'):
                with open(entry.path, encoding='utf-8') as f:
                    config_map[entry.name] = f.read().strip()

        if config_map:
            app.config.update(**config_map)


def load_controllers():
    for c in os.listdir('controllers'):
        head, _ = os.path.splitext(c)
        if not head.startswith('_') and not head.startswith('.'):
            __import__('bomber.controllers.' + head)


def set_logger():
    logger_file = os.environ.get('LOGGER_FILE')
    if not logger_file:
        logger_file = 'access.log'
    default_format = ('[%(asctime)s] [%(levelname)s] '
                      '[%(module)s: %(lineno)d] %(message)s')
    logging_level = app.config.get('logging.level', 'ERROR')
    filepath = app.config.get('logging.filename', os.path.expandvars('$HOME'))
    filename = filepath + '/' + logger_file
    logging.basicConfig(
        level=getattr(logging, logging_level.upper()),
        format=default_format,
        datefmt='%Y-%m-%d %H:%M:%S %z'
    )
    ch = TimedRotatingFileHandler(filename, when='D', encoding="utf-8")
    ch.setFormatter(default_format)
    logging.root.addHandler(ch)


def install_plugins():
    app.install(packing_plugin)
    app.install(UserPlugin('bomber.models:Bomber'))
    app.install(ApiUserPlugin('bomber.models:APIUser'))
    app.install(ApplicationPlugin())
    app.install(logging_plugin)
    app.install(ip_whitelist_plugin)


def base_config():
    load_config()
    set_logger()
    db.init()


def init_app():
    base_config()

    load_controllers()
    install_plugins()

    register_error_handler()

    return app
