#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bottle import default_app

from .base import ServiceAPI
from bomber.plugins import PretreatmentPlugin

app = default_app()


class LoginService(ServiceAPI):
    def __init__(self):
        super().__init__()

    def default_token(self):
        return app.config['service.login_service.token']

    def get_base_url(self):
        return app.config['service.login_service.base_url']

    @PretreatmentPlugin('/login-log/get-device', 'GET')
    def login_log_device(self):
        pass
