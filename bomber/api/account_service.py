#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import logging
import traceback

from bottle import default_app

from .base import ServiceAPI
from .login_service import LoginService
from .golden_eye import GoldenEye
from bomber.plugins import PretreatmentPlugin

app = default_app()


class AccountService(ServiceAPI):
    def default_token(self):
        return app.config['service.account_service.token']

    def get_base_url(self):
        return app.config['service.account_service.base_url']

    @PretreatmentPlugin('/userAccount/users/{user_id}', 'GET')
    def get_user(self, path_params, **params):
        """
        /api/v1/users/<user_id>

        :param path_params:
        :param params:
        :return:
        """
        pass

    @PretreatmentPlugin('/onlineProfile/phone/account/{user_id}', 'GET')
    def _add_contact(self, path_params, **params):
        """
        /api/v1/bomber/<user_id>/add_contact

        :param path_params:
        :param params:
        :return:
        """

    def add_contact(self, user_id):
        try:
            contact_list = self._add_contact(path_params={'user_id': user_id})
            account = contact_list['account']
            if isinstance(account, list):
                return account
            else:
                return [account]
        except:
            logging.error("add contact: %s", traceback.format_exc())
            return []

    @PretreatmentPlugin('/identity/device/number', 'POST')
    def _other_login_devices(self, **params):
        pass

    def other_login_contact(self, **params):
        """
        /api/v1/bomber/<user_id>/other_login

        :return:
        """
        other_log_devices = LoginService().login_log_device(**params)['data']
        logging.debug("other log devices: %s", other_log_devices)
        if not other_log_devices:
            return {}

        devices = self._other_login_devices(device_nos=list(
            set(other_log_devices)))
        logging.debug("devices: %s", devices)

        if len(devices) < 2:
            logging.error("At least one device")
            return {}

        try:
            other = GoldenEye().get(
                '/bomber/%s/other_login' % json.dumps(list(set(devices)))
            )
            resp_json = other.json()
            if 'data' in resp_json:
                return resp_json['data']
        except:
            logging.error("Some error when get other login devices")
        return {}

    @PretreatmentPlugin('/identity/{user_id}/ktpNumber', 'GET')
    def ktp_number(self, path_params, **params):
        """
        /api/v1/bomber/<user_id:int>ktp_number

        :param path_params:
        :param params:
        :return:
        """
        pass
