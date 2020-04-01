#-*- coding:utf-8 -*-

import logging
from bottle import default_app

from .base import ServiceAPI
from bomber.plugins import PretreatmentPlugin

app = default_app()


class AuditService(ServiceAPI):
    def default_token(self):
        return app.config['service.audit_service.token']

    def get_base_url(self):
        return app.config['service.audit_service.base_url']



    @PretreatmentPlugin('/scout/phone_invalid/{cat}', 'GET')
    def _phone_invalid(self, path_params, **params):
        """
        根据手机号获取其他手机号
        :param path_params:
        :param params:
        :return:
        """

    @PretreatmentPlugin('/scout/bomber/get-audit/{cat}', 'GET')
    def _bomber_get_audit(self, path_params, **params):
        """
        bomber获取审核件问题信息
        :param path_params:
        :param params:
        :return:
        """

    def phone_invalid(self, cat, **params):
        try:
            resp = self._phone_invalid(path_params={"cat":cat}, **params)
        except Exception as e:
            logging.error("get phone_invalid error:%s"%str(e))
            return []
        return resp.get("data",{}).get("data",[])

    def bomber_get_audit(self, cat, **params):
        try:
            resp = self._bomber_get_audit(path_params={"cat":cat},**params)
        except Exception as e:
            logging.error("bomber_get_audit error:%s"%str(e))
            resp = []
        return resp




