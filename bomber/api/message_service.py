#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bottle import default_app

from .base import ServiceAPI
from bomber.plugins import PretreatmentPlugin

app = default_app()


class MessageService(ServiceAPI):
    def __init__(self):
        super().__init__()

    def default_token(self):
        return app.config['service.message_service.token']

    def get_base_url(self):
        return app.config['service.message_service.base_url']

    @PretreatmentPlugin('/fcm/fetch-token', 'POST')
    def token_list(self, **params):
        pass

    @PretreatmentPlugin('/msg/send/batch', 'POST')
    def send_batch(self, **params):
        """
        批量发送
        {
          "app_name": "string",
          "failed_retry": true, #是否重试
          "is_masking": true, #是否掩码短信
          "list": [
            {
              "content": "content",
              "receiver": 86888888123,
              "title": "title" # sms消息字段值为空
            }
          ],
          "message_level": 0, #消息等级，0:low,1:normal,2high
          "message_type": "FCM", # 消息类型 SMS,FCM
          "sms_type": 1 #短信类型0-非短信;
                                1-验证码短信;
                                2-打款还款提醒信;
                                3-批量短信;
                                4-批量模板短信;
                                5-单条模板短信
                                99-其他短信
        }
        :param params:
        :return:
        """
        pass

    @PretreatmentPlugin('/msg/send/batch_template', 'POST')
    def send_batch_template(self, **params):
        """
        批量发送模板短信
        {
          "app_name": "KtaKilat",
          "failed_retry": true,
          "is_masking": true,
          "list": [
            {
              "data_map": {},
              "receiver": 86888888123
            }
          ],
          "message_level": 0,
          "message_type": "FCM",
          "sms_type": 1,
          "type_id": 1 # 消息模板id
        }

        :param params:
        :return:
        """
        pass

    @PretreatmentPlugin('/msg/send/single', 'POST')
    def send_single(self, **params):
        """

        {
          "app_name": "string",
          "content": "content",
          "failed_retry": true,
          "is_masking": true,
          "message_level": 0,
          "message_type": "FCM",
          "receiver": 86888888123,
          "sms_type": 99,
          "title": "title"
        }
        :param params:
        :return:
        """
        pass

    @PretreatmentPlugin('/msg/send/single_template', 'POST')
    def send_single_template(self, **params):
        """
        发送单挑模板消息
        {
          "app_name": "string",
          "data_map": {},
          "failed_retry": true,
          "id": 1,
          "is_masking": true,
          "message_level": 0,
          "message_type": "FCM",
          "receiver": 86888888123,
          "sms_type": 1,
          "type_id": 1
        }
        :param params:
        :return:
        """
        pass

