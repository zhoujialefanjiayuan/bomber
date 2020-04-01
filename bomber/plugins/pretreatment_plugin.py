#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from functools import wraps
import json

from bottle import abort


class PretreatmentPlugin(object):
    def __init__(self, url, method, query=None):
        self.url = url
        self.method = method
        self.query = query

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            service = args[0]
            method_lower = self.method.lower()
            method = getattr(service, method_lower)
            logging.debug('request url {}, params {}'.format(self.url, kwargs))

            url = self.url
            if 'path_params' in kwargs:
                url = self.url.format(**kwargs['path_params'])
                kwargs.pop("path_params")
            if method_lower == 'post':
                resp = method(url, json=kwargs)
            else:
                resp = method(url, kwargs)
            if resp.status_code == 502:
                abort(502, 'Bad Gateway')
            try:
                resp_json = resp.json()
            except json.JSONDecodeError:
                logging.error("parsing error, request content: {}"
                              .format(resp.content))
                abort(resp.status_code)
                return

            if not resp.ok:
                if 'code' in resp_json and 'message' in resp_json:
                    logging.exception('request has exception code: {} msg: {}'
                                      .format(resp_json['code'],
                                              resp_json.get('chineseMessage',"")))
                    abort(resp.status_code, resp_json['message'])
                else:
                    abort(resp.status_code)
            return resp_json

        return wrapper
