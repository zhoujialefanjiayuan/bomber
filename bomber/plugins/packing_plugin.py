import logging
import time
import traceback

from bottle import request, default_app

from bomber.models import MonLogger

app = application = default_app()


def packing_plugin(callback):
    def wrapper(*args, **kwargs):
        body = callback(*args, **kwargs)
        return {
            'data': body,
        }
    return wrapper


def logging_plugin(callback):
    def wrapper(*args, **kwargs):

        r_url = request.url

        content_type = request.headers.get('CONTENT_TYPE')
        if content_type == 'image/jpeg':
            params = 'image/jpeg'
        else:
            params = request.body.read()
        func_name = callback.__name__
        start = time.time()
        status = 'success'
        body = None
        try:
            body = callback(*args, **kwargs)
        except Exception as e:
            logging.error(traceback.format_exc())
            body = type(e)
            status = 'error'
            raise e
        finally:
            end = time.time()
            time_diff = round((end - start), 3)
            logging_level = app.config.get('logging.level')
            level = getattr(logging, logging_level.upper())
            if status == 'success':
                logging.info(
                    'request details: '
                    'func_name:%s, status:%s, time_diff:%ss, params:%s, '
                    'args:%s, kwargs: %s, body: %s',
                    func_name, status, time_diff, params,
                    args, kwargs, body
                )
                if int(level) <= logging.INFO:
                    mon_log = {
                        'r_url': r_url,
                        'func_name': func_name,
                        'status': status,
                        'time_diff': time_diff,
                        'params': str(params),
                        'args': str(args),
                        'kwargs': str(kwargs),
                        'body': str(body)
                    }
                    MonLogger(**mon_log).save()
            else:
                logging.error(
                    'request details: '
                    'func_name:%s, status:%s, time_diff:%ss, params:%s, '
                    'args:%s, kwargs: %s, body: %s',
                    func_name, status, time_diff, params,
                    args, kwargs, body
                )
                if int(level) <= logging.ERROR:
                    mon_log = {
                        'r_url': r_url,
                        'func_name': func_name,
                        'status': status,
                        'time_diff': time_diff,
                        'params': str(params),
                        'args': str(args),
                        'kwargs': str(kwargs),
                        'body': str(body)
                    }
                    MonLogger(**mon_log).save()
        return body
    return wrapper
