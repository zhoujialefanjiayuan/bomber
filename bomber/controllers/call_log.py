import logging
from peewee import fn
from bomber.models import CallLog, Bomber
from bomber.db import db
from datetime import datetime
from bottle import get, post, request
from bomber.plugins import page_plugin
from bomber.serializers import call_log_serializer


def state_msg(state='succeed', msg=''):
    return {
        'state': state,
        'msg': msg
    }


@get('/api/v1/call-logs/max_call_id')
def max_call_id():
    query = (CallLog
             .select(fn.MAX(CallLog.record_id).alias('max_record_id'))
             .first())
    # 第一次返回0
    return query.max_record_id or 0


@get('/api/v1/call-logs', apply=[page_plugin])
def show_call_logs():
    args = request.query

    call_logs = CallLog.select()

    if 'bomber_id' in args:
        call_logs = call_logs.where(CallLog.user_id == args.bomber_id)

    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d %H:%M')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d %H:%M')
        call_logs = call_logs.where(CallLog.time_start >= start_date,
                                    CallLog.time_start <= end_date)

    call_logs = call_logs.order_by(-CallLog.time_start)
    return call_logs, call_log_serializer


@post('/api/v1/call-logs')
def insert_call_log():
    call_logs = request.json
    if not call_logs:
        return state_msg(state='failed', msg='Request body is empty')

    with db.atomic():
        try:
            CallLog.insert_many(call_logs).execute()
            return state_msg(msg='Insert many succeed')
        except Exception as e:
            return state_msg(state='failed',
                             msg='Insert many failed Caused by %s' % e)
