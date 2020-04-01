import json
from datetime import datetime

from bottle import get
from peewee import fn

from bomber.constant_mapping import ConnectType, Cycle
from bomber.models import Template, SystemConfig
from bomber.plugins import packing_plugin
from bomber.serializers import template_serializer
from bomber.api import BillService
from bomber.utils import to_datetime
from bomber.models_readonly import ConnectHistoryR

cs_number_conf = {
    'DanaCepat': '02150202889',
    'PinjamUang': '02150201809',
    'KtaKilat': '02150201919',
    'IkiModel': '081291465687',
    'IkiDana':'081291465687'
}


@get('/api/v1/applications/<app_id:int>/templates/sms')
def get_sms_templates(bomber, application):
    # TODO 短信金额计算
    templates = Template.filter(
        Template.type == ConnectType.SMS.value,
        Template.app == application.app,
        Template.cycle <<
        ([bomber.role.cycle] if bomber.role.cycle else Cycle.values()),
    )
    bill_dict = BillService().bill_dict(application_id=application.external_id)
    promise_date = application.promised_date or bill_dict['due_at']
    total_amount = round(application.amount + bill_dict['late_fee'], 2)
    data = {
        'user_name': application.user_name,
        'overdue_days': application.overdue_days,
        'promised_date': promise_date.strftime('%d-%m-%Y'),
        'due_at': bill_dict['due_at'].strftime('%d-%m-%Y'),
        'targe_date': promise_date.strftime('%d-%m-%Y'),
        'black_date': promise_date.strftime('%d-%m-%Y'),
        'unpaid': 'Rp{:,}'.format(round(bill_dict['unpaid'], 2)),
        'amount': 'Rp{:,}'.format(total_amount),
        'phone': application.user_mobile_no,
        'app_name': application.app,
        'cs_number': cs_number_conf.get(application.app, '02150202889'),
    }
    for i in templates:
        i.text = i.text.format(**data)
    return template_serializer.dump(templates, many=True).data


@get('/api/v1/applications/<app_id:int>/templates/call')
def get_call_templates(bomber, application):
    templates = Template.filter(
        Template.type == ConnectType.CALL.value,
        Template.app == application.app,
        Template.cycle <<
        ([bomber.role.cycle] if bomber.role.cycle else Cycle.values()),
    )
    promise_date = application.promised_date or application.due_at
    total_amount = round(application.amount + application.late_fee, 2)
    data = {
        'user_name': application.user_name,
        'overdue_days': application.overdue_days,
        'promised_date': promise_date.strftime('%d-%m-%Y'),
        'unpaid': 'Rp{:,}'.format(round(application.unpaid, 2)),
        'total_amount': 'Rp{:,}'.format(total_amount),
        'phone': application.user_mobile_no,
        'app_name': application.app,
        'cs_number': cs_number_conf.get(application.app, '02150202889'),
    }
    for i in templates:
        i.text = i.text.format(**data)
    return template_serializer.dump(templates, many=True).data


@get('/api/v1/applications/<app_id:int>/templates/pay-method')
def get_pay_method(bomber, application):
    # TODO 金额计算
    templates = Template.filter(
        Template.type == ConnectType.PAY_METHOD.value,
        Template.app == application.app,
    )
    bill_dict = BillService().bill_dict(application_id=application.id)
    promise_date = application.promised_date or bill_dict['due_at']
    total_amount = round(application.amount + bill_dict['late_fee'], 2)
    data = {
        'user_name': application.user_name,
        'overdue_days': application.overdue_days,
        'promised_date': promise_date.strftime('%d-%m-%Y'),
        'unpaid': 'Rp{:,}'.format(round(bill_dict['unpaid'], 2)),
        'total_amount': 'Rp{:,}'.format(total_amount),
        'phone': application.user_mobile_no,
        'app_name': application.app,
        'cs_number': cs_number_conf.get(application.app, '02150202889'),
    }
    for i in templates:
        i.text = i.text.format(**data)
    return template_serializer.dump(templates, many=True).data


@get('/api/v1/applications/<app_id:int>/va', skip=[packing_plugin])
def get_va(bomber, application):
    accounts = BillService().accounts_list(user_id=application.user_id,
                                           is_deprecated=False)['data']
    if accounts:
        for acc in accounts:
            acc['no'] = acc['va']
    return json.dumps({'data': accounts})


@get('/api/v1/templates/va/sms')
def get_all_templates_va_sms():
    templates = (Template.select()
                 .where(Template.type == ConnectType.VA_SMS.value))
    return template_serializer.dump(templates, many=True).data


# 获取当前垂首单还可以返送多少条短信
@get("/api/v1/applications/<app_id>/send/sms/cnt")
def get_application_can_send_sms_cnt(bomber, application):
    result = {"sms": 1, "va": 2}
    keys_map = {"MANUAL_SEND_SMS_LIMIT_CNT": "sms",
                 "MANUAL_SEND_VA_SMS_LIMIT_CNT": "va"}
    sys_config = (SystemConfig.select()
                  .where(SystemConfig.key.in_(list(keys_map.keys()))))
    for sc in sys_config:
        if sc and sc.value.isdigit():
            result[keys_map[sc.key]] = int(sc.value)
    today = datetime.today().date()
    connect_historys = (ConnectHistoryR
                        .select(ConnectHistoryR.type,
                                fn.COUNT(ConnectHistoryR.id).alias("cnt"))
                        .where(ConnectHistoryR.application == application.id,
                               ConnectHistoryR.type << ConnectType.sms(),
                               ConnectHistoryR.created_at >= today)
                        .group_by(ConnectHistoryR.type))
    for ch in connect_historys:
        if ch.type == ConnectType.SMS.value:
            result["sms"] -= ch.cnt
        elif ch.type in ConnectType.va_sms():
            result["va"] -= ch.cnt
    result["sms"] = result["sms"] if result["sms"] > 0 else 0
    result["va"] = result["va"] if result["va"] > 0 else 0
    return result