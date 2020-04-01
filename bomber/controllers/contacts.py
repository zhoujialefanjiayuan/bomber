import logging
from datetime import datetime, timedelta
from bottle import get, post, request, abort
from peewee import JOIN, Param, SQL
from deprecated.sphinx import deprecated

from bomber.api.message_service import MessageService
from bomber.constant_mapping import (
    ContactsUseful,
    CallActionType,
    Relationship,
    ConnectType,
    PhoneStatus,
    SmsChannel,
    Cycle,
    SourceOption)

from bomber.db import db
from bomber.models import (
    ConnectHistory,
    SystemConfig,
    CallActions,
    Template,
    Contact,
    Bomber,
    Partner
)
from bomber.serializers import (
    connect_history_serializer,
    call_history_serializer,
    contact2_serializer,
    contact_serializer,
)
from bomber.utils import number_strip, plain_query
from bomber.controllers.asserts import (
    check_call_priority,
    check_commit_count,
    contact_classify,
    reserved_new,
)
from bomber.validator import (
    contact_call_validator,
    add_contact_validator,
    contact_sms_validator,
    contact_call2_validator,
)


@get('/api/v1/applications/<app_id:int>/contacts/<relationship:int>')
def application_contacts(bomber, application, relationship):
    contacts = Contact.filter(Contact.user_id == application.user_id)
    contacts = contact_classify(contacts, relationship)
    return contact_serializer.dump(contacts, many=True).data


@get('/api/v1/applications/<app_id:int>/contacts2/<relationship:int>')
def application_contacts2(bomber, application, relationship):
    if (application.cycle in (Cycle.C1A.value, Cycle.C1B.value) and
            relationship == Relationship.SUGGESTED.value):
        return []
    # TODO: SQL直接取出
    contacts = (Contact
                .select(Contact, CallActions)
                .join(CallActions,
                      JOIN.LEFT_OUTER,
                      on=((CallActions.contact_id == Contact.id) &
                          (CallActions.type != CallActionType.WHATS_APP.value))
                      .alias('call_action'))
                .where(Contact.user_id == application.user_id)
                .order_by(-CallActions.created_at))
    logging.debug('contacts: %s', contacts)
    contacts = contact_classify(contacts, relationship, bomber, application.id)
    data_list = contact2_serializer.dump(contacts, many=True).data
    return reserved_new(data_list)


@post('/api/v1/applications/<app_id:int>/contacts')
def add_contacts(bomber, application):
    form = add_contact_validator(request.json)
    is_exists = Contact.filter(
        Contact.user_id == application.user_id,
        Contact.name == form['name'],
        Contact.number == form['number'],
    ).exists()
    if is_exists:
        abort(400, 'contact already exists')

    #导包
    need_collection = SourceOption.need_collection()
    source = need_collection.get(form['relationship'],[])
    source = '' if source == [] else source[-1]


    contact = Contact.create(
        application=application.id,
        user_id=application.user_id,
        #添加字段
        source = source,
        name=form['name'],
        number=number_strip(form['number']),
        relationship=form['relationship'],
        sub_relation=form['sub_relation'],
        latest_remark=form['remark'],
        useful=form.get('useful', ContactsUseful.NONE.value)
    )
    return contact_serializer.dump(contact).data


@get('/api/v1/applications/<app_id:int>/connect_history')
def application_connect_history(bomber, application):
    call_query = (
        CallActions
            .select(CallActions,
                    SQL('CASE WHEN t1.type = 2 THEN 2 ELSE 0 END').alias('type'),
                    CallActions.note.alias('remark'),
                    Bomber)
            .join(Bomber, JOIN.INNER, on=(CallActions.bomber_id == Bomber.id)
                  .alias('operator'))
            .where(CallActions.application == application.id)
        .order_by(-CallActions.created_at)
    )

    sms_query = (
        ConnectHistory
            .select()
            .join(Bomber)
            .switch(ConnectHistory)
            .join(Template, join_type=JOIN.LEFT_OUTER)
            .where(ConnectHistory.application == application.id,
                   ConnectHistory.type.in_(ConnectType.sms()))
        .order_by(-ConnectHistory.created_at)
    )

    logging.debug("call query: %s", call_query)
    sms_serializer = (connect_history_serializer
                      .dump(sms_query, many=True).data)
    call_serializer = (call_history_serializer
                       .dump(call_query, many=True).data)
    return sms_serializer + call_serializer


@post('/api/v1/applications/<app_id:int>/contacts/<contact_id:int>/call')
@deprecated(version='1.0', reason='This function will be removed soon')
def add_connect_history_call(bomber, application, contact_id):
    form = contact_call_validator(request.json)
    contact = Contact.filter(
        Contact.user_id == application.user_id,
        Contact.id == contact_id,
    ).first()
    if not contact:
        abort(400, 'unknown contact')

    with db.atomic():
        connect_history = ConnectHistory.create(
            application=application.id,
            type=ConnectType.CALL.value,
            name=contact.name,
            number=contact.number,
            relationship=form['relationship'],
            sub_relation=form['sub_relation'],
            status=form['status'],
            result=form['result'] if 'result' in form else None,
            remark=form['remark'],
            operator=bomber.id,
        )
        contact.relationship = form['relationship']
        contact.latest_status = form['status']
        contact.latest_remark = form['remark']
        contact.sub_relation = form['sub_relation']
        contact.save()

        application.latest_call = bomber.id
        application.save()

    return connect_history_serializer.dump(connect_history).data


@post('/api/v1/applications/<app_id:int>/contacts/<contact_id:int>/call2')
def add_call_action(bomber, application, contact_id):
    form = contact_call2_validator(request.json)
    contact = Contact.filter(
        Contact.user_id == application.user_id,
        Contact.id == contact_id,
    ).first()
    if not contact:
        abort(400, 'unknown contact')

    not_first = check_commit_count(form['type'], form, application, bomber,
                                   contact, form.get('relationship'))
    if not_first:
        abort(400, 'repeated submit')

    with db.atomic():
        sub_relation = form['sub_relation']
        call_record_id = None

        if form.get('type') != CallActionType.WHATS_APP.value:
            item = ConnectHistory.create(
                application=application.id,
                type=ConnectType.CALL.value,
                name=contact.name,
                number=contact.number,
                relationship=form.get('relationship'),
                sub_relation=sub_relation,
                operator=bomber.id,
            )
            call_record_id = item.id

        CallActions.create(
            application=application.id,
            type=form['type'],
            cycle=application.cycle,
            name=contact.name,
            number=contact.number,
            relationship=form.get('relationship'),
            sub_relation=sub_relation,
            bomber_id=bomber.id,
            phone_status=form.get('phone_status'),
            real_relationship=form.get('real_relationship'),
            contact_id=contact.id,
            call_record_id=call_record_id,
            admit_loan=form.get('admit_loan'),
            still_old_job=form.get('still_old_job'),
            new_company=form.get('new_company'),
            overdue_reason=form.get('overdue_reason'),
            overdue_reason_desc=form.get('overdue_reason_desc'),
            pay_willing=form.get('pay_willing'),
            pay_ability=form.get('pay_ability'),
            note=form.get('note'),
            commit=form.get('commit'),
            connect_applicant=form.get('connect_applicant'),
            has_job=form.get('has_job'),
            help_willing=form.get('help_willing'),
            no_help_reason=form.get('no_help_reason'),
            last_connection_to_applicant=(
                form.get('last_connection_to_applicant')),
            helpful=form.get('helpful')
        )

        contact = check_call_priority(contact, form.get('phone_status'),
                                      form.get('real_relationship'),
                                      form.get('commit'))
        # 用real_relationship代替relationship
        contact.real_relationship = form.get('relationship')
        contact.sub_relation = sub_relation
        contact.save()

        application.latest_call = bomber.id
        application.save()


@post('/api/v1/applications/<app_id:int>/contacts/<contact_id:int>/sms')
def add_connect_history_sms(bomber, application, contact_id):
    keys_map = {ConnectType.SMS.value: "MANUAL_SEND_SMS_LIMIT_CNT",
                ConnectType.VA_SMS.value: "MANUAL_SEND_VA_SMS_LIMIT_CNT",
                ConnectType.PAY_METHOD.value: "MANUAL_SEND_VA_SMS_LIMIT_CNT"}
    if (Bomber
            .select()
            .join(Partner, on=Bomber.partner == Partner.id)
            .where(Partner.id << [1, 5],
                   Bomber.id == bomber.id)):
        abort(400, 'Service is not available')
    form = contact_sms_validator(request.json)
    contact = Contact.filter(
        Contact.user_id == application.user_id,
        Contact.id == contact_id,
    ).first()
    if not contact:
        abort(400, 'Unknown contact')
    tpl = Template.filter(Template.id == form['template_id']).first()
    if not tpl:
        abort(400, 'Unknown template')
    # 当天件的个数限制
    today = datetime.today().date()
    five_min_ago = datetime.now() - timedelta(minutes=5)
    send_sms_cnt = 0
    # 5分钟之内不能发送相同内容的短信
    connect_historys = (ConnectHistory.select()
                        .where(ConnectHistory.application == application.id,
                               ConnectHistory.created_at >= today,
                               ConnectHistory.type << ConnectType.sms()))
    for ch in connect_historys:
        if ch.created_at >= five_min_ago and ch.record == form["text"]:
            abort(400, "Repeated SMS")
        if ch.type == tpl.type and tpl.type == ConnectType.SMS.value:
            send_sms_cnt += 1
        elif ch.type in ConnectType.va_sms() and tpl.type in ConnectType.va_sms():
            send_sms_cnt += 1
    # 每个件每天发送短信数量限制
    sys_key = keys_map.get(tpl.type)
    send_sms_limit = 2
    if tpl.type == ConnectType.SMS.value:
        send_sms_limit = 1
    sys_config = (SystemConfig.select()
                  .where(SystemConfig.key == sys_key).first())
    if sys_config and sys_config.value.isdigit():
        send_sms_limit = int(sys_config.value)
    # 获取每天限制
    if send_sms_cnt > send_sms_limit and tpl.type == ConnectType.SMS.value:
        abort(400, "Maximum SMS")
    elif send_sms_cnt > send_sms_limit and tpl.type in ConnectType.va_sms():
        abort(400, "Maximum VA SMS")
    if tpl.type in ConnectType.va_sms():
        # va查看十分钟内是否有接通的催记
        ten_min_ago = datetime.now() - timedelta(minutes=10)
        call_actions = (CallActions.select()
                        .where(CallActions.application == application.id,
                               CallActions.number == number_strip(contact.number),
                               CallActions.created_at >= ten_min_ago,
                               CallActions.phone_status == PhoneStatus.CONNECTED.value))
        if not call_actions.exists():
            abort(400, "No connected Call")

    # dict 中 必须都可以 json dumps
    req_data = {
          "app_name": 'Pinjaman Juanusa' if application.app =='IkiDana' else 'Rupiah Tercepat',
          "content": form['text'],
          "failed_retry": True,
          "is_masking": True,
          "message_level": 0,
          "message_type": "SMS",
          "receiver": '62' + number_strip(contact.number),
          "sms_type": 99,
          "title": ""
        }
    result = MessageService().send_single(**req_data)
    if not result.get("result"):
        abort(500, "Send SMS failed")

    connect_history = ConnectHistory.create(
        application=application.id,
        type=tpl.type,
        name=contact.name,
        number=contact.number,
        relationship=contact.relationship,
        status=contact.latest_status,
        template=form['template_id'] if 'template_id' in form else None,
        operator=bomber.id,
        record=form['text'],
    )
    
    return connect_history_serializer.dump(connect_history).data
