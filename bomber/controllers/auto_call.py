import csv
import hashlib
from datetime import datetime, date
from io import StringIO
import logging

import bcrypt
from bottle import get, post, request, abort, response
from peewee import fn, Param, JOIN_LEFT_OUTER

from bomber.api import BillService
from bomber.constant_mapping import (
    ApplicationStatus,
    BomberCallSwitch,
    RealRelationship,
    AutoCallResult,
    AutoListStatus,
    CallActionType,
    SpecialBomber,
    ContactStatus,
    Relationship,
    PhoneStatus,
    Cycle,
)
from bomber.db import db
from bomber.models_readonly import RepaymentLogR, BomberPtpR
from bomber.models import (
    AutoCallActionsPopup,
    AutoCallListHistory,
    AutoCallActions,
    AutoIVRActions,
    AutoIVRStatus,
    IVRCallStatus,
    AutoCallList,
    RepaymentLog,
    SystemConfig,
    Application,
    CallActions,
    Summary2,
    Contact,
    AutoIVR,
    Bomber,
    Notes,
    Role,
)
from bomber.plugins import packing_plugin, ip_whitelist_plugin
from bomber.plugins.application_plugin import ApplicationPlugin
from bomber.serializers import (
    auto_call_actions_serializer,
    repayment_log_serializer,
    contact_serializer,
)
from bomber.utils import plain_query, plain_forms
from bomber.validator import (
    add_auto_call_popup_action_validator,
    add_auto_call_action2_validator,
    add_auto_call_action_validator,
    auto_call_no_answer_validator,
    auto_call_phones_validator,
    login_validator,
)
from bomber.controllers.asserts import (
    set_ptp_for_special_bomber,
    post_actions_report_error,
    check_call_priority,
    auto_call_follow_up,
    auto_call_set_phone,
    mail_box_set_phone,
    auto_call_continue,
    check_commit_count,
    auto_call_result,
    bomber_contact,
    auto_call_ptp,
)


@post('/api/v1/auto-call/login')
def login():
    form = login_validator(plain_forms())
    bomber = Bomber.filter(Bomber.username == form['username']).first()

    if not bomber:
        abort(403, 'username password do not match')

    password = form['password'].lower()
    if isinstance(password, str):
        password = bytes(password, 'utf-8')

    user_password = bytes(bomber.password, 'utf-8')

    if len(bomber.password) == 32:
        # 检查是不是 MD5 哈希保存的密码
        is_valid = hashlib.md5(password).hexdigest() == bomber.password
        if is_valid:
            # 如果是，转换成 bcrypt 哈希保存
            bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
            bomber.save()
    else:
        is_valid = bcrypt.checkpw(password, user_password)
        if not is_valid:
            is_valid = bcrypt.checkpw(password.lower(), user_password)
            if is_valid:
                bomber.password = bcrypt.hashpw(password, bcrypt.gensalt())
                bomber.save()

    if not is_valid:
        logging.info('%s %s login failed', bomber.name, bomber.id)
        abort(403, 'username password do not match')

    logging.info('%s %s login success', bomber.name, bomber.id)

    session = bomber.logged_in(expire_days=7)

    bomber.last_active_at = datetime.now()
    bomber.save()

    return {
        'status': 'success',
        'group_number': str(bomber.role.cycle),
        'token': session.jwt_token(),
    }


@get('/api/v1/auto-call/applications/<app_id:int>/contacts',
     skip=[ApplicationPlugin])
def get_contact(bomber, app_id):
    form = plain_query()

    application = Application.filter(Application.id == app_id).first()
    if not application:
        abort(404, 'application not found')

    contacts = Contact.filter(Contact.user_id == application.user_id)

    if 'mobile_no' in form:
        contacts = contacts.where(Contact.number == form['mobile_no'])

    return contact_serializer.dump(contacts, many=True).data


@get('/api/api/v1/auto-call/phones',
     skip=[packing_plugin, ip_whitelist_plugin])
def get_auto_call_phones_ivr():
    dpd_group = AutoIVR.dpd_groups()
    max_ivr_call_time = 15
    sys_config = (SystemConfig.select()
                  .where(SystemConfig.key == 'MAX_IVE_CALL_TIME')
                  .first())
    if sys_config and sys_config.value.isdigit():
        max_ivr_call_time = int(sys_config.value)
    offset = -1
    is_work_time = get_now_is_work_time()
    while offset < 50:
        # 尝试50次，避免无限循环
        offset += 1
        _item = None
        if not is_work_time:
            _item = (AutoIVR
                     .select()
                     .where(AutoIVR.status == AutoIVRStatus.AVAILABLE.value,
                            AutoIVR.call_time < max_ivr_call_time,
                            AutoIVR.group.in_(dpd_group))
                     .order_by(AutoIVR.call_time,
                               AutoIVR.called,
                               AutoIVR.group)
                     .offset(offset)
                     .limit(1)
                     .first())
        if not _item:
            _item = (AutoIVR
                     .select()
                     .where(AutoIVR.status == AutoIVRStatus.AVAILABLE.value,
                            AutoIVR.call_time < max_ivr_call_time,
                            AutoIVR.group.not_in(dpd_group))
                     .order_by(AutoIVR.call_time,
                               AutoIVR.called,
                               AutoIVR.group)
                     .offset(offset)
                     .limit(1)
                     .first())
        if not _item:
            break

        with db.atomic():
            item = (AutoIVR
                    .select()
                    .where(AutoIVR.id == _item.id,
                           AutoIVR.status == AutoIVRStatus.AVAILABLE.value)
                    .for_update()
                    .first())
            if not item:
                continue

            q = (AutoIVR
                 .update(status=AutoIVRStatus.PROCESSING.value,
                         call_time=AutoIVR.call_time + 1)
                 .where(AutoIVR.id == item.id,
                        AutoIVR.status == AutoIVRStatus.AVAILABLE.value))
            q.execute()
            logging.info("customer_number: %s call" %
                         str(item.application_id))
            return {
                'status': 1,
                'customer_number': str(item.user_id),
                'phone': item.numbers,
                'loanid': item.application_id,
                'group': item.group
            }

    return {
        'status': 0
    }

# 判断当前时间是否是工作时间
def get_now_is_work_time():
    t1_str = '08:30:00'
    t2_str = '09:00:00'
    t3_str = '19:30:00'
    t4_str = '21:00:00'
    now_time = datetime.now().time()
    t1 = datetime.strptime(t1_str, '%H:%M:%S')
    t2 = datetime.strptime(t2_str, '%H:%M:%S')
    t3 = datetime.strptime(t3_str, '%H:%M:%S')
    t4 = datetime.strptime(t4_str, '%H:%M:%S')
    if t1.time() <= now_time <= t2.time():
        return False
    if t3.time() <= now_time <= t4.time():
        return False
    return True


@post('/api/ivr/callback', skip=[ip_whitelist_plugin])
def auto_call_ivr_callback():
    """
    记录ivr系统回调结果，回调结果有成功和失败两种；成功callstate为1,
    失败callstate为0，因为是高并发作业所以需要考虑异常情况
    """
    form = plain_forms()
    try:
        loanid = int(form.get('loanid'))
        state = int(form.get('callstate'))
        group = int(form.get('group'))
        customer_number = int(form.get('customer_number'))
        logging.info("ivr call back params: %s", dict(form))
    except (ValueError, TypeError):
        abort(404, 'Invalid loan id or call state or group or customer_number')
    # state: 0表时未打通，1表示打通
    if state == 0:
        q = (
            AutoIVR
            .update(status=AutoIVRStatus.AVAILABLE.value)
            .where(AutoIVR.application_id == loanid,
                   AutoIVR.group == group,
                   AutoIVR.user_id == customer_number,
                   AutoIVR.status == AutoIVRStatus.PROCESSING.value
                   )).execute()
        if q == 0:
            # 分三种情况
            # 1: 一条数据回调成功后回调失败, 此时auto_ivr.status为3  # BUG
            # 2: 一条数据回调两次失败  # BUG
            # 3: 回调时用户已经还款, 此时auto_ivr.status为2  # 正常
            form['callstate'] = IVRCallStatus.FAILED.value
        elif q == 1:
            # 呼叫失败  # 正常
            form['callstate'] = IVRCallStatus.FAILED.value
    elif state == 1:
        q = (
            AutoIVR
            .update(called=AutoIVR.called + 1,
                    status=AutoIVRStatus.SUCCESS.value)
            .where(AutoIVR.application_id == loanid,
                   AutoIVR.group == group,
                   AutoIVR.user_id == customer_number,
                   AutoIVR.status << [AutoIVRStatus.PROCESSING.value,
                                      AutoIVRStatus.AVAILABLE.value]
                   )).execute()
        if q == 0:
            # 分两种情况
            # 1: 一条数据两次回调成功  # BUG
            # 2: 回调时用户已经还款，此时auto_ivr.status为2  # 正常
            form['callstate'] = IVRCallStatus.CALLBACKSUCCESSEXCEPTION.value
        elif q == 1:
            # 呼叫成功  # 正常
            form['callstate'] = IVRCallStatus.SUCCESS.value
    try:
        AutoIVRActions.create(**form)
    except Exception as err:
        logging.warning('ivr auto call %s failed: %s', loanid, err)


@get('/api/v1/auto-call/phones', skip=[packing_plugin, ip_whitelist_plugin])
def get_auto_call_phones():
    """
    select for update时如果记录已经被更新，给下一次select增加offset
    TODO: 使用队列优化

    :return:
    """
    form = auto_call_phones_validator(plain_query())
    cycle = form['group_number']
    offset = -1
    while offset < 50:
        offset += 1

        # 对于新进的件若没打通一天拨打三次(催收员下午上海时间两点上班，晚上八点下班)
        _item = None
        if 14 <= datetime.now().hour < 18:
            _item = (
                AutoCallList
                    .select()
                    .where(AutoCallList.cycle == cycle,
                           AutoCallList.called_counts < 1,
                           AutoCallList.numbers != '',
                           AutoCallList.status == AutoListStatus.PENDING.value)
                    .order_by(AutoCallList.called_counts, AutoCallList.id)
                    .offset(offset)
                    .limit(1)
                    .first())

        elif datetime.now().hour >= 18:
            _item = (
                AutoCallList
                    .select()
                    .where(AutoCallList.cycle == cycle,
                           AutoCallList.called_counts < 2,
                           AutoCallList.numbers != '',
                           AutoCallList.status == AutoListStatus.PENDING.value)
                    .order_by(AutoCallList.called_counts, AutoCallList.id)
                    .offset(offset)
                    .limit(1)
                    .first())

        if not _item:
            _item = (
                AutoCallList
                    .select()
                    .where(AutoCallList.cycle == cycle,
                           AutoCallList.numbers != '',
                           AutoCallList.status == AutoListStatus.PENDING.value)
                    .order_by(AutoCallList.called_times, AutoCallList.id)
                    .offset(offset)
                    .limit(1)
                    .first())

        if not _item:
            # 直接返回空
            break

        with db.atomic():
            item = (
                AutoCallList
                    .select()
                    .where(AutoCallList.cycle == cycle,
                           AutoCallList.status == AutoListStatus.PENDING.value,
                           AutoCallList.id == _item.id)
                    .for_update()
                    .first())
            if not item:
                continue

            q = (AutoCallList
                 .update(called_times=AutoCallList.called_times + 1,
                         called_counts=AutoCallList.called_counts + 1,
                         called_rounds=AutoCallList.called_rounds + 1,
                         status=AutoListStatus.PROCESSING.value,
                         description='processing')
                 .where(AutoCallList.id == item.id,
                        AutoCallList.status == AutoListStatus.PENDING.value))
            q.execute()

            AutoCallListHistory.create(
                list_id=item.id,
                application_id=item.application,
                follow_up_date=item.follow_up_date,
                cycle=item.cycle,
                called_times=item.called_times,
                status=item.status,
                next_number=item.next_number,
                numbers=item.numbers,
                called_rounds=item.called_rounds
            )

            update_application = (
                Application
                    .update(called_times=Application.called_times + 1)
                    .where(Application.id == item.application_id)
            )
            update_application.execute()

            # 对于cycle 2的件前两次只打本人，只有后两次才会拨打ec
            user_id = item.application.user_id
            if (item.application.cycle == Cycle.C1B.value
                    and item.called_counts < 2):
                self_numbers = (Contact
                                .select(Contact.number)
                                .where(Contact.user_id == user_id,
                                       Contact.relationship ==
                                       Relationship.APPLICANT.value,
                                       Contact.latest_status
                                       .not_in(ContactStatus.no_use())))

                if self_numbers.exists():
                    # 如果本人的电话不在自动外呼列表中不拨打
                    an = (item.numbers or '').split(',')
                    numbers = [i.number for i in self_numbers if i.number in an]
                    logging.info(
                        "customer_number: %s" % str(item.application_id))

                    return {
                        'status': 'success',
                        'customer_number': str(item.application_id),
                        'phone': ','.join(numbers),
                        'user_id': str(user_id)
                    }

            numbers = (item.numbers or '').split(',')

            remaining_numbers = (
                numbers[numbers.index(item.next_number):] or numbers
                if item.next_number else numbers
            )

            logging.info("customer_number: %s" % str(item.application_id))

            return {
                'status': 'success',
                'customer_number': str(item.application_id),
                'phone': ','.join(remaining_numbers),
                'user_id': str(user_id)
            }


@get('/api/v1/auto-call/applications/<app_id>/popup')
def application_popup(bomber, application):
    return


@get('/api/v1/auto-call/applications/<app_id>/action-history',
     skip=[ApplicationPlugin])
def actions_history(bomber, app_id):
    application = Application.filter(Application.id == app_id).first()
    if not application:
        abort(404, 'application not found')

    actions = AutoCallActions.filter(AutoCallActions.application == app_id)
    if not is_allow_call_id(bomber):
        for action in actions:
            action.call_id = None

    return auto_call_actions_serializer.dump(actions, many=True).data


@get('/api/v1/auto-call/applications/<app_id>/repayment-log-history',
     skip=[ApplicationPlugin])
def repayment_log_history(bomber, app_id):
    # bill_subs = BillService().external_sub_bills(bill_id=app_id)
    # return bill_subs
    repayment_logs = (RepaymentLogR
                      .select(RepaymentLogR.cycle,
                              RepaymentLogR.current_bomber,
                              RepaymentLogR.repay_at.alias('finished_at'),
                              RepaymentLogR.principal_part
                              .alias('principal_paid'),
                              RepaymentLogR.late_fee_part
                              .alias('late_fee_paid'),
                              RepaymentLogR.periods,
                              Bomber.username,
                              Bomber.name,
                              Bomber.id.alias("bid"))
                      .join(Bomber, JOIN_LEFT_OUTER,
                            RepaymentLogR.ptp_bomber == Bomber.id)
                      .filter(RepaymentLogR.application == app_id,
                              ((RepaymentLogR.principal_part > 0) |
                               (RepaymentLogR.late_fee_part > 0)))
                      .group_by(RepaymentLogR.repay_at,
                                RepaymentLogR.application)
                      .dicts())
    result = []
    for repayment in repayment_logs:
        bid = repayment.pop('bid')
        username = repayment.pop('username')
        name = repayment.pop('name')
        repayment['current_bomber'] = {
            "id":bid,
            "username":username,
            "name": name
        }
        result.append(repayment)
    return repayment_log_serializer.dump(result, many=True).data


def is_allow_call_id(bomber):
    # admin, oetari_1al, widyawati_1bl, kevin_2l, evelyin_admin,
    # kevin_3l  xifan_admin
    return bomber.id in (0, 2, 3, 11, 41, 50, 73)


@post('/api/v1/auto-call/applications/<app_id>/popup',
      skip=[ApplicationPlugin])
def patch_application_popup(bomber, app_id):
    form = add_auto_call_popup_action_validator(request.json)
    auto_call_item = (
        AutoCallList
        .select()
        .join(Application)
        .where(AutoCallList.application == app_id,
               AutoCallList.status == AutoListStatus.PROCESSING.value)
        .first()
    )
    if not auto_call_item:
        abort(404, 'application not found')
    application = auto_call_item.application

    contact = Contact.filter(Contact.user_id == application.user_id,
                             Contact.id == form['contact_id']).first()
    if not contact:
        abort(404, 'contact not found')

    with db.atomic():
        AutoCallActionsPopup.create(
            application=application.id,
            cycle=application.cycle,
            name=contact.name,
            number=contact.number,
            relationship=form['relationship'],
            sub_relation=form['sub_relation'],
            bomber=bomber.id
        )


@post('/api/v1/auto-call/applications/<app_id>/action-history',
      skip=[ApplicationPlugin])
def post_actions(bomber, app_id):
    form = add_auto_call_action_validator(request.json)
    auto_call_item = (
        AutoCallList
        .select()
        .join(Application)
        .where(AutoCallList.application == app_id,
               AutoCallList.status == AutoListStatus.PROCESSING.value)
        .first()
    )
    if not auto_call_item:
        abort(404, 'application not found')
    application = auto_call_item.application

    contact = Contact.filter(Contact.user_id == application.user_id,
                             Contact.id == form['contact_id']).first()
    if not contact:
        abort(404, 'contact not found')

    with db.atomic():
        sub_relation = form['sub_relation']
        result = form.get('result')
        reason = form.get('reason')
        follow_up_date = form.get('follow_up_date')
        promised_date = form.get('promised_date')
        promised_amount = form.get('promised_amount')

        contact.useful = form['useful']
        contact.real_relationship = form['relationship']
        contact.sub_relation = sub_relation
        contact.save()

        actions = AutoCallActions.create(
            application=application.id,
            cycle=application.cycle,
            name=contact.name,
            number=contact.number,
            relationship=contact.relationship,
            sub_relation=sub_relation,
            bomber=bomber.id,
            result=result,
            reason=reason,
            promised_amount=promised_amount,
            promised_date=promised_date,
            follow_up_date=follow_up_date,
            notes=form.get('notes'),
            auto_notes=form.get('auto_notes'),
            call_id=form.get('call_id')
        )

        # 设置下一个开始拨打的号码
        numbers = (auto_call_item.numbers or '').split(',')
        current_idx = numbers.index(contact.number)
        # 防止超过numbers
        next_number = numbers[(current_idx+1) % len(numbers)]
        auto_call_item.next_number = next_number

        # 修改判断号码是否为not useful的判断规则
        notes = (Notes
                 .select()
                 .where(Notes.note == form.get('auto_notes'))
                 .first())
        index = 0
        if notes.note == 'Mailbox':
            auto_call_actions = (AutoCallActions
                                 .select()
                                 .where(AutoCallActions.number ==
                                        contact.number)
                                 .order_by(-AutoCallActions.created_at)
                                 .limit(3))
            for auto_call_action in auto_call_actions:
                if auto_call_action.auto_notes == 'Mailbox':
                    index += 1
        if (notes.note == 'Wrong Number' or index == 3 or
                notes.groups == 'Connected, but useless'):

            contact.latest_status = ContactStatus.NO_USE.value
            contact.save()

            # 把无用联系人从队列中剔除
            numbers.pop(current_idx)
            auto_call_item.numbers = ','.join(numbers)

        if result == AutoCallResult.FOLLOW_UP.value:
            if not follow_up_date:
                abort(400, 'follow up date required')

            application.follow_up_date = follow_up_date
            application.save()

            auto_call_item.follow_up_date = follow_up_date

        auto_call_item.status = AutoListStatus.PENDING.value

        if result == AutoCallResult.PTP.value:
            if not promised_amount or not promised_date:
                abort(400, 'promise amount and date required')
            if promised_date < datetime.now().date():
                abort(400, 'promise date invalid')

            application.promised_date = promised_date
            application.promised_amount = promised_amount

            application.claimed_at = datetime.now()
            application.last_bomber = application.latest_bomber
            application.latest_bomber = bomber.id
            application.status = ApplicationStatus.PROCESSING.value
            application.ptp_bomber = bomber.id

            auto_call_item.status = AutoListStatus.REMOVED.value

        if result == AutoCallResult.CONTINUE.value:
            auto_call_item.called_times -= 1
            auto_call_item.called_rounds -= 1

        auto_call_item.save()

        application.latest_call = bomber.id
        application.save()

    return auto_call_actions_serializer.dump(actions).data


@post('/api/v1/auto-call/applications/<app_id>/action-history2',
      skip=[ApplicationPlugin])
def post_actions2(bomber, app_id):
    form = add_auto_call_action2_validator(request.json)
    auto_call_item = (
        AutoCallList
        .select()
        .join(Application)
        .where(AutoCallList.application == app_id)
        .first())
    if (not auto_call_item or
            auto_call_item.status != AutoListStatus.PROCESSING.value):
        # return directly when in this loop
        post_actions_report_error(auto_call_item)

    with db.atomic():
        auto_call_item.status = AutoListStatus.PENDING.value
        auto_call_item.save()

        application = auto_call_item.application

        contact = Contact.filter(Contact.user_id == application.user_id,
                                 Contact.id == form['contact_id']).first()
        if not contact:
            abort(404, 'contact not found')

        real_relationship = form.get('real_relationship')
        relationship = bomber_contact(real_relationship, form['relationship'])

        sub_relation = form['sub_relation']
        commit = form['commit']
        phone_status = form.get('phone_status')
        result = auto_call_result(commit,
                                  phone_status,
                                  form.get('admit_loan'),
                                  form.get('pay_willing'),
                                  form.get('help_willing'))
        follow_up_date = form.get('follow_up_date')
        promised_date = form.get('promised_date')
        promised_amount = form.get('promised_amount')

        contact.useful = form['useful']
        contact.real_relationship = relationship
        contact.sub_relation = sub_relation

        not_first = check_commit_count(CallActionType.AUTO.value, form,
                                       application, bomber,
                                       contact, relationship)
        if not_first:
            abort(400, 'repeated submit')

        actions = AutoCallActions.create(
            application=application.id,
            cycle=application.cycle,
            name=contact.name,
            number=contact.number,
            relationship=relationship,
            sub_relation=sub_relation,
            bomber=bomber.id,
            result=result,
            promised_amount=promised_amount,
            promised_date=promised_date,
            follow_up_date=follow_up_date,
            notes=form.get('note'),
            call_id=form.get('call_id')
        )
        if bomber.id == SpecialBomber.OLD_APP_BOMBER.value:
            set_ptp_for_special_bomber(app_id, promised_date)

        CallActions.create(
            type=CallActionType.AUTO.value,
            application=application.id,
            cycle=application.cycle,
            name=contact.name,
            number=contact.number,
            relationship=relationship,
            sub_relation=sub_relation,
            bomber_id=bomber.id,
            phone_status=phone_status,
            real_relationship=real_relationship,
            contact_id=contact.id,
            call_record_id=actions.id,
            admit_loan=form.get('admit_loan'),
            still_old_job=form.get('still_old_job'),
            new_company=form.get('new_company'),
            overdue_reason=form.get('overdue_reason'),
            overdue_reason_desc=form.get('overdue_reason_desc'),
            pay_willing=form.get('pay_willing'),
            pay_ability=form.get('pay_ability'),
            note=form.get('note'),
            commit=commit,
            connect_applicant=form.get('connect_applicant'),
            has_job=form.get('has_job'),
            help_willing=form.get('help_willing'),
            no_help_reason=form.get('no_help_reason'),
            last_connection_to_applicant=(
                form.get('last_connection_to_applicant')),

            promised_amount=promised_amount,
            promised_date=promised_date,
            follow_up_date=follow_up_date,
            call_id=form.get('call_id'),
            helpful=form.get('helpful')
        )
        contact = check_call_priority(contact, phone_status,
                                      real_relationship, commit)

        delete = False
        auto_call_actions = []
        if real_relationship in (RealRelationship.NO_RECOGNIZE.value,
                                 RealRelationship.UNWILLING_TO_TELL.value):
            # 催记中号码的real_relationship有被标记为1，2，3，4继续拨打
            use_status = RealRelationship.user_values()
            check_exists = (CallActions.select()
                            .where(CallActions.application == app_id,
                                   CallActions.number == contact.number,
                                   CallActions.real_relationship << use_status)
                            )
            if not check_exists.exists():
                delete = True

        if result == AutoCallResult.MAIL_BOX.value:
            # 最后八次通话是否是语音信箱
            auto_call_actions = (CallActions
                                 .select()
                                 .where(CallActions.number == contact.number)
                                 .order_by(-CallActions.created_at)
                                 .limit(8) or [])
            last_three_phones = [i.phone_status == PhoneStatus.MAIL_BOX.value
                                 for i in auto_call_actions]
            if (len(last_three_phones) == 8) and all(last_three_phones):
                delete = True

        auto_call_item, contact = auto_call_set_phone(auto_call_item,
                                                      contact,
                                                      delete)

        if not delete and result == AutoCallResult.MAIL_BOX.value:
            today = datetime.today().date()
            mail_box_count = 0
            for index,auto_call in enumerate(list(auto_call_actions)):
                if index > 1:
                    break
                if (auto_call.created_at.date() >= today and
                auto_call.phone_status == PhoneStatus.MAIL_BOX.value):
                    mail_box_count += 1
            if mail_box_count <= 1:
                auto_call_item = mail_box_set_phone(auto_call_item, contact)
            else:
                auto_call_item = mail_box_set_phone(auto_call_item,
                                                    contact,
                                                    times = mail_box_count)


        if result == AutoCallResult.FOLLOW_UP.value:
            application, auto_call_item = auto_call_follow_up(follow_up_date,
                                                              application,
                                                              auto_call_item)

        if result == AutoCallResult.PTP.value:
            application, auto_call_item = auto_call_ptp(promised_amount,
                                                        promised_date,
                                                        application,
                                                        auto_call_item,
                                                        bomber)

        if result == AutoCallResult.CONTINUE.value:
            auto_call_item = auto_call_continue(auto_call_item)

        contact.save()
        auto_call_item.save()
        application.latest_call = bomber.id
        application.save()


@get('/api/v1/auto-call/summary', skip=[packing_plugin])
def auto_summary(bomber):
    args = plain_query()

    start_date, end_date = None, None
    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    history = Summary2.select(
        Summary2.bomber,
        fn.SUM(Summary2.answered_calls).alias('answered_calls'),
        fn.SUM(Summary2.ptp).alias('ptp'),
        fn.SUM(Summary2.follow_up).alias('follow_up'),
        fn.SUM(Summary2.not_useful).alias('not_useful'),
        fn.SUM(Summary2.cleared).alias('cleared'),
        fn.SUM(Summary2.amount_recovered).alias('amount_recovered'),
    ).where(Summary2.bomber.is_null(False))

    cal_date = date.today()
    employees = Bomber.select(Bomber, Role).join(Role)
    actions = (AutoCallActions.select(AutoCallActions.bomber,
                                      AutoCallActions.result,
                                      fn.COUNT(AutoCallActions.id)
                                      .alias('count'))
               .where(fn.DATE(AutoCallActions.created_at) == cal_date))

    if not is_allow_call_id(bomber):
        for action in actions:
            action.call_id = None

    amount_recovered = (RepaymentLog
                        .select(RepaymentLog.current_bomber,
                                fn.SUM(RepaymentLog.principal_part)
                                .alias('principal_part'),
                                fn.SUM(RepaymentLog.late_fee_part)
                                .alias('late_fee_part'))
                        .where(fn.DATE(RepaymentLog.repay_at) == cal_date,
                               RepaymentLog.current_bomber.is_null(False),
                               RepaymentLog.is_bombed == True))

    cleared = (Application
               .select(Application.latest_bomber,
                       fn.COUNT(Application.id).alias('cleared'))
               .where(fn.DATE(Application.finished_at) == cal_date,
                      Application.status == ApplicationStatus.REPAID.value,
                      Application.latest_bomber.is_null(False)))

    if 'bomber_id' in args:
        employees = employees.where(Bomber.id == args.bomber_id)
        history = history.where(Summary2.bomber == args.bomber_id)
        actions = actions.where(AutoCallActions.bomber == args.bomber_id)
        amount_recovered = amount_recovered.where(
            RepaymentLog.current_bomber == args.bomber_id
        )
        cleared = cleared.where(Application.latest_bomber == args.bomber_id)

    # cycle 控制 本cycle的主管只能看本cycle的统计
    if bomber.role.cycle and 'bomber_id' not in args:
        employees = employees.where(
            Role.cycle == bomber.role.cycle
        )
        cycle_bids = [e.id for e in employees]
        history = history.where(Summary2.bomber << cycle_bids)
        actions = actions.where(AutoCallActions.bomber << cycle_bids)
        amount_recovered = amount_recovered.where(
            RepaymentLog.current_bomber << cycle_bids,
        )
        cleared = cleared.where(Application.latest_bomber << cycle_bids)

    if start_date and end_date:
        history = history.where(
            Summary2.date >= start_date,
            Summary2.date <= end_date,
        )

    history = history.group_by(Summary2.bomber)
    actions = actions.group_by(AutoCallActions.bomber, AutoCallActions.result)
    amount_recovered = amount_recovered.group_by(RepaymentLog.current_bomber)
    cleared = cleared.group_by(Application.latest_bomber)

    summary = {
        e.id: {
            'name': e.name,
            'cycle': e.role.cycle,
            'answered_calls': 0,
            'ptp': 0,
            'follow_up': 0,
            'not_useful': 0,
            'cleared': 0,
            'amount_recovered': 0,
        }
        for e in employees if e.name
    }

    for i in history:
        summary[i.bomber_id]['answered_calls'] += int(i.answered_calls)
        summary[i.bomber_id]['ptp'] += int(i.ptp)
        summary[i.bomber_id]['follow_up'] += int(i.follow_up)
        summary[i.bomber_id]['not_useful'] += int(i.not_useful)
        summary[i.bomber_id]['cleared'] += int(i.cleared)
        summary[i.bomber_id]['amount_recovered'] += i.amount_recovered

    # 如果 区间 不包含 today 则不计算当天数据 直接返回历史数据
    if start_date and end_date and end_date < cal_date:
        result = []
        for bomber_id, data in summary.items():
            result.append({
                'name': data['name'],
                'cycle': data['cycle'],
                'answered_calls': data['answered_calls'],
                'ptp': data['ptp'],
                'follow_up': data['follow_up'],
                'not_useful': data['not_useful'],
                'cleared': data['cleared'],
                'amount_recovered': str(data['amount_recovered']),
            })
        if 'export' in args and args.export == '1':
            response.set_header('Content-Type', 'text/csv')
            response.set_header('Content-Disposition',
                                'attachment; filename="bomber_call_export.csv"')

            with StringIO() as csv_file:
                fields = (
                    'name', 'cycle', 'answered_calls',
                    'ptp', 'follow_up', 'not_useful',
                    'cleared', 'amount_recovered',
                )
                w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
                w.writeheader()
                w.writerows(result)
                return csv_file.getvalue().encode('utf8', 'ignore')
        return {'data': result}

    for a in actions:
        summary[a.bomber_id]['answered_calls'] += a.count
        if a.result == AutoCallResult.PTP.value:
            summary[a.bomber_id]['ptp'] += a.count
        if a.result == AutoCallResult.FOLLOW_UP.value:
            summary[a.bomber_id]['follow_up'] += a.count
        if a.result == AutoCallResult.NOT_USEFUL.value:
            summary[a.bomber_id]['not_useful'] += a.count

    for i in amount_recovered:
        amount_recovered = i.principal_part + i.late_fee_part
        summary[i.current_bomber_id]['amount_recovered'] += amount_recovered

    for i in cleared:
        summary[i.latest_bomber_id]['cleared'] += i.cleared

    result = []
    for bomber_id, data in summary.items():
        result.append({
            'name': data['name'],
            'cycle': data['cycle'],
            'answered_calls': data['answered_calls'],
            'ptp': data['ptp'],
            'follow_up': data['follow_up'],
            'not_useful': data['not_useful'],
            'cleared': data['cleared'],
            'amount_recovered': str(data['amount_recovered']),
        })
    if 'export' in args and args.export == '1':
        response.set_header('Content-Type', 'text/csv')
        response.set_header('Content-Disposition',
                            'attachment; filename="bomber_export.csv"')

        with StringIO() as csv_file:
            fields = (
                'name', 'cycle', 'answered_calls',
                'ptp', 'follow_up', 'not_useful',
                'cleared', 'amount_recovered',
            )
            w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(result)
            return csv_file.getvalue().encode('utf8', 'ignore')
    return {'data': result}


@get('/api/v1/auto-call/no-answer')
def auto_call_no_answer():
    form = auto_call_no_answer_validator(plain_query())
    app_id = form['customer_number']
    logging.info("application %s no answer", app_id)
    update_auto_call_list = (
        AutoCallList
        .update(status=AutoListStatus.PENDING.value, description='no answer')
        .where(AutoCallList.application == app_id,
               AutoCallList.status == AutoListStatus.PROCESSING.value)
    )
    update_auto_call_list.execute()
    return {
        'status': 'success',
    }


@get('/api/v1/auto-call/list-process')
def auto_call_list_process(bomber):
    list_data = (
        AutoCallList
        .select(
            AutoCallList.cycle,
            AutoCallList.status,
            fn.IF(AutoCallList.follow_up_date > fn.NOW(), 1, 0)
            .alias('is_pending_follow_up'),
            fn.COUNT(AutoCallList.id).alias('count'),
            fn.max(AutoCallList.called_times).alias('round')
        )
        .group_by(
            AutoCallList.cycle,
            AutoCallList.status,
            fn.IF(AutoCallList.follow_up_date > fn.NOW(), 1, 0),
        )
    )
    cycle_map = {
        Cycle.C1A.value: '1a',
        Cycle.C1B.value: '1b',
        Cycle.C2.value: '2',
        Cycle.C3.value: '3',
    }
    status_map = {
        0: 'pending',
        1: 'processing',
        2: 'removed',
    }
    result = {
        k: {
            'cycle': v,
            'pending': 0,
            'processing': 0,
            'follow_up': 0,
            'removed': 0,
            'round': 0,
        }
        for k, v in cycle_map.items()
    }
    for item in list_data:
        data = result.get(item.cycle, {})
        data['cycle'] = cycle_map.get(item.cycle, 'unknown')
        status = status_map.get(item.status)
        if not status:
            continue
        if status == 'pending' and item.is_pending_follow_up:
            data['follow_up'] = item.count
        else:
            data[status] += item.count

        data['round'] = max(
            data.get('round', 0),
            item.round,
        )
        result[item.cycle] = data

    return list(result.values())


@get('/api/v1/auto-call/summary/cycle', skip=[packing_plugin])
def auto_summary_cycle(bomber):
    args = plain_query()

    start_date, end_date = None, None
    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    history = Summary2.select(
        Summary2.cycle,
        fn.SUM(Summary2.answered_calls).alias('answered_calls'),
        fn.SUM(Summary2.ptp).alias('ptp'),
        fn.SUM(Summary2.follow_up).alias('follow_up'),
        fn.SUM(Summary2.not_useful).alias('not_useful'),
        fn.SUM(Summary2.cleared).alias('cleared'),
        fn.SUM(Summary2.amount_recovered).alias('amount_recovered'),
    ).where(Summary2.cycle.is_null(False), Summary2.cycle != 0)

    cal_date = date.today()
    actions = (AutoCallActions.select(AutoCallActions.cycle,
                                      AutoCallActions.result,
                                      fn.COUNT(AutoCallActions.id)
                                      .alias('count'))
               .where(fn.DATE(AutoCallActions.created_at) == cal_date))
    if not is_allow_call_id(bomber):
        for action in actions:
            action.call_id = None

    amount_recovered = (RepaymentLog
                        .select(RepaymentLog.cycle,
                                fn.SUM(RepaymentLog.principal_part)
                                .alias('principal_part'),
                                fn.SUM(RepaymentLog.late_fee_part)
                                .alias('late_fee_part'))
                        .where(fn.DATE(RepaymentLog.repay_at) == cal_date,
                               RepaymentLog.current_bomber.is_null(False),
                               RepaymentLog.is_bombed == True))

    cleared = (Application
               .select(Application.cycle,
                       fn.COUNT(Application.id).alias('cleared'))
               .where(fn.DATE(Application.finished_at) == cal_date,
                      Application.status == ApplicationStatus.REPAID.value,
                      Application.latest_bomber.is_null(False)))

    if start_date and end_date:
        history = history.where(
            Summary2.date >= start_date,
            Summary2.date <= end_date,
        )

    history = history.group_by(Summary2.cycle)
    actions = actions.group_by(AutoCallActions.cycle, AutoCallActions.result)
    amount_recovered = amount_recovered.group_by(RepaymentLog.cycle)
    cleared = cleared.group_by(Application.cycle)

    cycle_map = {
        Cycle.C1A.value: '1a',
        Cycle.C1B.value: '1b',
        Cycle.C2.value: '2',
        Cycle.C3.value: '3',
        Cycle.M3.value: 'M3+',
    }

    summary = {
        i: {
            'cycle': cycle_map[i],
            'answered_calls': 0,
            'ptp': 0,
            'follow_up': 0,
            'not_useful': 0,
            'cleared': 0,
            'amount_recovered': 0,
        }
        for i in Cycle.values()
    }

    for i in history:
        summary[i.cycle]['answered_calls'] += int(i.answered_calls)
        summary[i.cycle]['ptp'] += int(i.ptp)
        summary[i.cycle]['follow_up'] += int(i.follow_up)
        summary[i.cycle]['not_useful'] += int(i.not_useful)
        summary[i.cycle]['cleared'] += int(i.cleared)
        summary[i.cycle]['amount_recovered'] += i.amount_recovered

    # 如果 区间 不包含 today 则不计算当天数据 直接返回历史数据
    if start_date and end_date and end_date < cal_date:
        result = []
        for cycle, data in summary.items():
            result.append({
                'cycle': data['cycle'],
                'answered_calls': data['answered_calls'],
                'ptp': data['ptp'],
                'follow_up': data['follow_up'],
                'not_useful': data['not_useful'],
                'cleared': data['cleared'],
                'amount_recovered': str(data['amount_recovered']),
            })
        if 'export' in args and args.export == '1':
            response.set_header('Content-Type', 'text/csv')
            response.set_header('Content-Disposition',
                                'attachment; filename="bomber_call_export.csv"')

            with StringIO() as csv_file:
                fields = (
                    'cycle', 'answered_calls', 'ptp', 'follow_up', 'not_useful',
                    'cleared', 'amount_recovered',
                )
                w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
                w.writeheader()
                w.writerows(result)
                return csv_file.getvalue().encode('utf8', 'ignore')
        return {'data': result}

    for a in actions:
        summary[a.cycle]['answered_calls'] += a.count
        if a.result == AutoCallResult.PTP.value:
            summary[a.cycle]['ptp'] += a.count
        if a.result == AutoCallResult.FOLLOW_UP.value:
            summary[a.cycle]['follow_up'] += a.count
        if a.result == AutoCallResult.NOT_USEFUL.value:
            summary[a.cycle]['not_useful'] += a.count

    for i in amount_recovered:
        amount_recovered = i.principal_part + i.late_fee_part
        summary[i.cycle]['amount_recovered'] += amount_recovered

    for i in cleared:
        summary[i.cycle]['cleared'] += i.cleared

    result = []
    for cycle, data in summary.items():
        result.append({
            'cycle': data['cycle'],
            'answered_calls': data['answered_calls'],
            'ptp': data['ptp'],
            'follow_up': data['follow_up'],
            'not_useful': data['not_useful'],
            'cleared': data['cleared'],
            'amount_recovered': str(data['amount_recovered']),
        })
    if 'export' in args and args.export == '1':
        response.set_header('Content-Type', 'text/csv')
        response.set_header('Content-Disposition',
                            'attachment; filename="bomber_export.csv"')

        with StringIO() as csv_file:
            fields = (
                'cycle', 'answered_calls', 'ptp', 'follow_up', 'not_useful',
                'cleared', 'amount_recovered',
            )
            w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(result)
            return csv_file.getvalue().encode('utf8', 'ignore')
    return {'data': result}


# 获取坐席号对应外呼状态
@get('/api/v1/bomber/ext/auto_call/status', skip=[ip_whitelist_plugin])
def get_bomber_auto_call_status():
    bomber_ptp = (BomberPtpR.select()
                  .where(BomberPtpR.auto_ext.is_null(False)))
    result = []
    for bp in bomber_ptp:
        disable = 'n'
        if bp.switch is not None:
            if bp.switch == BomberCallSwitch.OFF.value:
                disable = 'y'
            result.append({'ext': bp.auto_ext, 'disable': disable})
            continue
        if bp.today_switch is not None:
            if bp.today_switch == BomberCallSwitch.OFF.value:
                disable = 'y'
            if (bp.today_switch == BomberCallSwitch.ON.value and
                    bp.ptp_switch == BomberCallSwitch.OFF.value):
                disable = 'y'
            result.append({'ext': bp.auto_ext, 'disable': disable})
            continue
        if bp.ptp_switch is not None:
            if bp.ptp_switch == BomberCallSwitch.OFF.value:
                disable = 'y'
        result.append({'ext': bp.auto_ext, 'disable': disable})
    return result