#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from decimal import Decimal
from datetime import datetime, timedelta, date
import logging

from bottle import abort

from bomber.models import (
    OldLoanApplication,
    SystemConfig,
    Application,
    CallActions,
    Contact,
    SCI,
)
from bomber.constant_mapping import (
    ApplicationStatus,
    RealRelationship,
    CallActionCommit,
    ApplicantSource,
    PriorityStatus,
    AutoCallResult,
    AutoListStatus,
    SpecialBomber,
    OldLoanStatus,
    ContactStatus,
    SourceOption,
    Relationship,
    HelpWilling,
    PhoneStatus,
    PayWilling,
    AdmitLoan,
)

from bomber.utils import get_cycle_by_overdue_days

def late_fee_limit(cycle):
    # 罚金减免上限
    name = 'CYCLE_{}_DISCOUNT'.format(cycle)
    try:
        key = getattr(SCI, name)
    except AttributeError:
        return Decimal('0')
    config = SystemConfig.prefetch(key)
    return config.get(key, key.default_value)


def contact_classify(contacts, relationship, bomber=None, app_id=None):
    # 2018-11-26: 本人，家人，公司,非申请人本人填写的一律放到suggested
    need_classify_list = Relationship.need_classify()  #[0,1,2]
    need_collection = SourceOption.need_collection()
    if bomber and bomber.id == SpecialBomber.OLD_APP_BOMBER.value:
        old_app = OldLoanApplication.get(
            OldLoanApplication.application_id == app_id,
            OldLoanApplication.status == OldLoanStatus.PROCESSING.value)
        _number_str = old_app and old_app.numbers or ''
        numbers = [nu for nu in _number_str.split(',') if nu]
        contacts = (contacts
                    .where(Contact.relationship == relationship,
                           Contact.source ==
                           ApplicantSource.NEW_APPLICANT.value,
                           Contact.number.in_(numbers)))
    elif relationship in need_classify_list:
        source_list = need_collection.get(relationship, [])

        contacts = (contacts
                    .where(Contact.source
                           .in_(source_list),
                           Contact.relationship == relationship))
    else:
        contacts3 = (contacts
                     .where(Contact.relationship
                            .not_in(need_classify_list)))
        for relation in need_classify_list:
            source_list = need_collection.get(relation, [])
            contacts3 = contacts3 | (contacts
                                     .where(Contact.relationship == relation,
                                            Contact.source.not_in(source_list))
                                     )

        contacts = contacts3
    return contacts


def reserved_new(data_list):
    t1_list = []
    t2_list = []
    for item in data_list:
        _id = item['id']
        if _id not in t1_list:
            t1_list.append(_id)
            t2_list.append(item)
    return t2_list


def bomber_contact(real_relationship, relationship):
    if real_relationship == RealRelationship.SELF.value:
        return Relationship.APPLICANT.value
    elif real_relationship == RealRelationship.SPOUSE_OR_FAMILY.value:
        return Relationship.FAMILY.value
    elif real_relationship == RealRelationship.COLLEAGUE.value:
        return Relationship.COMPANY.value
    elif real_relationship == RealRelationship.FRIEND.value:
        return Relationship.SUGGESTED.value
    else:
        return relationship


def auto_call_result(commit, phone_status=None, admit_loan=None,
                     pay_willing=None, help_willing=None):
    if phone_status == PhoneStatus.MAIL_BOX.value:
        return AutoCallResult.MAIL_BOX.value

    if commit == CallActionCommit.FOLLOW_UP.value:
        return AutoCallResult.FOLLOW_UP.value

    if admit_loan == AdmitLoan.YES.value:
        if pay_willing == PayWilling.WILLING_TO_PAY.value:
            return AutoCallResult.FOLLOW_UP.value
        elif pay_willing == PayWilling.PTP.value:
            return AutoCallResult.PTP.value

    if help_willing == HelpWilling.YES.value:
        return AutoCallResult.FOLLOW_UP.value
    elif help_willing == HelpWilling.REIMBURSEMENT.value:
        return AutoCallResult.PTP.value

    return AutoCallResult.CONTINUE.value


def auto_call_set_phone(auto_call_item, contact, delete=False):
    # 设置下一个开始拨打的号码, 并判断是否无效
    numbers = (auto_call_item.numbers or '').split(',')
    if contact.number not in numbers:
        abort(404, 'Invalid number')
    current_idx = numbers.index(contact.number)
    # 防止超过numbers
    next_number = numbers[(current_idx + 1) % len(numbers)]
    auto_call_item.next_number = next_number

    if delete:
        contact.latest_status = ContactStatus.NO_USE.value
        numbers.pop(current_idx)
        auto_call_item.numbers = ','.join(numbers)
    return auto_call_item, contact


def auto_call_follow_up(follow_up_date, application, auto_call_item):
    if not follow_up_date:
        abort(400, 'follow up date required')

    application.follow_up_date = follow_up_date

    auto_call_item.follow_up_date = follow_up_date
    auto_call_item.status = AutoListStatus.PENDING.value
    return application, auto_call_item


def auto_call_ptp(promised_amount, promised_date,
                  application, auto_call_item, bomber):
    if not promised_amount or not promised_date:
        abort(400, 'promise amount and date required')
    if promised_date < datetime.now().date():
        abort(400, 'promise date invalid')

    new_cycle = get_cycle_by_overdue_days(application.overdue_days)
    if new_cycle > application.cycle:
        abort(400, 'Can not extend PTP')


    application.promised_date = promised_date
    application.promised_amount = promised_amount
    application.claimed_at = datetime.now()
    application.last_bomber = application.latest_bomber
    application.latest_bomber = bomber.id
    application.status = ApplicationStatus.PROCESSING.value
    application.ptp_bomber = bomber.id

    auto_call_item.status = AutoListStatus.REMOVED.value
    return application, auto_call_item


def auto_call_continue(auto_call_item):
    auto_call_item.called_times -= 1
    auto_call_item.called_rounds -= 1
    auto_call_item.status = AutoListStatus.PENDING.value
    return auto_call_item


def set_ptp_for_special_bomber(application_id, promised_date):
    return (OldLoanApplication
            .update(promised_date=promised_date)
            .where(OldLoanApplication.application_id == application_id,
                   OldLoanApplication.status == OldLoanStatus.PROCESSING.value)
            .execute())


def check_call_priority(contact, status, real, commit):
    if contact.call_priority != PriorityStatus.DEFAULT.value:
        return contact
    if (status == PhoneStatus.CONNECTED.value
        and real in RealRelationship.user_values()
            and commit == CallActionCommit.NO.value):
        contact.call_priority = 1
    return contact


def check_commit_count(call_type, form, app, bomber, contact, relationship):
    end_date = datetime.now() - timedelta(minutes=3)
    real_relationship = form.get('real_relationship')
    still_old_job = form.get('still_old_job')
    overdue_reason = form.get('overdue_reason')
    reason_desc = form.get('overdue_reason_desc')
    connect_applicant = form.get('connect_applicant')
    no_help_reason = form.get('no_help_reason')
    last = form.get('last_connection_to_applicant')
    call_action = (CallActions.select()
                   .where(CallActions.type == call_type,
                          CallActions.number == contact.number,
                          CallActions.relationship == relationship,
                          CallActions.sub_relation == form.get('sub_relation'),
                          CallActions.bomber_id == bomber.id,
                          CallActions.contact_id == contact.id,
                          CallActions.application == app.id,
                          CallActions.phone_status == form.get('phone_status'),
                          CallActions.real_relationship == real_relationship,
                          CallActions.admit_loan == form.get('admit_loan'),
                          CallActions.still_old_job == still_old_job,
                          CallActions.new_company == form.get('new_company'),
                          CallActions.overdue_reason == overdue_reason,
                          CallActions.overdue_reason_desc == reason_desc,
                          CallActions.pay_willing == form.get('pay_willing'),
                          CallActions.pay_ability == form.get('pay_ability'),
                          CallActions.note == form.get('note'),
                          CallActions.commit == form.get('commit'),
                          CallActions.connect_applicant == connect_applicant,
                          CallActions.has_job == form.get('has_job'),
                          CallActions.help_willing == form.get('help_willing'),
                          CallActions.no_help_reason == no_help_reason,
                          CallActions.last_connection_to_applicant == last,
                          CallActions.created_at >= end_date
                          ))

    if call_action.exists():
        return True
    return False


def post_actions_report_error(auto_call_item):
    # TODO: Add all the situation
    if not auto_call_item:
        abort(404, 'application not found')

    logging.info("auto call application: %s, status: %s",
                 auto_call_item.application_id, auto_call_item.status)

    if auto_call_item.status == AutoListStatus.REMOVED.value:
        application = auto_call_item.application
        if application.status == ApplicationStatus.REPAID.value:
            abort(404, 'bill has clear')

        if (application.promised_date and
                application.promised_date.date() >= date.today()):
            abort(404, 'has been set ptp')

    abort(404, 'application not found')


# 1.一个件连续2次语音信箱,今天不在外呼改电话
# 2.如果本次是语音信箱，半个小时之后外呼该件
def mail_box_set_phone(auto_call_item,contact,times=1):
    if times == 1:
        auto_call_item.status = AutoListStatus.MAILBOX.value
    elif times >= 2:
        # 设置下一个开始拨打的号码, 并判断是否无效
        numbers = (auto_call_item.numbers or '').split(',')
        if contact.number not in numbers:
            return auto_call_item
        current_idx = numbers.index(contact.number)
        # 防止超过numbers
        next_number = numbers[(current_idx + 1) % len(numbers)]
        auto_call_item.next_number = next_number
        numbers.pop(current_idx)
        auto_call_item.numbers = ','.join(numbers)
    return auto_call_item

