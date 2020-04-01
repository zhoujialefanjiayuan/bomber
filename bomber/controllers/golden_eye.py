# -*- coding:utf-8 -*-

import logging
import datetime

from bottle import get

from bomber.models_readonly import (
    CallActionsR,
    ApplicationR,
    RepaymentLogR,
    NewCdrR
)
from bomber.models import AutoIVRActions, Contact
from bomber.constant_mapping import Cycle, ApplicationStatus
from bomber.auth import check_api_user
from bomber.plugins import ip_whitelist_plugin
from bomber.utils import plain_query


@get('/api/v1/calc/callactions/<application_id>', skip=[ip_whitelist_plugin])
def calc_call_actions_for_goden_eye(application_id):
    """
    根据application_id获取用户上次逾期数据，计算相关数据
    """
    check_api_user()
    args = plain_query()
    finished_at = args.get("finished_at")
    result = {}
    # application没有，ivr中也可能有数据
    is_ivrjt_res = calc_is_get_through_auto_ivr(application_id, finished_at)
    result.update(is_ivrjt_res)
    application = (ApplicationR
                   .select(ApplicationR.id, ApplicationR.user_id,
                           ApplicationR.finished_at, ApplicationR.created_at)
                   .where(ApplicationR.id == application_id,
                          ApplicationR.status == ApplicationStatus.REPAID.value)
                   .first())
    if not application:
        # ivr表示还款之前，没进bomber的打电话记录
        result["is_lsbr_bff"] = is_ivrjt_res["is_ivrjt"]
        return {"data": result}
    try:
        call_actions_res = calc_call_actions(application, result["is_ivrjt"])
        result.update(call_actions_res)
        newcdr_self_and_ec_res = calc_newcdr_self_and_ec_num(application)
        result.update(newcdr_self_and_ec_res)
    except Exception as e:
        logging.error(
            "calc_call_actions_for_goden_eye is defeated,appid:%s,error:%s" % (
                application_id, str(e)))
        return {"data": result}
    return {"data": result}


# 计算calc_keys中的字段
def calc_call_actions(application=None, is_lsbr_bff=0):
    result = {}
    if not application:
        logging.info("calc_call_cations is defeated, application is none")
        return result
    # 获取催收等级是1a的通话记录
    call_actions = (CallActionsR
                    .select()
                    .where(CallActionsR.cycle == Cycle.C1A.value,
                           CallActionsR.application_id == application.id)
                    .order_by(-CallActionsR.created_at)
                    )
    # 获取到还款完成的前三天和前七天的时间
    finish_three_day = application.finished_at + datetime.timedelta(days=-3)
    finish_seven_day = application.finished_at + datetime.timedelta(days=-7)
    # 获取还款信息
    repayment_logs = get_repayment_log(application)
    # 记录是否以获取到还款前最后一个通话
    is_before_finish_last_contact = False
    overdue_ptp_KP_dict = {}
    result.update({"br_tercnt": 0,
                   "br_jtcnt": 0,
                   "ec_jtcnt": 0,
                   "klec_num": 0,
                   "yxec_num": 0,
                   "hlec_num": 0,
                   "klec_tercnt": 0,
                   "od_ptp_cnt": 0,
                   "od_kp_cnt": 0
                   })
    for call in call_actions:
        if call.phone_status == 4 and call.real_relationship == 1:
            result["is_brcont"] = 1
            result["br_jtcnt"] += 1
            # 计算还款完成前3天和前7天的是否联系到本人
            if call.created_at > finish_seven_day:
                result["is_brjt_bf7laf"] = 1
            if call.created_at > finish_three_day:
                result["is_brjt_bf3laf"] = 1
            if call.commit == 1:
                result["br_tercnt"] += 1
        elif call.phone_status == 4 and call.real_relationship in (2, 3, 4):
            result["ec_jtcnt"] += 1
            result["klec_num"] += 1
            if call.connect_applicant == 1:
                result["yxec_num"] += 1
            if call.help_willing != 2:
                result["hlec_num"] += 1
            if call.commit == 1:
                result["klec_tercnt"] += 1
        else:
            pass
        if call.admit_loan == 1:
            result["is_od_nal"] = 1
        if call.overdue_reason:
            overdue_reason_key = "is_od_rs%s" % call.overdue_reason
            result[overdue_reason_key] = 1
        if call.pay_willing:
            pay_willing_key = "is_pw%s" % call.pay_willing
            result[pay_willing_key] = 1
        if call.pay_ability:
            pay_ability_key = "is_pa%s" % call.pay_ability
            result[pay_ability_key] = 1
        # 计算下p次数
        if call.promised_date:
            result["od_ptp_cnt"] += 1
            KP_key = "%s-%s" % (call.created_at.strftime("%Y%m%d%H%M%S"),
                                call.promised_date.strftime("%Y%m%d%H%M%S"))
            for repayment in repayment_logs:
                if KP_key in overdue_ptp_KP_dict:
                    break
                else:
                    call_promised_date = datetime.datetime.strptime(
                        call.promised_date.strftime("%Y-%m-%d 23:59:59"),
                        "%Y-%m-%d %H:%M:%S")
                    if (call.created_at <= repayment.repay_at and
                            repayment.repay_at <= call_promised_date):
                        overdue_ptp_KP_dict[KP_key] = 1
                        result["od_kp_cnt"] += 1
        # 如果创建时间小于等于完成时间，并且before_finish_last_contact为空，
        # 说明是还款前最后一次接通的通话是否是本人
        if not (is_before_finish_last_contact and
                call.created_at <= application.finished_at and
                call.phone_status == 4):
            is_before_finish_last_contact = True
            if call.real_relationship == 1:
                result["is_lsbr_bff"] = 1
            else:
                result["is_lsbr_bff"] = 0
    # 如果没有call_actions,要看ivr的值，ivr表示没进bomber前给本人大的电话
    if "is_lsbr_bff" not in result:
        result["is_lsbr_bff"] = is_lsbr_bff
    return result


# 计算拨打本人电话数和ec电话数
def calc_newcdr_self_and_ec_num(application=None):
    result = {"br_cacnt": 0, "ec_cacnt": 0}
    if not application:
        logging.info(
            "calc_newcdr_self_and_ec_num is defeated, application is none")
        return result
    # 获取处理后的手机号
    self_number, ec_number = handle_contact_num(application)
    if not any((self_number, ec_number)):
        logging.info(
            "get user_id=:%s contact number is null" % application.user_id)
        return result
    number = self_number + ec_number
    # 获取newCdr中的通话记录
    newcdrs = NewCdrR.select(NewCdrR.callto).where(
        NewCdrR.loanid == str(application.id),
        NewCdrR.callto << number)
    for newcdr in newcdrs:
        if newcdr.callto in self_number:
            result["br_cacnt"] += 1
        else:
            result["ec_cacnt"] += 1
    return result


# 处理得到的手机号NewCdrR中的手机号加0
def handle_contact_num(application=None):
    if not application:
        logging.info("handle_contact_num is defeated, application is none")
        return False, False
    # 获取符合条件的电话，本人(relationship=0),ec=(relationship=1,source=ec)
    contacts = (Contact
                .select(Contact.number, Contact.relationship)
                .where(Contact.user_id == application.user_id,
                       (Contact.relationship == 0) |
                       (Contact.relationship == 1 & Contact.source == 'ec'))
                )
    self_number, ec_number = [], []
    # 处理电话
    for call in contacts:
        # 去除手机号为空的情况
        if not call.number:
            continue
        c_numbers = call.number.split(",")
        for c in c_numbers:
            new_c = c.replace("+62", "")
            new_c = new_c.replace("-", "")
            # NewCdrR中的手机号加0
            new_c = "0{}".format(new_c)
            if call.relationship == 0:
                self_number.append(new_c)
            else:
                ec_number.append(new_c)
    # 两个手机号列表去重
    self_number = list(set(self_number))
    ec_number = list(set(ec_number))
    return self_number, ec_number


# 获取还款信息
def get_repayment_log(appllcation=None):
    if not appllcation:
        logging.info("calc_repayment_log is defeated, application is none")
        return False
    repayment_logs = RepaymentLogR.select().where(
        RepaymentLogR.application_id == appllcation.id)
    # 18年10月以前的数据有重复，key=》appllicaiton_id-repay_at
    repayment_logs = {"{}-{}".format(r.application_id, r.repay_at.strftime(
        "%Y-%m-%d %H:%M:%S")): r for r in repayment_logs}
    repayment_logs = repayment_logs.values()
    return repayment_logs


# 催收ivr是否接通,如果未进bomber之前也要获取到还款前三天和前七天是否接通本人
def calc_is_get_through_auto_ivr(application_id=None, finished_at=None):
    result = {"is_ivrjt": 1,
              "is_brjt_bf3laf": 0,
              "is_brjt_bf7laf": 0
              }
    if not application_id:
        logging.info(
            "calc_is_get_through_auto_ivr is defeated, application is none")
        return result
    auto_ivr_actions = AutoIVRActions.select().where(
        AutoIVRActions.loanid == int(application_id),
        AutoIVRActions.callstate == 1)
    if finished_at:
        if isinstance(finished_at, str):
            finished_at = datetime.datetime.strptime(finished_at,
                                                     "%Y-%m-%d %H:%M:%S")
        finished_three_days = finished_at + datetime.timedelta(days=-3)
        finished_seven_days = finished_at + datetime.timedelta(days=-7)
        for auto_ivr in auto_ivr_actions:
            if auto_ivr.created_at >= finished_seven_days:
                result["is_brjt_bf7laf"] = 1
            if auto_ivr.created_at >= finished_three_days:
                result["is_brjt_bf3laf"] = 1
                break
    if not auto_ivr_actions:
        result["is_ivrjt"] = 0
    return result
