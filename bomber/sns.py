import json
from enum import Enum

import boto3
import logging
from bottle import default_app


app = default_app()


class MessageAction(Enum):
    USER_CREATED = 'USER_CREATED'
    USER_LOGGED_IN = 'USER_LOGGED_IN'
    # 用户修改手机号
    USER_UPDATE_PHONE = 'USER_UPDATE_PHONE'
    APPLICATION_CREATED = 'APPLICATION_CREATED'
    APPLICATION_APPROVED = 'APPLICATION_APPROVED'
    APPLICATION_REJECTED = 'APPLICATION_REJECTED'
    APPLICATION_CANCELED = 'APPLICATION_CANCELED'
    APPLICATION_OVERDUE = 'APPLICATION_OVERDUE'
    BILL_CREATED = 'BILL_CREATED'
    BILL_PAID = 'BILL_PAID'
    BILL_CLEARED = 'BILL_CLEARED'
    BILL_CLEARED_BEFORE_CONFIRM = 'BILL_CLEARED_BEFORE_CONFIRM'
    BILL_REVOKE = 'BILL_REVOKE'
    BANKCARD_HOLDER_CHECKED = 'BANKCARD_HOLDER_CHECKED'
    VIRTUAL_ACCOUNT_CREATED = 'VIRTUAL_ACCOUNT_CREATED'
    REMIT_SUCCESS = 'REMIT_SUCCESS'
    REMIT_FAILED = 'REMIT_FAILED'
    REMIT_ING = 'REMIT_ING'
    SELFIE_RECOGNITION = 'SELFIE_RECOGNITION'
    EKTP_QUERIED = 'EKTP_QUERIED'
    EKTP_RESULT = 'EKTP_RESULT'
    BILL_RELIEF = 'BILL_RELIEF'
    OVERDUE_BILL_SYNC = 'OVERDUE_BILL_SYNC'
    BOMBER_CALC_OVERDUE_DAYS = 'BOMBER_CALC_OVERDUE_DAYS'
    BOMBER_AUTOMATIC_ESCALATION = 'BOMBER_AUTOMATIC_ESCALATION'
    BOMBER_CALC_SUMMARY = 'BOMBER_CALC_SUMMARY'
    BOMBER_CALC_SUMMARY2 = 'BOMBER_CALC_SUMMARY2'
    BOMBER_SYNC_CONTACTS = 'BOMBER_SYNC_CONTACTS'
    APPLICATION_BOMBER = 'APPLICATION_BOMBER'
    BOMBER_AUTO_SMS = 'BOMBER_AUTO_SMS'
    BOMBER_REMIND_PROMISE = 'BOMBER_REMIND_PROMISE'
    BOMBER_AUTO_CALL_LIST = 'BOMBER_AUTO_CALL_LIST'
    BOMBER_AUTO_CALL_CONTACT = 'BOMBER_AUTO_CALL_CONTACT'
    BOMBER_DISCOUNT_APPROVED = 'BOMBER_DISCOUNT_APPROVED'
    BOMBER_SCAVENGER = 'BOMBER_SCAVENGER'
    BOMBER_CLEAR_OVERDUE_PTP = 'BOMBER_CLEAR_OVERDUE_PTP'
    BOMBER_HEALTH_CHECK = 'BOMBER_HEALTH_CHECK'
    GET_SMS_STATUS = 'GET_SMS_STATUS'
    BOMBER_TMP_TEST = 'BOMBER_TMP_TEST'
    BOMBER_AUTO_CALL_LIST_RECORD = 'BOMBER_AUTO_CALL_LIST_RECORD'
    BOMBER_AUTO_MESSAGE_DAILY = 'BOMBER_AUTO_MESSAGE_DAILY'
    BOMBER_MANUAL_CALL_LIST = 'BOMBER_MANUAL_CALL_LIST'
    # report
    REPORT_BOMBER_COLLECTION = 'REPORT_BOMBER_COLLECTION'
    # app合并
    APP_MERGE = 'APP_MERGE'
    GET_IVR = 'GET_IVR'
    OLD_LOAN_APPLICATION = 'OLD_LOAN_APPLICATION'
    UPDATE_OLD_LOAN_APPLICATION = 'UPDATE_OLD_LOAN_APPLICATION'
    # 待催维度的recover_rate,每周执行
    RECOVER_RATE_WEEK_MONEY = 'RECOVER_RATE_WEEK_MONEY'
    # 入催维度的recover_rate，每天执行
    RECOVER_RATE_WEEK_MONEY_INTO = 'RECOVER_RATE_WEEK_MONEY_INTO'
    SUMMARY_CREATE = 'SUMMARY_CREATE'
    SUMMARY_NEW = 'SUMMARY_NEW'
    UPDATE_SUMMARY_NEW = 'UPDATE_SUMMARY_NEW'
    SUMMARY_NEW_CYCLE = 'SUMMARY_NEW_CYCLE'
    MODIFY_BILL = 'MODIFY_BILL'
    CONTACT_FROM_TOTAL = 'CONTACT_FROM_TOTAL'
    IMPORT_CONTACT_TO_MON = 'IMPORT_CONTACT_TO_MON'
    DROP_DUPLICATED_CONTACT = 'DROP_DUPLICATED_CONTACT'
    BOMBER_CALC_OVERDUE_DAYS_OVER = 'BOMBER_CALC_OVERDUE_DAYS_OVER'
    # bomber人员变动，进行分件
    BOMBER_CHANGE_DISPATCH_APPS = 'BOMBER_CHANGE_DISPATCH_APPS'
    REPAIR_BOMBER = 'REPAIR_BOMBER'
    UPDATE_BOMBER_FOR_SPECIAL = 'UPDATE_BOMBER_FOR_SPECIAL'
    # 每天上午下午统计员工的下p，打电话，回款等信息
    SUMMARY_DAILY = 'SUMMARY_DAILY'
    # 每个月月底进行重新分件操作
    MONTH_DISPATCH_APP = 'MONTH_DISPATCH_APP'
    # 每天记录催收单信息
    SUMMARY_BOMBER_OVERDUE = 'SUMMARY_BOMBER_OVERDUE'
    # 分期逾期短信提箱
    BOMBER_INSTALMENT_AUTO_MESSAGE_DAILY = 'BOMBER_INSTALMENT_AUTO_MESSAGE_DAILY'
    # 实时统计员工手中的下p件的个数
    BOMBER_PTP_REAL_TIME_SUMMARY = 'BOMBER_PTP_REAL_TIME_SUMMARY'
    # 定时关闭更改员工的自动外呼状态
    BOMBER_TODAY_PTP_FOLLOW_SWITCH_OFF = 'BOMBER_TODAY_PTP_FOLLOW_SWITCH_OFF'
    # 每天8点定时打开催收员自动外呼的状态
    BOMBER_TODAY_PTP_FOLLOW_SWITCH_ON = 'BOMBER_TODAY_PTP_FOLLOW_SWITCH_ON'


def send_to_bus(action, payload):
    arn = app.config['aws.sns.arn_bus']
    return send_to_sns(arn, action, payload)


def send_to_sns(arn, action, payload):
    assert isinstance(action, MessageAction)

    client = boto3.client('sns')

    message = json.dumps({
        'action': action.value,
        'payload': payload,
    })
    logging.info('sns message body: %s', message)

    response = client.publish(
        TopicArn=arn,
        Message=message,
    )

    msg_id = response
    logging.info('sns message id: %s', response['MessageId'])
    return msg_id


def send_to_default_q(action, payload):
    url = app.config['aws.sqs.queue_url']
    return send_to_sqs(url, action, payload)


def send_to_sqs(url, action, payload):
    assert isinstance(action, MessageAction)

    client = boto3.client('sqs')

    message = json.dumps({
        'action': action.value,
        'payload': payload,
    })
    logging.info('send sqs message body: %s', message)

    response = client.send_message(
        QueueUrl=url,
        MessageBody=message,
    )

    msg_id = response
    logging.info('sqs message id: %s', response['MessageId'])
    return msg_id
