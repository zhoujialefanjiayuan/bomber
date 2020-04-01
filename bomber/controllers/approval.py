from datetime import datetime, date

import logging
from bottle import get, post, request, route, abort
from decimal import Decimal
from peewee import JOIN_LEFT_OUTER

from bomber.api import GoldenEye, BillService
from bomber.constant_mapping import (
    ApplicationStatus,
    ApplicationType,
    ApprovalStatus,
    InboxCategory,
)
from bomber.db import db
from bomber.models import (
    Application,
    Escalation,
    RejectList,
    Discount,
    Transfer,
    Inbox,
)
from bomber.plugins import page_plugin
from bomber.serializers import (
    discount_application_serializer,
    transfer_application_serializer,
    reject_application_serializer,
    reject_list_serializer,
    escalation_serializer,
    transfer_serializer,
    discount_serializer,
)
from bomber.sns import send_to_bus, MessageAction
from bomber.utils import (
    get_cycle_by_overdue_days,
    get_permission,
    utc_datetime
)
from bomber.validator import (
    review_discount_validator,
    review_transfer_validator,
    review_reject_validator,
    discount_validator,
    transfer_validator,
    reject_validator,
)
from bomber.models_readonly import OverdueBillR
from bomber.controllers.asserts import late_fee_limit


@get('/api/v1/applications/<app_id:int>/escalations', apply=[page_plugin])
def get_escalations(bomber, application):
    escalations = Escalation.filter(
        Escalation.application == application.id,
    )
    return escalations, escalation_serializer


@get('/api/v1/applications/<app_id:int>/transfers', apply=[page_plugin])
def get_transfers(bomber, application):
    transfers = Transfer.filter(
        Transfer.application == application.id,
    ).order_by(-Transfer.created_at)
    return transfers, transfer_serializer


@get('/api/v1/transfers/pending')
def get_pending_transfers(bomber):
    transfers = Transfer.select().join(Application).where(
        Transfer.status == ApprovalStatus.PENDING.value
    ).order_by(-Transfer.created_at)
    data = BillService().get_base_applications_v2(transfers,
                                                  transfer_application_serializer,
                                                  paginate=True)
    result = wapper_get_base_applications_v2(data=data, paginate=True)
    return result


@post('/api/v1/transfers')
def add_transfers(bomber):
    form = transfer_validator(request.json)

    application_list = form.get('application_list')
    applications = Application.filter(
        Application.id << application_list,
        Application.status == ApplicationStatus.PROCESSING.value,
    )
    exist_transfer = Transfer.select().where(
        Transfer.status == ApprovalStatus.PENDING.value,
    )
    processing_app_list = {t.application_id for t in exist_transfer}
    transfers = []
    with db.atomic():
        for app in applications:
            if app.latest_bomber_id != bomber.id:
                abort(403, 'permission denied')
            if app.latest_bomber_id == form['transfer_to']:
                abort(400, 'latest bomber equals to transfer_to')

            if app.status in [ApplicationStatus.REPAID.value,
                              ApplicationStatus.BAD_DEBT.value]:
                abort(400, 'application already static')
            if app.id in processing_app_list:
                abort(400, 'application %s transfer '
                           'already in process' % app.id)

            transfers.append(Transfer.create(
                application=app.id,
                operator=bomber.id,
                current_bomber=app.latest_bomber,
                transfer_to=form['transfer_to'],
                reason=form['reason'],
            ))

    return transfer_serializer.dump(transfers, many=True).data


@route('/api/v1/transfers', method='PATCH')
def review_transfer(bomber):
    form = review_transfer_validator(request.json)
    transfer_list = form.get('transfer_list')

    transfers = (Transfer
                 .select(Transfer, Application)
                 .join(Application)
                 .where(Transfer.id << transfer_list,
                        Transfer.status == ApprovalStatus.PENDING.value,
                        Application.cycle <<
                        ([bomber.role.cycle]
                         if bomber.role.cycle else [1, 2, 3, 4])))

    updated_transfers = []
    with db.atomic():
        for transfer in transfers:
            transfer.status = form['status']
            transfer.reviewer = bomber.id
            transfer.reviewed_at = datetime.now()
            transfer.save()
            updated_transfers.append(transfer)
            if form['status'] == ApprovalStatus.REJECTED.value:
                continue
            app = transfer.application
            if app.status in [ApplicationStatus.REPAID.value,
                              ApplicationStatus.BAD_DEBT.value]:
                continue
            if app.latest_bomber == transfer.transfer_to:
                continue
            if app.cycle != transfer.transfer_to.role.cycle:
                abort(400, 'transfer to employee cycle not match')

            app.last_bomber = app.latest_bomber
            app.latest_bomber = transfer.transfer_to
            # transfer in 也算作 claimed
            app.claimed_at = datetime.now()
            app.save()

            if not app.latest_bomber_id:
                return
            #TODO  转单之后 出案和入案

            Inbox.create(
                title='application %s transfer approved' % app.external_id,
                content='application %s transfer approved' % app.external_id,
                receiver=transfer.operator_id,
                category=InboxCategory.APPROVED.value,
            )
            Inbox.create(
                title='application %s transferred in' % app.external_id,
                content='application %s transferred in' % app.external_id,
                receiver=app.latest_bomber_id,
                category=InboxCategory.TRANSFERRED_IN.value,
            )

    return transfer_serializer.dump(updated_transfers, many=True).data


review_transfer.permission = get_permission('approval', 'review_transfer')


@get('/api/v1/applications/<app_id:int>/rejects', apply=[page_plugin])
def get_reject_history(bomber, application):
    rejects = RejectList.filter(
        RejectList.application == application.id,
    ).order_by(-RejectList.created_at)
    return rejects, reject_list_serializer


@get('/api/v1/rejects/pending')
def get_pending_reject(bomber):
    rejects = RejectList.select().join(Application).where(
        RejectList.status == ApprovalStatus.PENDING.value
    ).order_by(-RejectList.created_at)

    data = BillService().get_base_applications_v2(rejects,
                                                  reject_application_serializer,
                                                  paginate=True)
    result = wapper_get_base_applications_v2(data=data, paginate=True)
    return result


@post('/api/v1/application/<app_id:int>/reject-lists')
def add_reject_list(bomber, application):
    form = reject_validator(request.json)
    if application.is_rejected:
        abort(400, 'application is already rejected')
    if application.status == ApplicationStatus.REPAID.value:
        abort(400, 'application is already cleared')

    reject = RejectList.filter(
        RejectList.application == application.id,
        RejectList.status == ApprovalStatus.PENDING.value,
    )
    if reject.exists():
        abort(400, 'application is already in process')

    reject = RejectList.create(
        application=application.id,
        operator=bomber.id,
        reason=form['reason'],
    )
    return reject_list_serializer.dump(reject).data


@route('/api/v1/reject-lists', method='PATCH')
def review_reject(bomber):
    form = review_reject_validator(request.json)
    reject_list = form.get('reject_list')

    reject_lists = (RejectList
                    .select(RejectList, Application)
                    .join(Application)
                    .where(RejectList.id << reject_list,
                           RejectList.status == ApprovalStatus.PENDING.value))

    updated_rejects = []
    with db.atomic():
        for reject in reject_lists:
            reject.status = form['status']
            reject.reviewer = bomber.id
            reject.reviewed_at = datetime.now()
            reject.save()
            updated_rejects.append(reject)
            if form['status'] == ApprovalStatus.REJECTED.value:
                continue
            app = reject.application
            app.is_rejected = True
            app.save()

            # 向golden_eye服务请求加入黑名单
            golden_eye = GoldenEye().post(
                '/applications/%s/blacklist-reject' % app.external_id
            )
            if not golden_eye.ok:
                logging.error('application %s add reject list error', app.id)
                abort(500, 'add reject list error')

            if not app.latest_bomber_id:
                return
            Inbox.create(
                title='application %s reject approved' % app.external_id,
                content='application %s reject approved' % app.id,
                receiver=app.latest_bomber_id,
                category=InboxCategory.APPROVED.value,
            )

    return reject_list_serializer.dump(updated_rejects, many=True).data


review_reject.permission = get_permission('approval', 'review_reject')


@get('/api/v1/applications/<app_id:int>/discounts', apply=[page_plugin])
def get_discounts(bomber, application):
    discounts = Discount.filter(
        Discount.application == application.id,
    ).order_by(-Discount.created_at)
    return discounts, discount_serializer


@get('/api/v1/discounts/history')
def get_pending_discounts(bomber):
    discounts = Discount.select().join(Application).where(
        Discount.status << ApprovalStatus.review_values()
    ).order_by(Discount.id.desc())
    data = BillService().get_base_applications_v2(discounts,
                                                  discount_application_serializer,
                                                  paginate=True)
    result = wapper_get_base_applications_v2(data=data, paginate=True)
    return result


@get('/api/v1/discounts/pending')
def get_pending_discounts(bomber):
    discounts = Discount.select().join(Application).where(
        Discount.status == ApprovalStatus.PENDING.value
    ).order_by(-Discount.created_at)
    data = BillService().get_base_applications_v2(discounts,
                                                  discount_application_serializer,
                                                  paginate=True)
    result = wapper_get_base_applications_v2(data=data, paginate=True)
    return result



@post('/api/v1/application/<app_id:int>/discount')
def add_discount_list(bomber, application):
    form = discount_validator(request.json)
    if form['discount_to'] < 0:
        abort(400, 'invalid discount amount')

    if form['effective_to'] < date.today():
        abort(400, 'invalid effective time')

    # 如果是分期催收单申请折扣，一定到指定哪一期
    if application.type == ApplicationType.CASH_LOAN_STAGING.value:
        if not form.get("overdue_bill_id"):
            abort(400, 'missing a parameter')

    if application.status == ApplicationStatus.REPAID.value:
        abort(400, 'application already cleared')

    is_exists = Discount.filter(
        Discount.application == application.id,
        Discount.status == ApprovalStatus.PENDING.value,
    )
    if application.type == ApplicationType.CASH_LOAN_STAGING.value:
        is_exists = is_exists.filter(Discount.overdue_bill_id ==
                                     form['overdue_bill_id'])
    if is_exists.exists():
        abort(400, 'application discount is already in process')

    result, message = check_discount(application, form)
    if not result:
        abort(400, message)
    overdue_bill_id = form.get("overdue_bill_id")
    overdue_bill_id = overdue_bill_id if overdue_bill_id else None
    periods = form['periods'] if form.get("periods") else None
    create_param = {
        "application": application.id,
        "operator": bomber.id,
        "discount_to": form['discount_to'],
        "effective_to": form["effective_to"],
        "reason": form["reason"],
        "overdue_bill_id": overdue_bill_id,
        "periods": periods
    }
    discount = Discount.create(**create_param)
    return discount_serializer.dump(discount).data

# 检测减免金额是否合适
def check_discount(application, form=None):
    if application.type == ApplicationType.CASH_LOAN.value:
        cycle = application.cycle
        bill_dict = BillService().bill_dict(application_id=application.external_id)
    else:
        overdue_bill = (OverdueBillR.select()
                        .where(OverdueBillR.id == form["overdue_bill_id"],
                               OverdueBillR.collection_id == application.id,
                               OverdueBillR.status !=
                               ApplicationStatus.REPAID.value)
                        .first())
        if not overdue_bill:
            return False, 'application sub_bill is repaid'
        bill_sub_ids = [overdue_bill.sub_bill_id]
        sub_bill_list = BillService().sub_bill_list(bill_sub_ids=bill_sub_ids)
        if not sub_bill_list:
            return False, 'application sub_bill is not exists'
        bill_dict = sub_bill_list[0]
        cycle = get_cycle_by_overdue_days(overdue_bill.overdue_days)

    late_fee_limit_rate = late_fee_limit(cycle)
    if (form['discount_to'] + bill_dict['principal_paid'] +
            bill_dict['late_fee_paid'] < (bill_dict['amount'] +
                                          bill_dict['late_fee'] *
                                          (1 - late_fee_limit_rate))):
        return False, 'exceeding the late fee reduction limit'
    return True, ''


@route('/api/v1/discount', method='PATCH')
def review_discount(bomber):
    form = review_discount_validator(request.json)
    discount_list = form.get('discount_list')

    discounts = (Discount
                 .select(Discount, Application)
                 .join(Application)
                 .where(Discount.id << discount_list,
                        Discount.status == ApprovalStatus.PENDING.value))

    updated_discounts = []
    with db.atomic():
        for discount in discounts:
            discount.status = form['status']
            discount.reviewer = bomber.id
            discount.reviewed_at = datetime.now()
            discount.save()

            if form['status'] == ApprovalStatus.REJECTED.value:
                continue
            app = discount.application
            if app.status not in (ApplicationStatus.UNCLAIMED.value,
                                  ApplicationStatus.PROCESSING.value,
                                  ApplicationStatus.AB_TEST.value):
                abort(400, 'application %s status invalid' % app.external_id)
            # 向repayment服务请求减免
            start_date = utc_datetime(
                discount.created_at.strftime('%Y-%m-%d %H-%M'))
            param = {"application_id": app.external_id,
                     "amount": str(discount.discount_to),
                     "due_date": discount.effective_to.strftime('%Y-%m-%d'),
                     "start_date": start_date}
            if app.type == ApplicationType.CASH_LOAN_STAGING.value:
                # 获取折扣对应的子账单
                overdue_bill = (OverdueBillR.select()
                                .where(OverdueBillR.id == discount.overdue_bill_id)
                                .first())
                if not overdue_bill:
                    abort(400, 'application overdue_bill is not exists')
                if overdue_bill.status == ApplicationStatus.REPAID.value:
                    abort(400, 'application overdue_bill is repaid')
                param["bill_sub_id"] = overdue_bill.sub_bill_id

            BillService().bill_relief(**param)

            if app.latest_bomber_id:
                Inbox.create(
                    title='application %s discounts approved' % app.external_id,
                    content='application %s discounts approved' % app.external_id,
                    receiver=app.latest_bomber_id,
                    category=InboxCategory.APPROVED.value,
                )

            send_to_bus(
                MessageAction.BOMBER_DISCOUNT_APPROVED, {
                    'id': app.id,
                    'msg_type': 'DISCOUNT_APPROVED',
                    'discount_to': 'Rp{:,}'.format(discount.discount_to),
                    'effective_to': discount.effective_to.strftime('%d-%m-%Y'),
                }
            )

    return discount_serializer.dump(updated_discounts, many=True).data


# 对get_base_application_v2的结果进行封装,
# 主要是处理分期的件，获取金额和对应的期数
def wapper_get_base_applications_v2(data, paginate=True):
    fields_list = ['late_fee_rate',
                   'late_fee_initial',
                   'late_fee',
                   'principal_paid',
                   'late_fee_paid',
                   'repaid',
                   'unpaid',
                   'interest',
                   'disbursed_date',
                   'amount_net',
                   'amount',
                   'overdue_days']
    result = data
    if paginate:
        result = data["result"]
    if not result:
        return data
    overdue_ids = {}
    for app in result:
        if app["application"]["type"] == ApplicationType.CASH_LOAN_STAGING.value:
            overdue_ids[app["id"]] = app["overdue_bill_id"]
    if not overdue_ids:
        return data
    ids = list(overdue_ids.values())
    overdue_bills = (OverdueBillR.select()
                     .where(OverdueBillR.id << ids))
    overdue_bill_dict = {b.id:b for b in overdue_bills}
    sub_ids = [b.sub_bill_id for b in overdue_bills]
    try:
        sub_bills = BillService().sub_bill_list(bill_sub_ids = sub_ids)
    except Exception as e:
        logging.info("wapper_get_base_applications_v2 error:%s"%str(e))
        return data
    sub_bill_dict = {s["id"]:s for s in sub_bills}
    overdue_sub = {}
    for k,v in overdue_bill_dict.items():
        sb = sub_bill_dict.get(v.sub_bill_id,{})
        overdue_sub[v.id] = sb
    for app in result:
        sb = overdue_sub.get(app.get("overdue_bill_id"), {})
        fields_dict = {}
        for k,v in sb.items():
            if k in fields_list:
                fields_dict[k] = str(v) if isinstance(v, Decimal) else v
        app["application"].update(fields_dict)
    return data



