from datetime import datetime, date, timedelta
from decimal import Decimal
import logging
import traceback

from bottle import get, route, request, abort, post
from peewee import DoesNotExist, fn, Param, JOIN, SQL, JOIN_LEFT_OUTER

from bomber.controllers.summary import get_paid_total
from bomber.auth import check_api_user
from bomber.api import BillService, Remittance
from bomber.plugins import ip_whitelist_plugin
from bomber.constant_mapping import (
    ApplicationStatus,
    BomberCallSwitch,
    ApplicationType,
    SpecialBomber,
    OldLoanStatus,
    DisAppStatus,
    PartnerType,
    Cycle,
    SIM,
)
from bomber.models import (
    SummaryBomber,
    Application,
    CycleTarget,
    BomberPtp,
    Bomber,
)
from bomber.models_readonly import (
    DispatchAppHistoryR,
    OldLoanApplicationR,
    BombingHistoryR,
    RepaymentLogR,
    ApplicationR,
    CallActionsR,
    OverdueBillR,
    DispatchAppR,
    BomberPtpR,
)
from bomber.serializers import (
    application_mark_serializer,
    application_serializer,
)
from bomber.utils import (
    get_cycle_by_overdue_days,
    get_permission,
    plain_query
)
from bomber.controllers.asserts import late_fee_limit


@get('/api/v1/applications/unclaimed')
def unclaimed(bomber):
    apps = Application.filter(
        Application.status == ApplicationStatus.UNCLAIMED.value,
        Application.latest_bomber >> None,
        Application.cycle <<
        ([bomber.role.cycle] if bomber.role.cycle else Cycle.values()),
    ).order_by(-Application.overdue_days, Application.apply_at)
    data = (BillService()
            .get_base_applications(apps,
                                   application_serializer,
                                   paginate=True))
    return wapper_base_applications(data)


@get('/api/v1/applications/all')
def application_all(bomber):
    args = plain_query()

    apps = Application.select()

    if 'application_id' in args:
        apps = apps.where(Application.external_id == args.application_id)
    if 'user_id' in args:
        apps = apps.where(Application.user_id == args.user_id)
    if 'user_name' in args:
        apps = apps.where(Application.user_name == args.user_name)
    if 'mobile' in args:
        apps = apps.where(Application.user_mobile_no == args.mobile)
    if 'cycle' in args:
        apps = apps.where(Application.cycle == args.cycle)
    if 'status' in args:
        apps = apps.where(Application.status == args.status)

    apps = apps.order_by(-Application.id)

    return (BillService()
            .get_base_applications(apps,
                                   application_serializer,
                                   paginate=True))


application_all.permission = get_permission('application', 'all')


@route('/api/v1/applications/claim', method='PATCH')
def application_claim(bomber):
    form = request.json
    app_ids = form.get('claimed_apps')

    if not isinstance(app_ids, list):
        abort(400, 'invalid data')

    apps = Application.filter(
        Application.id << app_ids,
        Application.status == ApplicationStatus.UNCLAIMED.value,
        Application.latest_bomber >> None,
        Application.cycle <<
        ([bomber.role.cycle] if bomber.role.cycle else Cycle.values()),
    )

    if not apps.exists():
        abort(400, 'application not found')

    for app in apps:
        app.latest_bomber_id = bomber.id
        app.status = ApplicationStatus.PROCESSING.value
        app.claimed_at = datetime.now()
        app.save()

    return BillService().get_base_applications(apps, application_serializer)


@get('/api/v1/applications/bomber/data')
def bomber_data(bomber):
    partner = None
    try:
        partner = bomber.partner_id
    except DoesNotExist:
        # partner not exist
        pass
    target_date = {'amount': 0, 'grade': 0, 'cycle_target': 0, 'target_rate': 0,
                   'new': 0, 'today': 0, 'next_day': 0, 'other': 0, 'follow': 0
                   }
    if bomber.id != SpecialBomber.OLD_APP_BOMBER.value:
        sub1 = (CallActionsR
                .select(CallActionsR.created_at,
                        CallActionsR.application,
                        CallActionsR.bomber_id,
                        CallActionsR.promised_date)
                ).alias('sub1')
        apps = (ApplicationR
                .select(ApplicationR.id,
                        ApplicationR.type,
                        fn.MAX(DispatchAppHistoryR.entry_at)
                        .alias('entry_at'),
                        fn.DATE(ApplicationR.promised_date)
                        .alias('promised_date'),
                        fn.DATE(ApplicationR.follow_up_date)
                        .alias('follow_up_date'))
                .join(DispatchAppHistoryR, JOIN_LEFT_OUTER, on=(
                (ApplicationR.id == DispatchAppHistoryR.application) &
                (ApplicationR.latest_bomber ==
                 DispatchAppHistoryR.bomber_id)))
                .where(ApplicationR.latest_bomber == bomber.id,
                       ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                               ApplicationStatus.AB_TEST.value])
                .group_by(ApplicationR.id))
        if bomber.role.cycle == Cycle.C1A.value:
            apps = (ApplicationR
                    .select(ApplicationR.id,
                            ApplicationR.type,
                            fn.MAX(sub1.c.created_at).alias('entry_at'),
                            fn.DATE(ApplicationR.promised_date)
                            .alias('promised_date'),
                            fn.DATE(ApplicationR.follow_up_date)
                            .alias('follow_up_date'))
                    .join(sub1, JOIN_LEFT_OUTER, on=(
                    (ApplicationR.id == sub1.c.application_id) &
                    (sub1.c.bomber_id == ApplicationR.latest_bomber)))
                    .where(ApplicationR.latest_bomber == bomber.id,
                           ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                                   ApplicationStatus.AB_TEST.value])
                     .group_by(ApplicationR.id))
        today = date.today()
        next_day = today + timedelta(days=1)
        dahr = DispatchAppHistoryR.alias()
        unfollowed = (dahr
                      .select(dahr.application_id)
                      .where(dahr.bomber_id == bomber.id,
                             dahr.entry_at < today,
                             dahr.out_at.is_null(True),
                             ~fn.EXISTS(CallActionsR
                                        .select(Param('1'))
                                        .where(dahr.application_id ==
                                               CallActionsR.application_id,
                                               dahr.bomber_id ==
                                               CallActionsR.bomber_id,
                                               CallActionsR.created_at <
                                               next_day))))
        new_ids = [i.application_id for i in unfollowed]
        for app in apps:
            if app.id in new_ids:
                if bomber.role.cycle == Cycle.C1A.value:
                    continue
                target_date["new"] += 1
            else:
                if ((bomber.role.cycle in
                    (Cycle.C1B.value, Cycle.C2.value, Cycle.C3.value)) or partner):
                    if app.entry_at and app.entry_at.date() == today:
                        target_date['new'] += 1
                if app.promised_date == today:
                    target_date['today'] += 1
                elif app.promised_date == next_day:
                    target_date['next_day'] += 1
                elif app.follow_up_date == today:
                    target_date['follow'] += 1
                else:
                    # c1 和 c2,c3的entry_at的业务含义不同
                    if ((bomber.role.cycle in
                    (Cycle.C1B.value, Cycle.C2.value, Cycle.C3.value)) or partner):
                        if app.entry_at and app.entry_at.date() == today:
                            continue
                        target_date['other'] += 1
                    else:
                        target_date['other'] += 1

        # 得到该催收员当月催回金额，以及排名等信息
        cleared = (SummaryBomber
                   .select(
                        fn.SUM(SummaryBomber.cleared_amount).alias('paid'),
                        SummaryBomber.bomber_id)
                   .where(SummaryBomber.cycle == bomber.role.cycle,
                          SummaryBomber.time >= date.today().strftime('%Y-%m'),
                          SummaryBomber.time < date.today(),
                          SummaryBomber.cycle != SummaryBomber.bomber_id)
                   .group_by(SummaryBomber.bomber_id)
                   .order_by(SQL('paid').desc()))
        for c in cleared:
            if c.bomber_id == bomber.id:
                target_date['grade'] += 1
                target_date['amount'] = round(int(c.paid) / 1000000, 2)
                break
            target_date['grade'] += 1

        # 得到当月的1号\
        paid_amount = 0
        month = today.strftime('%Y-%m') + '-01'
        paid = get_paid_total(month, date.today() + timedelta(days=1))
        for p in paid:
            if p[1] and p[0] == bomber.role.cycle:
                paid_amount = p[1]
        cycle_paid = int(paid_amount) / 1000000

        target_amount = (CycleTarget
                         .filter(CycleTarget.target_month == month,
                                 CycleTarget.cycle == bomber.role.cycle)
                         .first())
        if target_amount:
            amount = target_amount.target_amount
            rate = round((cycle_paid / amount if amount else 0) * 100, 2)
            target_date['target_rate'] = rate
            target_date['cycle_target'] = format(int(amount), ',')
        return {'amount': target_date['amount'], 'grade': target_date['grade'],
                'cycle_target': target_date['cycle_target'],
                'other': target_date['other'], 'follow': target_date['follow'],
                'today': target_date['today'], 'new': target_date['new'],
                'next_day': target_date['next_day'],
                'target_rate': target_date['target_rate']}
    return None


@get('/api/v1/applications/processing')
def processing(bomber):
    args = plain_query()
    prop = args.get('prop')
    order = args.get('order')
    loan_type = args.get('type')
    partner = None
    try:
        partner = bomber.partner_id
    except DoesNotExist:
        # partner not exist
        pass
    old_loan_bomber = SpecialBomber.OLD_APP_BOMBER.value
    today = date.today()
    current_followed = (fn.EXISTS(CallActionsR
                                  .select(Param('1'))
                                  .where(CallActionsR.application_id ==
                                         ApplicationR.id,
                                         CallActionsR.bomber_id == bomber.id,
                                         CallActionsR.created_at > today)))

    if bomber.id == old_loan_bomber:
        apps = (ApplicationR
                .select(ApplicationR, current_followed.alias('cca'))
                .join(OldLoanApplicationR,
                      on=(OldLoanApplicationR.application_id == ApplicationR.id))
                .where(OldLoanApplicationR.bomber_id == old_loan_bomber,
                       OldLoanApplicationR.status ==
                       OldLoanStatus.PROCESSING.value))
    else:
        sub1 = (CallActionsR
                .select(CallActionsR.created_at,
                        CallActionsR.application,
                        CallActionsR.bomber_id,
                        CallActionsR.promised_date)
                ).alias('sub1')

        apps = (ApplicationR.select(
                    ApplicationR.id, ApplicationR.user_id,
                    ApplicationR.user_name,ApplicationR.cycle,
                    ApplicationR.type,
                    ApplicationR.external_id,
                    fn.MAX(DispatchAppHistoryR.entry_at)
                    .alias('entry_at'),
                    fn.MAX(DispatchAppHistoryR.expected_out_time)
                    .alias('expected_out_time'),
                    ApplicationR.overdue_days,
                    ApplicationR.amount_net,
                    fn.Date(ApplicationR.promised_date)
                    .alias('promised_date'),
                    fn.Date(ApplicationR.follow_up_date)
                    .alias('follow_up_date'),
                    ApplicationR.loan_success_times,
                    fn.MAX(sub1.c.created_at).alias('last_bomber_time'),
                    current_followed.alias('cca'))
                .join(sub1, JOIN_LEFT_OUTER, on=(
                     (ApplicationR.id == sub1.c.application_id) &
                     (sub1.c.bomber_id == ApplicationR.latest_bomber)))
                .join(DispatchAppHistoryR, JOIN_LEFT_OUTER, on=(
                     (ApplicationR.id == DispatchAppHistoryR.application) &
                     (ApplicationR.latest_bomber ==
                      DispatchAppHistoryR.bomber_id)))
                .where(
                 ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                         ApplicationStatus.AB_TEST.value],
                 ApplicationR.latest_bomber == bomber.id,
                 ApplicationR.cycle <<
                 ([bomber.role.cycle] if bomber.role.cycle
                     else Cycle.values()),
                ).group_by(ApplicationR.id))
        if bomber.role.cycle == Cycle.C1A.value:

            apps = (ApplicationR.select(
                      ApplicationR.id, ApplicationR.user_id,
                      ApplicationR.user_name, ApplicationR.cycle,
                      ApplicationR.type,
                      ApplicationR.external_id,
                      sub1.c.created_at.alias('entry_at'),
                      ApplicationR.promised_date.alias('expected_out_time'),
                      ApplicationR.overdue_days,
                      ApplicationR.amount_net,
                      fn.Date(ApplicationR.promised_date).alias('promised_date'),
                      fn.Date(ApplicationR.follow_up_date).alias('follow_up_date'),
                      ApplicationR.loan_success_times,
                      fn.MAX(CallActionsR.created_at)
                      .alias('last_bomber_time'),
                      current_followed.alias('cca'))
                    .join(sub1, JOIN_LEFT_OUTER, on=(
                         (ApplicationR.id == sub1.c.application_id) &
                         (sub1.c.bomber_id == ApplicationR.latest_bomber) &
                         (sub1.c.promised_date == ApplicationR.promised_date)))
                    .join(CallActionsR, JOIN_LEFT_OUTER, on=(
                         (ApplicationR.id == CallActionsR.application) &
                         (ApplicationR.latest_bomber == CallActionsR.bomber_id)))
                    .where(
                     ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                             ApplicationStatus.AB_TEST.value],
                     ApplicationR.latest_bomber == bomber.id,
                     ApplicationR.cycle <<
                     ([bomber.role.cycle] if bomber.role.cycle
                         else Cycle.values()),
                    ).group_by(ApplicationR.id))

        next_day = today + timedelta(days=1)
        tn_days = [today, next_day]
        dahr = DispatchAppHistoryR.alias()
        unfollowed = (dahr
                      .select(dahr.application_id)
                      .where(dahr.bomber_id == bomber.id,
                             dahr.entry_at < today,
                             dahr.out_at.is_null(True),
                             ~fn.EXISTS(CallActionsR
                                        .select(Param('1'))
                                        .where(dahr.application_id ==
                                               CallActionsR.application_id,
                                               dahr.bomber_id ==
                                               CallActionsR.bomber_id,
                                               CallActionsR.created_at <
                                               next_day))))
        if loan_type == 'new':
            if bomber.role.cycle == Cycle.C1A.value:
                return {"page": 1,
                        "page_size": 20,
                        "result": [],
                        "total_count": 0,
                        "total_page": 1}
            apps1 = apps.where(fn.Date(DispatchAppHistoryR.entry_at)
                               == today)
            apps1_ids = [i.id for i in apps1]
            apps2_ids = [i.application_id for i in unfollowed]
            apps = apps.where(ApplicationR.id.in_(apps1_ids + apps2_ids))

        elif loan_type == 'today':
            apps = apps.where(ApplicationR.promised_date == today)
        elif loan_type == 'nextday':
            apps = apps.where(ApplicationR.promised_date == next_day)
        elif loan_type == 'other':
            apps2_ids = [i.application_id for i in unfollowed]
            if (bomber.role.cycle in
                    (Cycle.C1B.value, Cycle.C2.value, Cycle.C3.value) or partner):
                apps = apps.where(
                           fn.Date(ApplicationR.follow_up_date) != today,
                           fn.Date(DispatchAppHistoryR.entry_at) != today,
                           (ApplicationR.promised_date.is_null(True) |
                            ApplicationR.promised_date.not_in(tn_days)
                            )
                )
                if apps2_ids:
                    apps = apps.where(ApplicationR.id.not_in(apps2_ids))
            else:
                apps = apps.where(
                           fn.Date(ApplicationR.follow_up_date) != today,
                           (ApplicationR.promised_date.is_null(True)) |
                           ApplicationR.promised_date.not_in(tn_days))
                if apps2_ids:
                    apps = apps.where(ApplicationR.id.not_in(apps2_ids))

        elif loan_type == 'follow':
            apps = apps.where(fn.Date(ApplicationR.follow_up_date) == today,
                              (ApplicationR.promised_date.not_in(tn_days) |
                               ApplicationR.promised_date.is_null(True)))

    if 'application_id' in args:
        apps = apps.where(ApplicationR.external_id == args.application_id)
    if 'user_id' in args:
        apps = apps.where(ApplicationR.user_id == args.user_id)
    if 'user_name' in args:
        apps = apps.where(ApplicationR.user_name == args.user_name)
    if 'mobile' in args:
        apps = apps.where(ApplicationR.user_mobile_no == args.mobile)
    if 'overdue_days' in args:
        apps = apps.where(ApplicationR.overdue_days == args.overdue_days)
    if 'promised' in args:
        apps = apps.where(
            ApplicationR.promised_date.is_null(
                False if args.promised == '1' else True
            )
        )
    if 'last_collector_id' in args:
        apps = apps.where(
            ApplicationR.last_bomber == args.last_collector_id
        )

    if order and prop:
        if order == 'descending':
            apps = apps.order_by(SQL('%s desc'%prop))
        else:
            apps = apps.order_by(SQL('%s asc'%prop))
    else:
        apps = apps.order_by(SQL('overdue_days desc'))

    # 旧件逾期催收
    if bomber.id == old_loan_bomber:
        data = BillService().get_base_applications(apps,
                                                   application_mark_serializer,
                                                   paginate=True)
        return wapper_base_applications(data)
    data = BillService().get_base_applications(apps,
                                               application_mark_serializer,
                                               paginate=True)
    return wapper_base_applications(data)


# 如果有分期的账单，重新计算分期的支付和未支付的金额
def wapper_base_applications(data):
    apps = data.get("result")
    if not apps:
        return data
    apps_dict = {a["id"]:a for a in apps}
    aids = list(apps_dict.keys())
    # 获取催收单中所有的子账单
    overdue_bills = (OverdueBillR
                     .select(OverdueBillR.collection_id,
                             OverdueBillR.sub_bill_id)
                     .where(OverdueBillR.collection_id << aids,
                            OverdueBillR.periods.is_null(False),
                            OverdueBillR.no_active == 0))
    sub_bill_ids = [ob.sub_bill_id for ob in overdue_bills]
    if not sub_bill_ids:
        return data
    sub_bill_data = BillService().sub_bill_list(bill_sub_ids=sub_bill_ids)
    sub_bill_dict = {s["id"]:s for s in sub_bill_data}
    new_paid = {}
    # 计算新的已付和未付
    for ob in overdue_bills:
        bill = sub_bill_dict[ob.sub_bill_id]
        if ob.collection_id in new_paid:
            new_paid[ob.collection_id]["repaid"] += float(bill["repaid"])
            new_paid[ob.collection_id]["unpaid"] += float(bill["unpaid"])
        else:
            new_paid[ob.collection_id] = {
                "repaid": float(bill["repaid"]),
                "unpaid": float(bill["unpaid"])}
    for aid,app in apps_dict.items():
        paid = new_paid.get(int(aid), {})
        app.update(paid)
    return data


@get('/api/v1/applications/repaid')
def repaid(bomber):
    args = plain_query()

    partner = None
    try:
        partner = bomber.partner_id
    except DoesNotExist:
        # partner not exist
        pass

    special_bomber = SpecialBomber.OLD_APP_BOMBER.value
    if partner and partner == PartnerType.INDOJAYA_C3.value:
        sub_query = (CallActionsR.select(Param('1'))
                     .where(RepaymentLogR.current_bomber ==
                            CallActionsR.bomber_id,
                            RepaymentLogR.application ==
                            CallActionsR.application,
                            RepaymentLogR.repay_at >
                            CallActionsR.created_at))
        apps = (RepaymentLogR
                .select(
                    RepaymentLogR.repay_at.alias('repay_at'),
                    (RepaymentLogR.principal_part +
                     RepaymentLogR.late_fee_part).alias('repaid'),
                    RepaymentLogR.cycle.alias('cycle'),
                    RepaymentLogR.application.alias('id'),
                    fn.MAX(ApplicationR.user_id).alias('user_id'),
                    fn.MAX(ApplicationR.loan_success_times)
                    .alias('loan_success_times'),
                    fn.MAX(ApplicationR.external_id).alias('external_id'),
                    fn.MAX(ApplicationR.user_mobile_no).alias('user_mobile_no')
                )
                .join(ApplicationR, JOIN_LEFT_OUTER,
                      ApplicationR.id == RepaymentLogR.application)
                .where(fn.EXISTS(sub_query),
                       RepaymentLogR.current_bomber == bomber.id,
                       ((RepaymentLogR.principal_part > 0) |
                        (RepaymentLogR.late_fee_part > 0)))
                .group_by(RepaymentLogR.repay_at,
                          RepaymentLogR.application))
    else:
        if bomber.id == special_bomber:
            sub_query = (BombingHistoryR.select(Param('1'))
                         .where(OldLoanApplicationR.bomber_id ==
                                BombingHistoryR.bomber_id,
                                OldLoanApplicationR.application_id ==
                                BombingHistoryR.application_id,
                                BombingHistoryR.promised_date.is_null(False)))
            apps = (RepaymentLogR
                    .select(RepaymentLogR.principal_part,
                            RepaymentLogR.late_fee_part,
                            RepaymentLogR.application.alias('id'),
                            RepaymentLogR.application,
                            RepaymentLogR.cycle.alias('cycle'),
                            (RepaymentLogR.principal_part +
                             RepaymentLogR.late_fee_part).alias('repaid'))
                    .join(OldLoanApplicationR,
                          JOIN.INNER,
                          on=((OldLoanApplicationR.application_id ==
                               RepaymentLogR.application_id) &
                              (RepaymentLogR.repay_at >
                               OldLoanApplicationR.start_date) &
                              (RepaymentLogR.repay_at <
                               OldLoanApplicationR.end_date)))
                    .where(fn.EXISTS(sub_query))
                    .order_by(-RepaymentLogR.repay_at))
            return (BillService().query_page(apps,
                                             application_serializer,
                                             paginate=True))
        else:
            cycle = bomber.role.cycle
            apps = (RepaymentLogR
                    .select(
                        RepaymentLogR.repay_at.alias('repay_at'),
                        (RepaymentLogR.principal_part +
                         RepaymentLogR.late_fee_part).alias('repaid'),
                        RepaymentLogR.cycle.alias('cycle'),
                        RepaymentLogR.application.alias('id'),
                        fn.MAX(ApplicationR.user_id).alias('user_id'),
                        fn.MAX(ApplicationR.loan_success_times)
                        .alias('loan_success_times'),
                        fn.MAX(ApplicationR.external_id).alias('external_id'),
                        fn.MAX(ApplicationR.user_mobile_no)
                        .alias('user_mobile_no'))
                    .join(ApplicationR, JOIN_LEFT_OUTER,
                          ApplicationR.id == RepaymentLogR.application)
                    .where(RepaymentLogR.current_bomber == bomber.id,
                           ((RepaymentLogR.principal_part > 0) |
                            (RepaymentLogR.late_fee_part > 0)))
                    .group_by(RepaymentLogR.repay_at,
                              RepaymentLogR.application))

            if cycle in [Cycle.C2.value, Cycle.C3.value] and not partner:
                sub_query = (BombingHistoryR.select(Param('1'))
                             .where(RepaymentLogR.current_bomber ==
                                    BombingHistoryR.bomber,
                                    RepaymentLogR.application ==
                                    BombingHistoryR.application,
                                    ((BombingHistoryR.promised_date
                                      .is_null(False)) |
                                     (BombingHistoryR.promised_amount
                                      .is_null(False))),
                                    RepaymentLogR.repay_at >
                                    BombingHistoryR.created_at))
                apps = (RepaymentLogR
                        .select(
                            RepaymentLogR.repay_at.alias('repay_at'),
                            (RepaymentLogR.principal_part +
                             RepaymentLogR.late_fee_part).alias('repaid'),
                            RepaymentLogR.cycle.alias('cycle'),
                            RepaymentLogR.application.alias('id'),
                            fn.MAX(ApplicationR.user_id).alias('user_id'),
                            fn.MAX(ApplicationR.loan_success_times)
                            .alias('loan_success_times'),
                            fn.MAX(ApplicationR.external_id)
                            .alias('external_id'),
                            fn.MAX(ApplicationR.user_mobile_no)
                            .alias('user_mobile_no')
                        )
                        .join(ApplicationR, JOIN_LEFT_OUTER,
                              ApplicationR.id == RepaymentLogR.application)
                        .where(fn.EXISTS(sub_query),
                               RepaymentLogR.current_bomber == bomber.id,
                               ((RepaymentLogR.principal_part > 0) |
                                (RepaymentLogR.late_fee_part > 0)))
                        .group_by(RepaymentLogR.repay_at,
                                  RepaymentLogR.application))

        if 'application_id' in args:
            apps = apps.where(ApplicationR.external_id == args.application_id)
        if 'user_id' in args:
            apps = apps.where(ApplicationR.user_id == args.user_id)
        if 'user_name' in args:
            apps = apps.where(ApplicationR.user_name == args.user_name)
        if 'mobile' in args:
            apps = apps.where(ApplicationR.user_mobile_no == args.mobile)
        if 'repay_at' in args:
            apps = apps.where(fn.Date(RepaymentLogR.repay_at)
                              == args.repay_at)

        apps = apps.order_by(-ApplicationR.repay_at,
                             -ApplicationR.follow_up_date)

    return BillService().query_page(apps, application_serializer, paginate=True)


@get('/api/v1/applications/<app_id:int>')
def applications(bomber, application):
    app_dict = application_serializer.dump(application).data
    remittance = Remittance()
    try:
        resp = remittance.get('/remittances',
                              {'external_id': application.external_id})
        result = resp.json()['data']['result']
        if len(result) > 0:
            app_dict['bank_code'] = result[0].get('bank_code')
            app_dict['account_no'] = result[0].get('account_no')
            app_dict['remit_amount'] = result[0].get('amount')
    except:
        logging.error(traceback.format_exc())

    data = BillService().get_base_application(app_dict=app_dict)
    data = wapper_base_application(application, data)
    return data


# 根据催收单获取对应预期分期可获取到的折扣
def wapper_base_application(app, data):
    bill_discount = {}
    # 获取所有的分期子账单
    overdue_bills = (OverdueBillR.select()
                     .where(OverdueBillR.collection_id == app.id,
                            OverdueBillR.status != ApplicationStatus.REPAID.value))
    sub_ids = [o.sub_bill_id for o in overdue_bills]
    if not sub_ids or app.type == ApplicationType.CASH_LOAN.value:
        late_fee_limit_rate = late_fee_limit(app.cycle)
        late_fee = Decimal(data['late_fee'])
        principal_paid = Decimal(data['principal_paid'])
        late_fee_paid = Decimal(data['late_fee_paid'])
        min_discount_to = (app.amount +
                           late_fee * (1 - late_fee_limit_rate)
                           - principal_paid - late_fee_paid)
        min_discount_to = max(float(min_discount_to), 0.0)
        data["min_discount_to"] = min_discount_to
        bill_discount[0] = {
            "periods": 0,
            "min_discount_to": min_discount_to,
            "overdue_bill_id": sub_ids[0] if sub_ids else ''
        }
    else:
        try:
            sub_bill = BillService().sub_bill_list(bill_sub_ids = sub_ids)
        except Exception as e:
            logging.info("get_discount_sub_bill error:%s" % str(e))
            data["bill_dicount"] = bill_discount
            return data
        sub_bill_dict = {int(sb["id"]):sb for sb in sub_bill}
        all_late_fee=all_late_fee_paid=all_principal_paid=repaid=unpaid=0
        for ob in overdue_bills:
            cycle = get_cycle_by_overdue_days(ob.overdue_days)
            late_fee_limit_rate = late_fee_limit(cycle)
            sb = sub_bill_dict.get(ob.sub_bill_id,{})
            late_fee = sb['late_fee']
            principal_paid = sb['principal_paid']
            late_fee_paid = sb['late_fee_paid']
            min_discount_to = (ob.amount + late_fee * (1 - late_fee_limit_rate)
                               - principal_paid - late_fee_paid)
            min_discount_to = max(float(min_discount_to), 0.0)
            bill_discount[sb["periods"]] = {
                "periods": sb["periods"],
                "min_discount_to": min_discount_to,
                "overdue_bill_id": ob.id
            }
            all_late_fee += late_fee
            all_late_fee_paid += late_fee_paid
            all_principal_paid += principal_paid
            unpaid += sb["unpaid"]
            repaid += sb["repaid"]
        data['unpaid'] = float(unpaid)
        data['repaid'] = float(repaid)
        data['late_fee'] = float(all_late_fee)
        data['late_fee_paid'] = float(all_late_fee_paid)
        data['principal_paid'] = float(all_principal_paid)
    data["bill_discount"] = bill_discount
    return data


# 获取分期中每期子账单的信息
@get('/api/v1/applications/<app_id:int>/sub_bills')
def application_sub_bill(bomber, application):
    app_type= application.type
    if app_type == ApplicationType.CASH_LOAN.value:
        return []
    # 主张单id
    bill_id = application.bill_id
    if not bill_id:
        overdue = (OverdueBillR.select(OverdueBillR.bill_id)
                   .where(OverdueBillR.collection_id == application.id)
                   .first())
        if not overdue or not overdue.bill_id:
            return []
        bill_id = overdue.bill_id
    try:
        sub_bill = BillService().all_sub_bill(bill_id = bill_id)
    except Exception as e:
        logging.info("get sub_bill error:%s" % str(e))
        return []
    return sub_bill


@get('/api/v1/service/applications/<app_id:int>', skip=[ip_whitelist_plugin])
def api_applications(app_id):
    check_api_user()
    application = Application.filter(Application.external_id == app_id).first()
    if not application:
        abort(404, 'application not found')
    return BillService().get_base_application(application,
                                              application_serializer)


@get('/api/v1/applications/collector', skip=[ip_whitelist_plugin])
def applications_collector():
    check_api_user()
    application_ids = request.params.getall('application_ids')
    apps = (Application
            .select(Application.id, Bomber)
            .join(Bomber, on=(Bomber.id == Application.latest_bomber))
            .where(Application.id << application_ids))

    return {
        a.id: {
            'cycle': a.cycle,
            'collector': a.latest_bomber and a.latest_bomber.name
        } for a in apps
    }

# 当前下p个数查询
@get('/api/v1/applications/bomber/ptp/summary')
def bomber_ptp_applications_summary(bomber):
    bomber_ptp = (BomberPtpR.select(BomberPtpR.ptp_cnt)
                  .where(BomberPtpR.bomber_id == bomber.id)
                  .first())
    ptp_cnt = bomber_ptp.ptp_cnt if bomber_ptp else 0
    return ptp_cnt

# 当日下p的件跟进完成，跟进完后开启自动外呼
@post('/api/v1/applications/bomber/ptp/follow_up_done')
def bomber_ptp_follow_up_done(bomber):
    today = datetime.today().date()
    tomorrow = today + timedelta(days=1)
    begin_date = get_query_time()
    if not begin_date:
        return
    # 跟进判断时间提前半个小时
    begin_date = begin_date - timedelta(minutes=30)
    today_ptp = (ApplicationR.select(ApplicationR.id)
                 .where(ApplicationR.latest_bomber == bomber.id,
                        ApplicationR.status != ApplicationStatus.REPAID.value,
                        ApplicationR.promised_date >= today,
                        ApplicationR.promised_date < tomorrow))
    ptp_ids = [a.id for a in today_ptp]
    if not ptp_ids:
        q = (BomberPtp.update(today_switch=BomberCallSwitch.ON.value)
             .where(BomberPtp.bomber_id == bomber.id)
             .execute())
        return
    follow_ptp = (CallActionsR.select(CallActionsR.application)
                  .where(CallActionsR.created_at >= begin_date,
                         CallActionsR.created_at <= datetime.now(),
                         CallActionsR.application.in_(ptp_ids))
                  .group_by(CallActionsR.application))
    follow_ids = [c.application for c in follow_ptp]
    if len(follow_ids) != len(ptp_ids):
        #件未跟进完，不接自动外呼
        abort(400, 'Not follow up')
    q = (BomberPtp.update(today_switch=BomberCallSwitch.ON.value)
         .where(BomberPtp.bomber_id == bomber.id)
         .execute())
    return

# 获取当前查询时间
def get_query_time():
    now = datetime.now()
    now_time = now.time()
    t1_str = str(now.date()) + ' 11:00:00'
    t2_str = str(now.date()) + ' 15:00:00'
    t3_str = str(now.date()) + ' 18:30:00'
    t1 = datetime.strptime(t1_str, '%Y-%m-%d %H:%M:%S')
    t2 = datetime.strptime(t2_str, '%Y-%m-%d %H:%M:%S')
    t3 = datetime.strptime(t3_str, '%Y-%m-%d %H:%M:%S')

    if t1.time() <= now_time < t2.time():
        return t1
    elif t2.time() <= now_time < t3.time():
        return t2
    elif now_time >= t3.time():
        return t3
    else:
        return False
