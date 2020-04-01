import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from collections import defaultdict

from bottle import get, response
from io import StringIO
from peewee import fn, Param
import pandas as pd

from bomber.constant_mapping import (
    ApplicationStatus,
    EscalationType,
    ApprovalStatus,
    ConnectType,
    GroupCycle,
    Cycle)
from bomber.models import (
    BombingHistory,
    ConnectHistory,
    SummaryBomber,
    RepaymentLog,
    Application,
    CycleTarget,
    Escalation,
    Transfer,
    CallLog,
    Summary,
    Bomber,
    Role,
)
from bomber.worker import (
    get_cycle_new_case_call,
    get_new_case_amount,
    get_recover_amount,
    get_call_and_made,
    get_new_case_call,
    get_calltime_avg,
    get_ptp_call_cnt,
    get_calltime_sum,
    get_claimed_cnt,
    cycle_new_case,
    run_member_sql,
    get_ptp_data,
    run_all_sql,
    get_ptp_cnt
)
from bomber.plugins import packing_plugin
from bomber.utils import plain_query, get_permission, time_logger
from bomber.models_readonly import (
    DispatchAppHistoryR,
    AutoCallActionsR,
    ConnectHistoryR,
    BombingHistoryR,
    ApplicationR,
    CallActionsR,
    BomberR,
    NewCdrR,)


def calc_amount_recovered(start_date=None, end_date=None, bomber_list=None):
    """
    amount_recovered 2018年09月01号(不包括当天)之前沿用以前逻辑,
    之后使用新逻辑
    """
    date_2018_09_01 = date(2018, 9, 1)
    if end_date and end_date < date_2018_09_01 or not bomber_list:
        return {}
    # 因为start_date和end_date同时为None或不为None,所以只需要判断一个即可
    if start_date is None or start_date <= date_2018_09_01:
        start_date = date_2018_09_01

    repayment_log = (
        RepaymentLog
        .select(RepaymentLog.application.alias('application_id'),
                RepaymentLog.current_bomber.alias('current_bomber_id'),
                (RepaymentLog.principal_part + RepaymentLog.late_fee_part)
                .alias('pay_amount'), RepaymentLog.repay_at)
        .where(RepaymentLog.repay_at >= start_date,
               RepaymentLog.current_bomber.in_(bomber_list))
        .group_by(RepaymentLog.application, RepaymentLog.repay_at))

    if end_date is not None:
        # 包含end_date当天的数据
        end_date += timedelta(days=1)
        repayment_log = repayment_log.where(RepaymentLog.repay_at < end_date)

    bombing_history = (
        BombingHistory
        .select(Param('1'))
        .where(RepaymentLog.current_bomber == BombingHistory.bomber,
               RepaymentLog.application == BombingHistory.application,
               RepaymentLog.repay_at > BombingHistory.created_at))

    connect_history = (
        ConnectHistory
        .select(Param('1'))
        .where(RepaymentLog.current_bomber == ConnectHistory.operator,
               RepaymentLog.application == ConnectHistory.application,
               RepaymentLog.repay_at > ConnectHistory.created_at))

    sub_q1 = (repayment_log.where(fn.EXISTS(bombing_history)))

    sub_q2 = (repayment_log.where(fn.EXISTS(connect_history)))

    union_q = (sub_q1 | sub_q2)
    data_list = [
        {'application': i.application_id,
         'bomber': i.current_bomber_id,
         'pay_amount': i.pay_amount,
         'repay_at': i.repay_at}
        for i in union_q]
    df = pd.DataFrame(data_list)

    # 根据 application 和 repay_at 去重
    reserved_df = df.drop_duplicates(['application', 'repay_at'])
    amount_per_bomber = (reserved_df
                         .groupby(['bomber'])
                         .agg({'pay_amount': 'sum'}))
    result = amount_per_bomber.to_dict()
    return result.get('pay_amount') or {}


def filter_c2_and_c3_bomber_list(lst):
    """
    返回属于c2,c3 员工id列表

    :param lst: list
    :return: list
    """
    c2_and_c3_cycle = [Cycle.C2.value, Cycle.C3.value]
    q = (Bomber
         .select()
         .where(Role.cycle.in_(c2_and_c3_cycle))
         .join(Role))
    if lst:
        q = q.where(Bomber.id.in_(lst))
    return [i.id for i in q]


@get('/api/v1/department/summary', skip=[packing_plugin])
def department_summary(bomber):
    args = plain_query()
    start_date, end_date = None, None
    cycle_dict = {1: 'C1A', 2: 'C1B', 3: 'C2', 4: 'C3', 5:'M3'}

    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    history = SummaryBomber.select(
        SummaryBomber.bomber_id,
        SummaryBomber.cycle,
        fn.SUM(SummaryBomber.unfollowed_cnt).alias('new_case_cnt'),
        fn.SUM(SummaryBomber.unfollowed_call_cnt).alias('new_case_call_cnt'),
        fn.SUM(SummaryBomber.ptp_today_cnt).alias('ptp_today_cnt'),
        fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('ptp_today_call'),
        fn.SUM(SummaryBomber.ptp_next_cnt).alias('ptp_next_cnt'),
        fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('ptp_next_call'),
        fn.SUM(SummaryBomber.call_cnt).alias('call_made'),
        fn.SUM(SummaryBomber.call_connect_cnt).alias('call_connected'),
        fn.SUM(SummaryBomber.calltime_case_sum).alias('calltime_case_sum'),
        fn.SUM(SummaryBomber.sms_cnt).alias('sms_sent'),
        fn.SUM(SummaryBomber.promised_cnt).alias('promised'),
        fn.SUM(SummaryBomber.cleared_cnt).alias('cleared'),
        fn.SUM(SummaryBomber.cleared_amount).alias('amount_recovered'),
        fn.SUM(SummaryBomber.calltime_sum).alias('calltime_sum'),
        fn.SUM(SummaryBomber.case_made_cnt).alias('case_made_cnt')
    ).where(SummaryBomber.bomber_id.is_null(False))

    cal_date = date.today()
    next_date = cal_date + timedelta(days=1)
    employees = (Bomber.select().join(Role)
                 .where(Role.id << [1, 2, 4, 5, 6, 8,9],
                        Bomber.last_active_at > (cal_date - timedelta(days=5)))
                 )

    sms_sent = (
        ConnectHistoryR
        .select(ConnectHistoryR.operator,
                fn.COUNT(ConnectHistoryR.id).alias('sms_sent'))
        .where(ConnectHistoryR.created_at >= cal_date,
               ConnectHistoryR.created_at < next_date,
               ConnectHistoryR.type.in_(ConnectType.sms())))

    if 'bomber_id' in args:
        employees = employees.where(Bomber.id == args.bomber_id)
        history = history.where(SummaryBomber.bomber_id == args.bomber_id)
        sms_sent = sms_sent.where(ConnectHistoryR.operator == args.bomber_id)

    # cycle 控制 本cycle的主管只能看本cycle的统计
    if bomber.role.cycle and 'bomber_id' not in args:
        employees = employees.where(Role.cycle == bomber.role.cycle)
        # 分组
        if bomber.role.cycle in GroupCycle.values() and bomber.group_id > 0:
            employees = employees.where(Bomber.group_id == bomber.group_id)
        cycle_bids = [e.id for e in employees]
        history = history.where(SummaryBomber.bomber_id << cycle_bids)
        sms_sent = sms_sent.where(ConnectHistoryR.operator << cycle_bids)

    history = history.group_by(SummaryBomber.bomber_id)
    sms_sent = sms_sent.group_by(ConnectHistoryR.operator)

    summary = {
        i.id: {
            'cycle': i.role.cycle,
            'name': i.name,
            'claimed': 0,
            'new_case': 0,
            'new_followed_cnt': 0,
            'PTP_Due_Today': 0,
            'PTP_Due_Today_followed': 0,
            'PTP_Due_nextday': 0,
            'PTP_Due_nextday_followed': 0,
            'call_made': 0,
            'call_connected': 0,
            'call_duraction': 0,
            'sms_sent': 0,
            'promised': 0,
            'cleared': 0,
            'amount_recovered': Decimal(0),
            'total_call_duraction': 0,
            'case_made_cnt': 0
        }
        for i in employees if i.name
    }

    # 当日期输入不完整时默认查询当天数据。当查询时间维度包括当天时提前计算当天数据
    if not start_date or end_date > cal_date or not end_date:
        bomber_amount_cnt = get_recover_amount(next_date, cal_date, True)
        bomber_call_made_connect_case = get_call_and_made(next_date, cal_date, True)
        claimed_bomber = get_claimed_cnt(next_date, cal_date, True)
        ptp_bomber_amount_cnt = get_ptp_data(next_date, cal_date, True)
        total_call_duraction = get_calltime_sum(cal_date, next_date, True)
        # new_case = get_new_case_amount(cal_date, next_date)
        new_case_followed = get_new_case_call(cal_date, next_date, True)
        unfollowed = get_unfollowed(cal_date)
        calltime_sum = get_calltime_avg(cal_date, next_date, True)

        sub_query = (CallActionsR
                     .select(Param('1'))
                     .where(CallActionsR.application == ApplicationR.id,
                            CallActionsR.bomber_id == ApplicationR.latest_bomber,
                            CallActionsR.created_at > cal_date))

        today = (ApplicationR
                 .select(fn.COUNT(ApplicationR.id).alias('cnt'),
                         ApplicationR.latest_bomber)
                 .where(
                    ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                            ApplicationStatus.AB_TEST.value],
                    ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                           .cycle else Cycle.values()),
                    ApplicationR.promised_date == cal_date)
                 .group_by(ApplicationR.latest_bomber))
        today_follow = today.where(fn.EXISTS(sub_query))

        next_ptp = (ApplicationR
                    .select(fn.COUNT(ApplicationR.id).alias('cnt'),
                            ApplicationR.latest_bomber)
                    .where(
                     ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                             ApplicationStatus.AB_TEST.value],
                     ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                            .cycle else Cycle.values()),
                     ApplicationR.promised_date == next_date)
                    .group_by(ApplicationR.latest_bomber))
        next_follow = next_ptp.where(fn.EXISTS(sub_query))

        all_bomber = summary.keys()

        for i in claimed_bomber:
            if i[1] in all_bomber:
                summary[i[1]]['claimed'] += int(i[0]) if i[0] else 0

        for i in bomber_amount_cnt:
            if i[0] in all_bomber:
                summary[i[0]]['cleared'] += int(i[2]) if i[2] else 0
                summary[i[0]]['amount_recovered'] += int(i[1]) if i[1] else 0

        for i in bomber_call_made_connect_case:
            if i[0] in all_bomber:
                summary[i[0]]['call_made'] += int(i[1]) if i[1] else 0
                summary[i[0]]['call_connected'] += int(i[3]) if i[3] else 0
                summary[i[0]]['case_made_cnt'] += int(i[2]) if i[2] else 0

        for i in ptp_bomber_amount_cnt:
            if i[0] in all_bomber:
                summary[i[0]]['promised'] += int(i[2]) if i[2] else 0

        for i in sms_sent:
            if i.operator_id in all_bomber:
                summary[i.operator_id]['sms_sent'] += int(i.sms_sent)

        for i in today:
            bomber_id = i.latest_bomber.id
            if bomber_id in all_bomber:
                summary[bomber_id]['PTP_Due_Today'] += (int(i.cnt) if
                                                        i.cnt else 0)
        for i in next_ptp:
            bomber_id = i.latest_bomber.id
            if bomber_id in all_bomber:
                summary[bomber_id]['PTP_Due_nextday'] += (int(i.cnt) if
                                                          i.cnt else 0)
        for i in today_follow:
            bomber_id = i.latest_bomber.id
            if bomber_id in all_bomber:
                summary[bomber_id]['PTP_Due_Today_followed'] += (int(i.cnt)
                                                                 if i.cnt
                                                                 else 0)

        for i in next_follow:
            bomber_id = i.latest_bomber.id
            if bomber_id in all_bomber:
                summary[bomber_id]['PTP_Due_nextday_followed'] += (int(i.cnt)
                                                                   if i.cnt
                                                                   else 0)
        for key, value in total_call_duraction.items():
            if key in all_bomber:
                summary[key]['total_call_duraction'] += (int(value)
                                                         if value else 0)
        for key, value in unfollowed.items():
            if key in all_bomber:
                summary[key]['new_case'] += (int(value)
                                             if value else 0)

        for i in new_case_followed:
            if i[0] in all_bomber:
                summary[i[0]]['new_followed_cnt'] += int(i[1]) if i[1] else 0

        for key, value in calltime_sum.items():
            if key in all_bomber:
                summary[key]['call_duraction'] += int(value[0])

        if not start_date or not end_date or start_date == cal_date:
            result = []
            for bomber_id, data in summary.items():
                new_followed_rate = (
                    data['new_followed_cnt'] / data['new_case']
                    if data['new_case'] else 1)
                new_follow_rate = str(round(new_followed_rate * 100, 2)) + '%'

                today_due = data['PTP_Due_Today']
                today_follow = data['PTP_Due_Today_followed']
                today_follow_rate = today_follow / today_due if today_due else 1
                today_follow_rate = (1 if today_follow_rate > 1
                                     else today_follow_rate)
                today_follow_rate = str(round(today_follow_rate * 100, 2)) + '%'

                next_due = data['PTP_Due_nextday']
                next_follow = data['PTP_Due_nextday_followed']
                next_follow_rate = next_follow / next_due if next_due else 1
                next_follow_rate = (1 if next_follow_rate > 1
                                    else next_follow_rate)
                next_follow_rate = str(round(next_follow_rate * 100, 2)) + '%'

                call_duraction = data['call_duraction']
                average_call_duration = (
                    call_duraction / data['call_connected']
                    if data['call_connected'] else 0)
                average_call_duration = str(round(average_call_duration, 2))

                result.append({
                    'cycle': cycle_dict[data['cycle']],
                    'name': str(data['name']),
                    'claimed': str(data['claimed']),
                    'new_case': str(data['new_case']),
                    'new_followed_rate': new_follow_rate,
                    'PTP_Due_Today': today_due,
                    'PTP_Due_Today_followed': today_follow,
                    'PTP_Due_Today_followed_rate': today_follow_rate,
                    'PTP_Due_nextday_followed_rate': next_follow_rate,
                    'call_made': str(data['call_made']),
                    'call_connected': str(data['call_connected']),
                    'case_made_cnt': str(data['case_made_cnt']),
                    'average_call_duration': average_call_duration,
                    'sms_sent': str(data['sms_sent']),
                    'promised': str(data['promised']),
                    'cleared': str(data['cleared']),
                    'amount_recovered': str(data['amount_recovered']),
                    'total_call_duraction': str(data['total_call_duraction']),
                })
            if 'export' in args and args.export == '1':
                response.set_header('Content-Type', 'text/csv')
                response.set_header('Content-Disposition',
                                    'attachment; filename="bomber_export.csv"')

                with StringIO() as csv_file:
                    fields = (
                        'cycle', 'name', 'claimed', 'new_case',
                        'new_followed_rate', 'PTP_Due_Today',
                        'PTP_Due_Today_followed', 'PTP_Due_Today_followed_rate',
                        'PTP_Due_nextday_followed_rate', 'call_made',
                        'call_connected', 'case_made_cnt','average_call_duration',
                        'sms_sent','promised', 'cleared', 'amount_recovered',
                        'total_call_duraction')
                    w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
                    w.writeheader()
                    w.writerows(result)
                    return csv_file.getvalue().encode('utf8', 'ignore')
            return {'data': result}

    # 得到除当天以外的所有历史数据
    claimed_bomber = []
    if start_date and end_date:
        query_date = end_date
        if end_date > cal_date:
            query_date = cal_date
        history = history.where(
            SummaryBomber.time >= start_date, SummaryBomber.time < query_date,
        )

        if end_date > start_date:
            claimed_bomber = get_claimed_cnt(end_date, start_date)

    bombers = set(summary.keys())
    for i in claimed_bomber:
        if i[1] in bombers:
            summary[i[1]]['claimed'] = int(i[0]) if i[0] else 0
    history = history.where(SummaryBomber.bomber_id << bombers)
    for i in history:
        summary[i.bomber_id]['new_case'] += int(i.new_case_cnt)
        summary[i.bomber_id]['new_followed_cnt'] += int(i.new_case_call_cnt)
        summary[i.bomber_id]['PTP_Due_Today'] += int(i.ptp_today_cnt)
        summary[i.bomber_id]['PTP_Due_Today_followed'] += int(i.ptp_today_call)
        summary[i.bomber_id]['PTP_Due_nextday'] += int(i.ptp_next_cnt)
        summary[i.bomber_id]['PTP_Due_nextday_followed'] += int(i.ptp_next_call)
        summary[i.bomber_id]['call_made'] += int(i.call_made)
        summary[i.bomber_id]['call_connected'] += int(i.call_connected)
        summary[i.bomber_id]['case_made_cnt'] += int(i.case_made_cnt)
        summary[i.bomber_id]['call_duraction'] += int(i.calltime_case_sum)
        summary[i.bomber_id]['sms_sent'] += int(i.sms_sent)
        summary[i.bomber_id]['promised'] += int(i.promised)
        summary[i.bomber_id]['cleared'] += int(i.cleared)
        summary[i.bomber_id]['amount_recovered'] += int(i.amount_recovered)
        summary[i.bomber_id]['total_call_duraction'] += int(i.calltime_sum)

    # 如果 区间 不包含 today 则不计算当天数据 直接返回历史数据
    if start_date and end_date:
        result = []
        for bomber_id, data in summary.items():
            new_followed_rate = (
                data['new_followed_cnt'] / data['new_case']
                if data['new_case'] else 1)
            new_follow_rate = str(round(new_followed_rate * 100, 2)) + '%'

            today_due = data['PTP_Due_Today']
            today_follow = data['PTP_Due_Today_followed']
            today_follow_rate = today_follow / today_due if today_due else 1
            today_follow_rate = (1 if today_follow_rate > 1
                                 else today_follow_rate)
            today_follow_rate = str(round(today_follow_rate * 100, 2)) + '%'

            next_due = data['PTP_Due_nextday']
            next_follow = data['PTP_Due_nextday_followed']
            next_follow_rate = next_follow / next_due if next_due else 1
            next_follow_rate = (1 if next_follow_rate > 1
                                else next_follow_rate)
            next_follow_rate = str(round(next_follow_rate * 100, 2)) + '%'

            call_duraction = data['call_duraction']
            average_call_duration = (
                call_duraction / data['call_connected']
                if data['call_connected'] else 0)
            average_call_duration = str(round(average_call_duration, 2))

            result.append({
                'cycle': cycle_dict[data['cycle']],
                'name': str(data['name']),
                'claimed': str(data['claimed']),
                'new_case': str(data['new_case']),
                'new_followed_rate': new_follow_rate,
                'PTP_Due_Today': today_due,
                'PTP_Due_Today_followed': today_follow,
                'PTP_Due_Today_followed_rate': today_follow_rate,
                'PTP_Due_nextday_followed_rate': next_follow_rate,
                'call_made': str(data['call_made']),
                'call_connected': str(data['call_connected']),
                'case_made_cnt': str(data['case_made_cnt']),
                'average_call_duration': average_call_duration,
                'sms_sent': str(data['sms_sent']),
                'promised': str(data['promised']),
                'cleared': str(data['cleared']),
                'amount_recovered': str(data['amount_recovered']),
                'total_call_duraction': str(data['total_call_duraction']),
            })
        if 'export' in args and args.export == '1':
            response.set_header('Content-Type', 'text/csv')
            response.set_header('Content-Disposition',
                                'attachment; filename="bomber_export.csv"')

            with StringIO() as csv_file:
                fields = ('cycle', 'name', 'claimed', 'new_case',
                          'new_followed_rate', 'PTP_Due_Today',
                          'PTP_Due_Today_followed', 'PTP_Due_Today_followed_rate',
                          'PTP_Due_nextday_followed_rate', 'call_made',
                          'call_connected','case_made_cnt','average_call_duration',
                          'sms_sent', 'promised', 'cleared',
                          'amount_recovered', 'total_call_duraction')
                w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
                w.writeheader()
                w.writerows(result)
                return csv_file.getvalue().encode('utf8', 'ignore')
        return {'data': result}


department_summary.permission = get_permission('department', 'summary')


@get('/api/v1/personal/summary/chart')
def department_summary(bomber):
    args = plain_query()
    start_date, end_date = None, None

    history = SummaryBomber.select(
        SummaryBomber.time,
        fn.SUM(SummaryBomber.unfollowed_cnt).alias('new'),
        fn.SUM(SummaryBomber.unfollowed_call_cnt).alias('new_call'),
        fn.SUM(SummaryBomber.ptp_today_cnt).alias('today'),
        fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('today_call'),
        fn.SUM(SummaryBomber.ptp_next_cnt).alias('next_day'),
        fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('next_call')
    ).where(SummaryBomber.bomber_id == bomber.id)

    if 'start_chart_date' in args and 'end_chart_date' in args:
        start_date = datetime.strptime(args.start_chart_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_chart_date, '%Y-%m-%d').date()

    if not start_date or not end_date:
        end_date = date.today()
        start_date = end_date - timedelta(days=7)

    history = history.where(
        SummaryBomber.time >= start_date,
        SummaryBomber.time < end_date,
    )
    history = history.group_by(SummaryBomber.time)

    new_data = {'new': [], 'new_rate': []}
    today = {'today': [], 'today_rate': []}
    next_day = {'next_day': [], 'next_rate': []}
    time = []
    for i in history:
        new_data['new'].append(str(i.new))
        new_call = i.new_call
        new_rate = round((new_call / i.new if i.new else 1) * 100, 2)
        new_rate = new_rate if new_rate < 100 else 100
        new_data['new_rate'].append(str(new_rate))

        today['today'].append(str(i.today))
        today_call = i.today_call
        today_rate = round((today_call / i.today if i.today else 1) * 100, 2)
        today_rate = today_rate if today_rate < 100 else 100
        today['today_rate'].append(str(today_rate))

        next_day['next_day'].append(str(i.next_day))
        call = i.next_call
        next_rate = round((call / i.next_day if i.next_day else 1) * 100, 2)
        next_rate = next_rate if next_rate < 100 else 100
        next_day['next_rate'].append(str(next_rate))

        time.append(i.time.strftime('%Y-%m-%d'))

    cal_date = date.today()
    if end_date > cal_date:
        new_case_cnt = 0
        unfollowed = get_unfollowed(cal_date, bomber.id)
        for _, value in unfollowed.items():
            new_case_cnt += int(value) if value else 0

        new_case_call = get_member_new_case_call(cal_date, end_date, bomber.id)
        new_case_call_cnt = new_case_call[0]
        new_data['new'].append(new_case_cnt)
        new_rate = round((new_case_call_cnt / new_case_cnt if
                          new_case_cnt else 1) * 100, 2)
        new_rate = new_rate if new_rate < 100 else 100
        new_data['new_rate'].append(str(new_rate))

        sub = (CallActionsR
               .select(Param('1'))
               .where(CallActionsR.application == ApplicationR.id,
                      CallActionsR.bomber_id == ApplicationR.latest_bomber,
                      CallActionsR.created_at > cal_date))

        today_ptp = (ApplicationR
                     .select(fn.COUNT(ApplicationR.id).alias('cnt'))
                     .where(
                      ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                              ApplicationStatus.AB_TEST.value],
                      ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                              .cycle else Cycle.values()),
                      ApplicationR.promised_date == cal_date,
                      ApplicationR.latest_bomber == bomber.id)
                     )
        today_follow = today_ptp.where(fn.EXISTS(sub))
        next_ptp = (ApplicationR
                    .select(fn.COUNT(ApplicationR.id).alias('cnt'))
                    .where(
                     ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                             ApplicationStatus.AB_TEST.value],
                     ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                            .cycle else Cycle.values()),
                     ApplicationR.promised_date == end_date,
                     ApplicationR.latest_bomber == bomber.id)
                    )
        next_follow = next_ptp.where(fn.EXISTS(sub))

        today_cnt = today_ptp.first()
        ptp_today_cnt = int(today_cnt.cnt) if today_cnt else 0
        next_cnt = next_ptp.first()
        ptp_next_cnt = int(next_cnt.cnt) if next_cnt else 0

        today_follow_cnt = today_follow.first()
        ptp_today_call_cnt = int(today_follow_cnt.cnt)
        next_follow_cnt = next_follow.first()
        ptp_next_call_cnt = int(next_follow_cnt.cnt)

        today['today'].append(ptp_today_cnt)
        today_rate = round((ptp_today_call_cnt / ptp_today_cnt if
                            ptp_today_cnt else 1) * 100, 2)
        today_rate = today_rate if today_rate < 100 else 100
        today['today_rate'].append(str(today_rate))

        next_day['next_day'].append(ptp_next_cnt)
        next_rate = round((ptp_next_call_cnt / ptp_next_cnt if
                           ptp_next_cnt else 1) * 100, 2)
        next_rate = next_rate if next_rate < 100 else 100
        next_day['next_rate'].append(str(next_rate))

        time.append(cal_date.strftime('%Y-%m-%d'))

    result = {'new_data': new_data, 'today': today,
              'next': next_day, 'date': time}
    return result


@get('/api/v1/personal/summary')
def personal_summary(bomber):
    args = plain_query()
    cal_date = date.today()

    start_date, end_date = cal_date, cal_date
    args['bomber_id'] = bomber.id

    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    history = Summary.select(
        Summary.bomber,
        fn.SUM(Summary.claimed).alias('claimed'),
        fn.SUM(Summary.completed).alias('completed'),
        fn.SUM(Summary.cleared).alias('cleared'),
        fn.SUM(Summary.escalated).alias('escalated'),
        fn.SUM(Summary.transferred).alias('transferred'),
        fn.SUM(Summary.amount_recovered).alias('amount_recovered'),
        fn.SUM(Summary.calls_made).alias('calls_made'),
        fn.SUM(Summary.calls_connected).alias('calls_connected'),
        fn.SUM(Summary.sms_sent).alias('sms_sent'),
    ).where(Summary.bomber.is_null(False))

    employees = Bomber.select().join(Role)
    claimed = (
        Application
        .select(Application.latest_bomber,
                fn.COUNT(Application.id).alias('claimed'))
        .where(fn.DATE(Application.claimed_at) == cal_date,
               Application.status <<
               [ApplicationStatus.PROCESSING.value,
                ApplicationStatus.REPAID.value],
               Application.latest_bomber.is_null(False)))

    cleared = (
        Application
        .select(Application.latest_bomber,
                fn.COUNT(Application.id).alias('cleared'))
        .where(fn.DATE(Application.finished_at) == cal_date,
               Application.status == ApplicationStatus.REPAID.value,
               Application.latest_bomber.is_null(False)))

    completed = (
        Application
        .select(Application.latest_bomber,
                fn.COUNT(Application.id).alias('completed'))
        .where(Application.latest_bombing_time.is_null(False),
               fn.DATE(Application.latest_bombing_time) == cal_date,
               Application.latest_bomber.is_null(False)))

    escalated = (
        Escalation
        .select(Escalation.current_bomber,
                fn.COUNT(Escalation.id).alias('escalated'))
        .where(fn.DATE(Escalation.created_at) == cal_date,
               Escalation.type == EscalationType.AUTOMATIC.value,
               Escalation.current_bomber.is_null(False),
               Escalation.status == ApprovalStatus.APPROVED.value))

    transferred = (
        Transfer
        .select(Transfer.operator,
                fn.COUNT(Transfer.id).alias('transferred'))
        .where(fn.DATE(Transfer.reviewed_at) == cal_date,
               Transfer.status == ApprovalStatus.APPROVED.value))

    amount_recovered = (
        RepaymentLog
        .select(RepaymentLog.current_bomber,
                fn.SUM(RepaymentLog.principal_part)
                .alias('principal_part'),
                fn.SUM(RepaymentLog.late_fee_part)
                .alias('late_fee_part'))
        .where(fn.DATE(RepaymentLog.repay_at) == cal_date,
               RepaymentLog.current_bomber.is_null(False),
               RepaymentLog.is_bombed == True))

    calls_made = (
        CallLog
        .select(CallLog.user_id,
                fn.COUNT(CallLog.record_id).alias('calls_made'))
        .where(fn.DATE(CallLog.time_start) == cal_date,
               CallLog.system_type == '1'))

    calls_connected = (
        CallLog
        .select(CallLog.user_id,
                fn.COUNT(CallLog.record_id)
                .alias('calls_connected'))
        .where(fn.DATE(CallLog.time_start) == cal_date,
               CallLog.duration > 10,
               CallLog.system_type == '1'))

    sms_sent = (
        ConnectHistory
        .select(ConnectHistory.operator,
                fn.COUNT(ConnectHistory.id).alias('sms_sent'))
        .where(fn.DATE(ConnectHistory.created_at) == cal_date,
               ConnectHistory.type.in_(ConnectType.sms())))

    if 'bomber_id' in args:
        employees = employees.where(Bomber.id == args.bomber_id)
        history = history.where(Summary.bomber == args.bomber_id)
        claimed = claimed.where(Application.latest_bomber == args.bomber_id)
        completed = completed.where(Application.latest_bomber ==
                                    args.bomber_id)
        cleared = cleared.where(Application.latest_bomber == args.bomber_id)
        escalated = (
            escalated
            .where(Escalation.current_bomber == args.bomber_id))
        transferred = transferred.where(Transfer.operator == args.bomber_id)
        amount_recovered = amount_recovered.where(
            RepaymentLog.current_bomber == args.bomber_id
        )
        calls_made = (
            calls_made
            .where(CallLog.user_id == args.bomber_id))
        calls_connected = calls_connected.where(CallLog.user_id ==
                                                args.bomber_id)
        sms_sent = sms_sent.where(ConnectHistory.operator == args.bomber_id)

    if start_date and end_date:
        history = history.where(
            Summary.date >= start_date, Summary.date <= end_date,
        )

    history = history.group_by(Summary.bomber)
    claimed = claimed.group_by(Application.latest_bomber)
    completed = completed.group_by(Application.latest_bomber)
    cleared = cleared.group_by(Application.latest_bomber)
    escalated = escalated.group_by(Escalation.current_bomber)
    transferred = transferred.group_by(Transfer.operator)
    amount_recovered = amount_recovered.group_by(RepaymentLog.current_bomber)
    calls_made = calls_made.group_by(CallLog.user_id)
    calls_connected = calls_connected.group_by(CallLog.user_id)
    sms_sent = sms_sent.group_by(ConnectHistory.operator)

    summary = {
        i.id: {
            'name': i.name,
            'claimed': 0,
            'completed': 0,
            'cleared': 0,
            'escalated': 0,
            'transferred': 0,
            'amount_recovered': Decimal(0),
            'calls_made': 0,
            'calls_connected': 0,
            'sms_sent': 0,
        }
        for i in employees if i.name
    }

    for i in history:
        summary[i.bomber_id]['claimed'] += int(i.claimed)
        summary[i.bomber_id]['completed'] += int(i.completed)
        summary[i.bomber_id]['cleared'] += int(i.cleared)
        summary[i.bomber_id]['escalated'] += int(i.escalated)
        summary[i.bomber_id]['transferred'] += int(i.transferred)
        summary[i.bomber_id]['amount_recovered'] += i.amount_recovered
        summary[i.bomber_id]['calls_made'] += int(i.calls_made)
        summary[i.bomber_id]['calls_connected'] += int(i.calls_connected)
        summary[i.bomber_id]['sms_sent'] += int(i.sms_sent)

    # 如果 区间 不包含 today 则不计算当天数据 直接返回历史数据
    if start_date and end_date and end_date < cal_date:
        result = []
        for bomber_id, data in summary.items():
            result.append({
                'name': data['name'],
                'claimed': data['claimed'],
                'completed': data['completed'],
                'cleared': data['cleared'],
                'escalated': data['escalated'],
                'transferred': data['transferred'],
                'amount_recovered': str(data['amount_recovered']),
                'calls_made': data['calls_made'],
                'calls_connected': data['calls_connected'],
                'sms_sent': data['sms_sent'],
            })
        return result[0]

    for i in claimed:
        summary[i.latest_bomber_id]['claimed'] += i.claimed

    for i in completed:
        summary[i.latest_bomber_id]['completed'] += i.completed

    for i in cleared:
        summary[i.latest_bomber_id]['cleared'] += i.cleared

    for i in escalated:
        summary[i.current_bomber_id]['escalated'] += i.escalated

    for i in transferred:
        summary[i.operator_id]['transferred'] += i.transferred

    for i in amount_recovered:
        amount_recovered = i.principal_part + i.late_fee_part
        summary[i.current_bomber_id]['amount_recovered'] += amount_recovered

    for i in calls_made:
        summary[int(i.user_id)]['calls_made'] += i.calls_made

    for i in calls_connected:
        summary[int(i.user_id)]['calls_connected'] += i.calls_connected

    for i in sms_sent:
        summary[i.operator_id]['sms_sent'] += i.sms_sent

    result = []
    for bomber_id, data in summary.items():
        result.append({
            'name': data['name'],
            'claimed': data['claimed'],
            'completed': data['completed'],
            'cleared': data['cleared'],
            'escalated': data['escalated'],
            'transferred': data['transferred'],
            'amount_recovered': str(data['amount_recovered']),
            'calls_made': data['calls_made'],
            'calls_connected': data['calls_connected'],
            'sms_sent': data['sms_sent'],
        })
    return result[0]

@time_logger
def get_member_new_case_call(begin_date, end_date, bomber_id):
    sql = """
        SELECT
            count( DISTINCT bd.application_id )
        FROM
            bomber.dispatch_app_history bd
            INNER JOIN bomber.call_actions bc 
                 ON bd.application_id = bc.application_id 
            AND bd.bomber_id = bc.bomber_id 
            AND date( bd.entry_at ) = date( bc.created_at ) 
        WHERE
            entry_at >= '%s' 
            AND entry_at < '%s' 
            AND bd.bomber_id = %s
    """ % (begin_date, end_date, bomber_id)
    result = run_member_sql(sql)

    return result

@time_logger
def get_member_ptp_call(begin_date, end_date, bomber_id):
    result = []
    for sql_data in (begin_date, end_date):
        sql = """
            select count(distinct b.application_id) as cnt, b.bomber_id
            from (
                select a.* 
                from 
                   (select application_id,bomber_id,created_at 
                   from bomber.auto_call_actions 
                   where promised_date ='%s'
                     and bomber_id=%s
                   union
                   select b.application_id,b.bomber_id,a.cdt
                   from bomber.bombing_history b
                   inner join (
                      select application_id,bomber_id,max(created_at) as cdt 
                      from bomber.bombing_history bb
                      where bb.created_at>date_sub('%s',interval 15 day)
                        and promised_date is not null
                      group by 1,2) a 
                   on b.application_id=a.application_id 
                      and b.bomber_id=a.bomber_id and a.cdt=b.created_at
                   where b.promised_date ='%s'
                     and b.bomber_id=%s
                ) a
                where exists(select 1 from bomber.application ba 
                             where ba.id=a.application_id 
                               and ((ba.finished_at is null) 
                                   or (ba.finished_at > '%s')))
                and exists(select 1 from bomber.call_actions bc 
                           where a.application_id = bc.application_id 
                             and a.bomber_id = bc.bomber_id 
                             and bc.created_at>'%s' 
                             and bc.created_at< 
                                 date_add('%s',interval 1 day) 
                             and bc.created_at>=a.created_at)
                union 
                select a.* 
                from 
                   (select application_id,bomber_id,created_at 
                   from bomber.auto_call_actions 
                   where promised_date ='%s'
                     and bomber_id=%s
                   union
                   select b.application_id,b.bomber_id,a.cdt
                   from bomber.bombing_history b
                   inner join (
                        select application_id,bomber_id,max(created_at) as cdt 
                        from bomber.bombing_history bb
                        where bb.created_at > date_sub('%s',interval 15 day)
                          and promised_date is not null
                        group by 1,2) a 
                   on b.application_id=a.application_id 
                      and b.bomber_id=a.bomber_id and a.cdt=b.created_at
                   where b.promised_date ='%s'
                     and b.bomber_id=%s
                ) a
                where exists(select 1 from bomber.application ba 
                             where ba.id=a.application_id 
                               and ba.finished_at > '%s' 
                               and ba.finished_at< 
                                   date_add('%s',interval 1 day))
                ) b
            group by 2
        """ % (sql_data, bomber_id, sql_data, sql_data, bomber_id, begin_date,
               begin_date, begin_date, sql_data, bomber_id, sql_data, sql_data,
               bomber_id, begin_date, begin_date)
        data = run_member_sql(sql)
        result.append(data[0])
    return result


def get_call(begin_date, end_date, bomber_id):
    sql = """
       select 
        count(case when relationship is not null then application_id end) 
          as 'call_cnt', 
          count(case when phone_status=4 then application_id end) as 'connect'
       from (
           select application_id, phone_status, relationship
           from bomber.call_actions ba
           where created_at>'%s' and created_at<'%s'
             and bomber_id = %s
             and type in (0, 1)) a
    """ % (begin_date, end_date, bomber_id)
    data = run_member_sql(sql)
    return data


def get_repay_cnt(begin_date, end_date, bomber_id, cycle):
    if cycle in [Cycle.C1A.value, Cycle.C1B.value]:
        sql = """
            SELECT 
              sum(principal_part+late_fee_part) as pay_amount,
              count(distinct application_id)
            from 
             (select a.principal_part,a.late_fee_part,
                     a.application_id,a.repay_at
             FROM bomber.repayment_log a 
             WHERE a.repay_at >= '%s' AND a.repay_at <'%s'
             AND a.current_bomber_id = %s
             and principal_part+late_fee_part>0
             group by 3,4) a
        """ % (begin_date, end_date, bomber_id)
        result = run_member_sql(sql)
    else:
        sql = """
            select sum(pay_amount) as pay_amount,
             count(distinct application_id)
            from (
            select application_id,current_bomber_id,pay_amount,repay_at
            from (
             select br.application_id,br.current_bomber_id,
                   br.principal_part+br.late_fee_part as pay_amount,br.repay_at
             from bomber.repayment_log br
             left join bomber.bomber bb on br.current_bomber_id=bb.id
             where exists (select 1 from bomber.bombing_history bb 
                           where br.current_bomber_id=bb.bomber_id 
                            and br.application_id=bb.application_id 
                            and bb.created_at<br.repay_at 
                            and (bb.promised_date is not null 
                            or bb.promised_amount is not null))
                and br.repay_at >= '%s'
                and br.repay_at < '%s'
                and br.current_bomber_id = %s
                and br.principal_part+br.late_fee_part > 0
                group by 1,4
                ) a
            group by 1,4) b
        """ % (begin_date, end_date, bomber_id)
        result = run_member_sql(sql)
    return result


@get('/api/v1/personal/summary/table_data')
def personal_summary(bomber):
    args = plain_query()
    cal_date = date.today()

    start_date, end_date = cal_date, cal_date + timedelta(days=1)
    bomber_id = bomber.id
    cycle = bomber.role.cycle

    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    query_date = end_date
    if end_date > cal_date:
        query_date = cal_date
    history = (SummaryBomber.select(
        SummaryBomber.cycle,
        fn.SUM(SummaryBomber.unfollowed_cnt).alias('new_case_cnt'),
        fn.SUM(SummaryBomber.unfollowed_call_cnt).alias('new_case_call_cnt'),
        fn.SUM(SummaryBomber.ptp_today_cnt).alias('ptp_today_cnt'),
        fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('ptp_today_call_cnt'),
        fn.SUM(SummaryBomber.ptp_next_cnt).alias('ptp_next_cnt'),
        fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('ptp_next_call_cnt'),
        fn.SUM(SummaryBomber.call_cnt).alias('call_cnt'),
        fn.SUM(SummaryBomber.call_connect_cnt).alias('call_connect_cnt'),
        fn.SUM(SummaryBomber.sms_cnt).alias('sms_sent'),
        fn.SUM(SummaryBomber.promised_cnt).alias('promised_cnt'),
        fn.SUM(SummaryBomber.cleared_cnt).alias('cleared_cnt'),
        fn.SUM(SummaryBomber.cleared_amount).alias('cleared_amount')
    ).where(SummaryBomber.bomber_id == bomber_id,
            SummaryBomber.time >= start_date,
            SummaryBomber.time < query_date)
    ).first()

    table_data = {'claimed_cnt': 0, 'new_case_cnt': 0, 'new_case_call_cnt': 0,
                  'ptp_today_cnt': 0, 'ptp_today_call_cnt': 0,
                  'ptp_next_cnt': 0, 'ptp_next_call_cnt': 0, 'call_cnt': 0,
                  'call_connect_cnt': 0, 'sms_sent': 0, 'promised_cnt': 0,
                  'cleared_cnt': 0, 'cleared_amount': 0}
    if history.cycle:
        table_data['new_case_cnt'] += int(history.new_case_cnt)
        table_data['new_case_call_cnt'] += int(history.new_case_call_cnt)
        table_data['ptp_today_cnt'] += int(history.ptp_today_cnt)
        table_data['ptp_today_call_cnt'] += int(history.ptp_today_call_cnt)
        table_data['ptp_next_cnt'] += int(history.ptp_next_cnt)
        table_data['ptp_next_call_cnt'] += int(history.ptp_next_call_cnt)
        table_data['call_cnt'] += int(history.call_cnt)
        table_data['call_connect_cnt'] += int(history.call_connect_cnt)
        table_data['sms_sent'] += int(history.sms_sent)
        table_data['promised_cnt'] += int(history.promised_cnt)
        table_data['cleared_cnt'] += int(history.cleared_cnt)
        table_data['cleared_amount'] += int(history.cleared_amount)

    # 对于一段时间的持有件需要重新计算
    table_date = start_date - timedelta(days=30)
    claimed = (DispatchAppHistoryR
               .select(fn.COUNT(DispatchAppHistoryR.application).alias('cnt'))
               .where(((DispatchAppHistoryR.out_at >= start_date) |
                       (DispatchAppHistoryR.out_at.is_null(True))),
                      DispatchAppHistoryR.bomber_id == bomber_id,
                      DispatchAppHistoryR.entry_at >= table_date,
                      DispatchAppHistoryR.entry_at < end_date
                      )
               ).first()
    table_data['claimed_cnt'] = claimed.cnt

    # 实时得到当天数据
    if end_date > cal_date:
        end_date = cal_date + timedelta(days=1)
        unfollowed = get_unfollowed(cal_date, bomber_id)
        # 这个方法此处至多返回返回一条记录
        for _, value in unfollowed.items():
            table_data['new_case_cnt'] += int(value) if value else 0

        new_case_call = get_member_new_case_call(cal_date, end_date, bomber_id)
        table_data['new_case_call_cnt'] += new_case_call[0]

        sub = (CallActionsR
               .select(Param('1'))
               .where(CallActionsR.application == ApplicationR.id,
                      CallActionsR.bomber_id == ApplicationR.latest_bomber,
                      CallActionsR.created_at > cal_date))

        today = (ApplicationR
                 .select(fn.COUNT(ApplicationR.id).alias('cnt'))
                 .where(
                   ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                           ApplicationStatus.AB_TEST.value],
                   ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                          .cycle else Cycle.values()),
                   ApplicationR.promised_date == cal_date,
                   ApplicationR.latest_bomber == bomber_id)
                 )
        today_follow = today.where(fn.EXISTS(sub))

        next_ptp = (ApplicationR
                    .select(fn.COUNT(ApplicationR.id).alias('cnt'))
                    .where(
                     ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                             ApplicationStatus.AB_TEST.value],
                     ApplicationR.cycle << ([bomber.role.cycle] if bomber.role
                                            .cycle else Cycle.values()),
                     ApplicationR.promised_date == end_date,
                     ApplicationR.latest_bomber == bomber_id)
                    )
        next_follow = next_ptp.where(fn.EXISTS(sub))

        today_cnt = today.first()
        table_data['ptp_today_cnt'] += int(today_cnt.cnt) if today_cnt else 0
        next_cnt = next_ptp.first()
        table_data['ptp_next_cnt'] += int(next_cnt.cnt) if next_cnt else 0

        today_follow_cnt = today_follow.first()
        table_data['ptp_today_call_cnt'] += int(today_follow_cnt.cnt)
        next_follow_cnt = next_follow.first()
        table_data['ptp_next_call_cnt'] += int(next_follow_cnt.cnt)

        call = get_call(cal_date, end_date, bomber_id)
        table_data['call_cnt'] += call[0]
        table_data['call_connect_cnt'] += call[1]

        sms = (ConnectHistoryR
               .select(fn.COUNT(ConnectHistoryR.application).alias('sms_send'))
               .where(ConnectHistoryR.created_at >= cal_date,
                      ConnectHistoryR.created_at < end_date,
                      ConnectHistoryR.type.in_(ConnectType.sms()),
                      ConnectHistoryR.operator == bomber_id)
               ).first()
        table_data['sms_sent'] += sms.sms_send

        auto_ptp = (AutoCallActionsR
                    .select(fn.COUNT(AutoCallActionsR.application).alias('ct'))
                    .where(AutoCallActionsR.created_at >= cal_date,
                           AutoCallActionsR.created_at < end_date,
                           AutoCallActionsR.promised_date.is_null(False),
                           AutoCallActionsR.bomber == bomber_id)
                    ).first()
        table_data['promised_cnt'] += auto_ptp.ct
        manual = (BombingHistoryR
                  .select(fn.COUNT(BombingHistoryR.application).alias('cnt'))
                  .where(BombingHistoryR.bomber == bomber_id,
                         BombingHistoryR.created_at >= cal_date,
                         BombingHistoryR.created_at < end_date,
                         BombingHistoryR.promised_date.is_null(False))
                  ).first()
        table_data['promised_cnt'] += manual.cnt

        repay = get_repay_cnt(cal_date, end_date, bomber_id, cycle)
        if repay[0]:
            table_data['cleared_amount'] += int(repay[0])
        table_data['cleared_cnt'] += int(repay[1])
    return [table_data]


@time_logger
def get_cycle_claimed(begin_date, end_date):
    sql = """
        select cycle,count(1)
        from bomber.application where cycle in (1,2,3,4)
        and (finished_at is null or (finished_at>'%s' and finished_at<'%s'))
        and created_at>'2018-09-01'
        group by 1
    """ % (begin_date, end_date)
    result = run_all_sql(sql)
    return result


@time_logger
def get_paid_total(begin_date, end_date):
    sql = """
        select cycle, sum(paid_amount) 
        from (select r.cycle,r.application_id,
                     r.principal_part+r.late_fee_part as paid_amount,
               r.repay_at,
               r.current_bomber_id
              from bomber.repayment_log r
           where repay_at>='%s'
             and repay_at<'%s'
             and r.cycle in (1,2,3,4)
             and r.no_active = 0
             and not exists(select 1 from bomber.bomber bb
                    where r.current_bomber_id = bb.id 
                      and bb.role_id=11)
            group by 1,2,4
           ) a
        group by 1
    """ % (begin_date, end_date)
    result = run_all_sql(sql)
    return result

@time_logger
def new_claimed(start_date, end_date, new_case):
    claimed = {1: 0, 2: 0, 3: 0, 4: 0, 5:0}
    result = []
    if not start_date or (start_date == date.today()
                          and end_date > date.today()):
        result = get_cycle_claimed(start_date, end_date)
        return result
    if start_date and end_date and (start_date < date.today()):
        begin_claimed = (SummaryBomber
                         .select(SummaryBomber.claimed_cnt,
                                 SummaryBomber.cycle)
                         .where(SummaryBomber.cycle == SummaryBomber.bomber_id,
                                SummaryBomber.time == start_date)
                         )
        for c in begin_claimed:
            claimed[c.cycle] += int(c.claimed_cnt)

        other_new = (SummaryBomber
                     .select(fn.SUM(SummaryBomber.new_case_cnt).alias('cnt'),
                             SummaryBomber.cycle)
                     .where(SummaryBomber.time > start_date,
                            SummaryBomber.time < end_date,
                            SummaryBomber.cycle == SummaryBomber.bomber_id)
                     .group_by(SummaryBomber.cycle))
        for o in other_new:
            claimed[o.cycle] += int(o.cnt)

        # 对于选择了当天时，加上当天的新件
        if end_date > date.today():
            for i in new_case:
                cycle = i[0]
                if cycle in Cycle.values():
                    claimed[cycle] += int(i[1])

        for k, v in claimed.items():
            result.append([k, v])

    return result


@get('/api/v1/cycle/target_rate', skip=[packing_plugin])
def get_target_rate():
    cycle = {1: 'C1A', 2: 'C1B', 3: 'C2', 4: 'C3'}
    today = date.today()
    month = today.strftime('%Y-%m') + '-01'
    repayment = get_paid_total(month, date.today() + timedelta(days=1))
    target_amount = (CycleTarget
                     .filter(CycleTarget.target_month == month)
                     .order_by(CycleTarget.cycle))
    paid, target, result = {}, {}, {}
    for p in repayment:
        paid[int(p[0])] = int(p[1] / 1000000)
    for t in target_amount:
        target[int(t.cycle)] = int(t.target_amount)

    for key, value in target.items():
        if key == Cycle.M3.value:
            continue
        rate = round((paid[key] / value if value else 0) * 100, 2)
        result[cycle[key]] = str(rate)

    return {'data': result}


@get('/api/v1/department/summary/cycle', skip=[packing_plugin])
def summary_cycle(bomber):
    args = plain_query()
    start_date, end_date = None, None

    if 'start_date' in args and 'end_date' in args:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    history = SummaryBomber.select(
        SummaryBomber.cycle,
        SummaryBomber.bomber_id,
        fn.SUM(SummaryBomber.cleared_cnt).alias('cleared'),
        fn.SUM(SummaryBomber.new_case_cnt).alias('new_case_cnt'),
        fn.SUM(SummaryBomber.new_case_call_cnt).alias('new_case_call_cnt'),
        fn.SUM(SummaryBomber.ptp_today_cnt).alias('ptp_today_cnt'),
        fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('ptp_today_call'),
        fn.SUM(SummaryBomber.ptp_next_cnt).alias('ptp_next_cnt'),
        fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('ptp_next_call'),
        fn.SUM(SummaryBomber.promised_cnt).alias('promised'),
        fn.SUM(SummaryBomber.cleared_amount).alias('amount_recovered'),
        fn.SUM(SummaryBomber.call_cnt).alias('call_made'),
        fn.SUM(SummaryBomber.call_connect_cnt).alias('call_connected'),
        fn.SUM(SummaryBomber.sms_cnt).alias('sms_sent'),
        fn.SUM(SummaryBomber.calltime_sum).alias('calltime_sum'),
        fn.SUM(SummaryBomber.work_ind).alias('work_cnt')
    ).where(SummaryBomber.cycle == SummaryBomber.bomber_id)

    summary = {
        i: {
            'cycle': i,
            'claimed': 0,
            'new_case': 0,
            'new_followed_cnt': 0,
            'PTP_Due_Today': 0,
            'PTP_Due_Today_followed': 0,
            'PTP_Due_nextday': 0,
            'PTP_Due_nextday_followed': 0,
            'call_made': 0,
            'call_connected': 0,
            'sms_sent': 0,
            'promised': 0,
            'cleared': 0,
            'amount_recovered': Decimal(0),
            'amount_recovered_total': Decimal(0),
            'total_call_duraction': 0,
            'work_cnt': 0
        }
        for i in (1, 2, 3, 4)
    }

    cal_date = date.today()
    paid_total = []
    new_case = []
    # 未選擇日期或日期選擇錯誤或日期到当天
    if not start_date or not end_date or end_date > cal_date:
        next_date = cal_date + timedelta(days=1)
        bomber_amount_cnt = get_recover_amount(next_date, cal_date, True)
        bomber_call_made_connect_case = get_call_and_made(next_date, cal_date, True)
        ptp_bomber_amount_cnt = get_ptp_data(next_date, cal_date, True)
        paid_total = get_paid_total(cal_date, next_date)
        total_call_duraction = get_calltime_sum(cal_date, next_date, True)
        new_case = cycle_new_case(cal_date, next_date, True)
        new_case_followed = get_new_case_call(cal_date, next_date, True)
        c1_new_case_followed = get_cycle_new_case_call(cal_date, next_date,True)
        sms_sent = (
            ConnectHistoryR
            .select(ConnectHistoryR.operator,
                    fn.COUNT(ConnectHistoryR.id).alias('sms_sent'))
            .where(ConnectHistoryR.created_at >= cal_date,
                   ConnectHistoryR.created_at < next_date,
                   ConnectHistoryR.type.in_(ConnectType.sms()))
            .group_by(ConnectHistoryR.operator))

        works = (BomberR
                 .select(BomberR.id.alias('id'))
                 .where(BomberR.role << [1, 2, 4, 5, 6, 8],
                        BomberR.last_active_at >= date.today()))

        sub = (CallActionsR
               .select(Param('1'))
               .where(CallActionsR.application == ApplicationR.id,
                      CallActionsR.bomber_id == ApplicationR.latest_bomber,
                      CallActionsR.created_at > cal_date))

        today = (ApplicationR
                 .select(fn.COUNT(ApplicationR.id).alias('cnt'),
                         ApplicationR.cycle)
                 .where(
                  ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                          ApplicationStatus.AB_TEST.value],
                  ApplicationR.cycle << [1, 2, 3, 4],
                  ApplicationR.promised_date == cal_date)
                 .group_by(ApplicationR.cycle))
        today_follow = today.where(fn.EXISTS(sub))

        next_ptp = (ApplicationR
                    .select(fn.COUNT(ApplicationR.id).alias('cnt'),
                            ApplicationR.cycle)
                    .where(
                     ApplicationR.status << [ApplicationStatus.PROCESSING.value,
                                             ApplicationStatus.AB_TEST.value],
                     ApplicationR.cycle << [1, 2, 3, 4],
                     ApplicationR.promised_date == next_date)
                    .group_by(ApplicationR.cycle))
        next_follow = next_ptp.where(fn.EXISTS(sub))

        active = cal_date - timedelta(days=5)
        bombers = BomberR.filter(BomberR.last_active_at > active,
                                 BomberR.role << [1, 4, 5, 6, 8])
        bomber_cycle = {bomber.id: bomber.role.cycle for bomber in bombers}
        for i in bomber_amount_cnt:
            cycle = bomber_cycle.get(i[0], 0)
            if cycle in Cycle.values():
                summary[cycle]['cleared'] += int(i[2]) if i[2] else 0
                summary[cycle]['amount_recovered'] += int(i[1]) if i[1] else 0

        for i in bomber_call_made_connect_case:
            cycle = bomber_cycle.get(i[0], 0)
            if cycle in Cycle.values():
                summary[cycle]['call_made'] += int(i[1]) if i[1] else 0
                summary[cycle]['call_connected'] += int(i[3]) if i[3] else 0

        for i in ptp_bomber_amount_cnt:
            cycle = bomber_cycle.get(i[0], 0)
            if cycle in Cycle.values():
                summary[cycle]['promised'] += int(i[2]) if i[2] else 0

        for i in sms_sent:
            cycle = bomber_cycle.get(i.operator.id, 0)
            if cycle in Cycle.values():
                summary[cycle]['sms_sent'] += int(i.sms_sent)

        for i in today:
            cycle = i.cycle
            if cycle in Cycle.values():
                summary[cycle]['PTP_Due_Today'] += int(i.cnt)

        for i in next_ptp:
            cycle = i.cycle
            if cycle in Cycle.values():
                summary[cycle]['PTP_Due_nextday'] += int(i.cnt)

        for i in today_follow:
            cycle = i.cycle
            if cycle in Cycle.values():
                summary[cycle]['PTP_Due_Today_followed'] += int(i.cnt)

        for i in next_follow:
            cycle = i.cycle
            if cycle in Cycle.values():
                summary[cycle]['PTP_Due_nextday_followed'] += int(i.cnt)

        for key, value in total_call_duraction.items():
            cycle = bomber_cycle.get(key, 0)
            if cycle in Cycle.values():
                summary[cycle]['total_call_duraction'] += value

        for i in new_case:
            cycle = i[0]
            if cycle in Cycle.values():
                summary[cycle]['new_case'] += int(i[1])

        for i in new_case_followed:
            cycle = bomber_cycle.get(i[0], 0)
            if cycle in Cycle.values():
                summary[cycle]['new_followed_cnt'] += int(i[1])

        for i in c1_new_case_followed:
            cycle = i[0]
            if cycle in Cycle.values():
                summary[cycle]['new_followed_cnt'] += int(i[1])

        for i in works:
            cycle = bomber_cycle.get(i.id, 0)
            if cycle in Cycle.values():
                summary[cycle]['work_cnt'] += 1

    if start_date and end_date:

        history = history.where(
            SummaryBomber.time >= start_date,
            SummaryBomber.time < end_date,
        )

        # 查询的日期维度包含当天时需要排除掉当天的数据
        paid_total = get_paid_total(start_date, end_date)

    history = history.group_by(SummaryBomber.cycle)
    claimed = new_claimed(start_date, end_date, new_case)

    # 当日期格式输入不完整时默认显示当天数据
    if not start_date and not end_date:
        history = []

    for i in history:
        if i.bomber_id == Cycle.M3.value:
            continue
        summary[i.bomber_id]['new_case'] += int(i.new_case_cnt)
        summary[i.bomber_id]['new_followed_cnt'] += int(i.new_case_call_cnt)
        summary[i.bomber_id]['PTP_Due_Today'] += int(i.ptp_today_cnt)
        summary[i.bomber_id]['PTP_Due_Today_followed'] += int(i.ptp_today_call)
        summary[i.bomber_id]['PTP_Due_nextday'] += int(i.ptp_next_cnt)
        summary[i.bomber_id]['PTP_Due_nextday_followed'] += int(i.ptp_next_call)
        summary[i.bomber_id]['call_made'] += int(i.call_made)
        summary[i.bomber_id]['call_connected'] += int(i.call_connected)
        summary[i.bomber_id]['sms_sent'] += int(i.sms_sent)
        summary[i.bomber_id]['promised'] += int(i.promised)
        summary[i.bomber_id]['cleared'] += int(i.cleared)
        summary[i.bomber_id]['amount_recovered'] += int(i.amount_recovered)
        summary[i.bomber_id]['total_call_duraction'] += int(i.calltime_sum)

    for i in claimed:
        if i[0] in summary.keys():
            summary[i[0]]['claimed'] += int(i[1])
    for i in paid_total:
        if i[0] in summary.keys():
            summary[i[0]]['amount_recovered_total'] += int(i[1])

    # 如果 区间 不包含 today 则不计算当天数据 直接返回历史数据
    # if start_date and end_date and end_date < cal_date:
    result = []
    for cycle, data in summary.items():
        if cycle == Cycle.M3.value:
            continue
        new_followed_rate = (data['new_followed_cnt'] / data['new_case']
                             if data['new_case'] else 1)
        new_followed_rate = 1 if new_followed_rate > 1 else new_followed_rate
        new_followed_rate = str(round(new_followed_rate * 100, 2)) + '%'

        today_due = data['PTP_Due_Today']
        today_follow = data['PTP_Due_Today_followed']
        today_follow_rate = today_follow / today_due if today_due else 1
        today_follow_rate = 1 if today_follow_rate > 1 else today_follow_rate
        today_follow_rate = str(round(today_follow_rate * 100, 2)) + '%'

        next_due = data['PTP_Due_nextday']
        next_follow = data['PTP_Due_nextday_followed']
        next_followed_rate = next_follow / next_due if next_due else 1
        next_followed_rate = 1 if next_followed_rate > 1 else next_followed_rate
        next_followed_rate = str(round(next_followed_rate * 100, 2)) + '%'

        result.append({
                'cycle': str(data['cycle']),
                'claimed': str(data['claimed']),
                'new_case': str(data['new_case']),
                'new_followed_rate': new_followed_rate,
                'PTP_Due_Today_followed_rate': today_follow_rate,
                'PTP_Due_nextday_followed_rate': next_followed_rate,
                'call_made': str(data['call_made']),
                'call_connected': str(data['call_connected']),
                'sms_sent': str(data['sms_sent']),
                'promised': str(data['promised']),
                'cleared': str(data['cleared']),
                'amount_recovered': str(data['amount_recovered']),
                'amount_recovered_total': str(data['amount_recovered_total']),
                'total_call_duraction': str(data['total_call_duraction']),
                'work_cnt': str(data['work_cnt'])
            })
    if 'export' in args and args.export == '1':
        response.set_header('Content-Type', 'text/csv')
        response.set_header('Content-Disposition',
                            'attachment; filename="bomber_export.csv"')

        with StringIO() as csv_file:
            fields = (
                'cycle', 'claimed', 'new_case', 'new_followed_rate',
                'PTP_Due_Today_followed_rate', 'PTP_Due_nextday_followed_rate',
                'call_made', 'call_connected', 'sms_sent', 'promised',
                'cleared', 'amount_recovered', 'amount_recovered_total',
                'total_call_duraction', 'work_cnt')
            w = csv.DictWriter(csv_file, fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(result)
            return csv_file.getvalue().encode('utf8', 'ignore')
    return {'data': result}

@time_logger
def get_unfollowed(begin_date, bomber_id=None):
    if bomber_id:
        in_clause = "and bomber_id = %s" % bomber_id
    else:
        in_clause = ""
    sql = """
        SELECT
            b.bomber_id,
            count(DISTINCT b.application_id)
        FROM
            (
                SELECT
                    bdh.application_id,
                    bdh.bomber_id
                FROM
                    bomber.dispatch_app_history bdh
                WHERE
                    entry_at > '%(begin_date)s'
                %(in_clause)s
                AND entry_at < date_add('%(begin_date)s', INTERVAL 1 DAY)
                UNION
                SELECT
                    bdh.application_id,
                    bdh.bomber_id
                FROM
                    bomber.dispatch_app_history bdh
                WHERE
                    entry_at < '%(begin_date)s'
                %(in_clause)s
                AND out_at IS NULL
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        bomber.call_actions bc
                    WHERE
                        bdh.application_id = bc.application_id
                    AND bdh.bomber_id = bc.bomber_id
                    AND bc.created_at <
                        date_add('%(begin_date)s', INTERVAL 1 DAY)
                )
            ) b
        GROUP BY
            b.bomber_id;
    """ % {'begin_date': begin_date, 'in_clause': in_clause}
    data = run_all_sql(sql)

    result = defaultdict(int)
    for d in data:
        result[d[0]] += d[1]
    return result
