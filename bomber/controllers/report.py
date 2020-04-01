import calendar
import logging
import json
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal
from peewee import fn, JOIN_LEFT_OUTER
from collections import defaultdict

from bottle import get, abort

from bomber.constant_mapping import (
    AutoCallResult, RoleWeight, ApplicationStatus, RipeInd, Cycle, GroupCycle)
from bomber.db import readonly_db
from bomber.models import AutoCallActions, RepaymentReport
from bomber.models import (
    RepaymentReportInto,
    ReportCollection,
    SummaryDaily,
    SummaryBomber,
    RepaymentLog,
    Application,
    Bomber,
    Role,
)
from bomber.utils import plain_query, get_permission, strptime
from bomber.validator import (
    report_collections_date_validator,
    report_summary_validator
)
from bomber.controllers.summary import new_claimed


@get('/api/v1/bomber/<bomber_id:int>/repayment-amount/summary')
def bomber_repayment_amount_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    repayment_log = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.cleared_amount.alias('data'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
    )

    summary = summary_for_repayment(repayment_log, category, start_date,
                                    end_date)

    return summary


bomber_repayment_amount_summary.permission = get_permission('report', 'member')


@get('/api/v1/bomber/<bomber_id:int>/ptp/summary')
def bomber_ptp_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.promised_cnt.alias('data'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = \
            calendar.monthrange(end_date.year, end_date.month)[1]
        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        # Put default days.
        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday() != 0:
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    summary = OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


bomber_ptp_summary.permission = get_permission('report', 'member')


@get('/api/v1/bomber/<bomber_id:int>/calltime/summary')
def bomber_calltime_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.calltime_sum.alias('data'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = \
            calendar.monthrange(end_date.year, end_date.month)[1]
        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        # Put default days.
        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday() != 0:
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    summary = OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


bomber_calltime_summary.permission = get_permission('report', 'member')


@get('/api/v1/bomber/<bomber_id:int>/calls/summary')
def bomber_calls_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # 2. Query summary table first.

    # 3. If data doesn't exist, run calculate function.
    # 4. If data is exist, just return.

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.call_cnt.alias('data'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = \
            calendar.monthrange(end_date.year, end_date.month)[1]
        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        # Put default days.
        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday():
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


bomber_ptp_summary.permission = get_permission('report', 'member')


@get('/api/v1/bomber/<bomber_id:int>/connect/summary')
def bomber_connect_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # 2. Query summary table first.

    # 3. If data doesn't exist, run calculate function.
    # 4. If data is exist, just return.

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.call_connect_cnt.alias('data'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = \
            calendar.monthrange(end_date.year, end_date.month)[1]
        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        # Put default days.
        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday():
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{0}-W{1}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


bomber_connect_summary.permission = get_permission('report', 'member')


@get('/api/v1/bomber/<bomber_id:int>/follow_rate/summary')
def bomber_connect_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]

    history = (
        SummaryBomber
        .select(SummaryBomber.time,
                fn.SUM(SummaryBomber.unfollowed_cnt).alias('new'),
                fn.SUM(SummaryBomber.unfollowed_call_cnt).alias('new_call'),
                fn.SUM(SummaryBomber.ptp_today_cnt).alias('today'),
                fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('today_call'),
                fn.SUM(SummaryBomber.ptp_next_cnt).alias('next_day'),
                fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('next_call'))
        .where(SummaryBomber.bomber_id == bomber_id,
               SummaryBomber.time >= start_date,
               SummaryBomber.time <= end_date)
        .group_by(SummaryBomber.time)
    )

    new, today, next_day, time = [], [], [], []

    for i in history:
        new_call = i.new_call
        new_rate = round((new_call / i.new
                          if i.new
                          else 1) * 100, 2)
        new_rate = new_rate if new_rate < 100 else 100
        new.append(str(new_rate))

        today_call = i.today_call
        today_rate = round((today_call / i.today if i.today else 1) * 100, 2)
        today_rate = today_rate if today_rate < 100 else 100
        today.append(str(today_rate))

        call = i.next_call
        next_rate = round((call / i.next_day if i.next_day else 1) * 100, 2)
        next_rate = next_rate if next_rate < 100 else 100
        next_day.append(str(next_rate))

        time.append(i.time.strftime('%Y-%m-%d'))

    result = {'new': new, 'today': today, 'next_day': next_day, 'date': time}
    return result


bomber_connect_summary.permission = get_permission('report', 'member')


def check_role_cycle_allow(bomber, cycle_id):
    role = Role().get(Role.id == bomber.role)
    if not ((role.weight == RoleWeight.DEPARTMENT.value) or
            (role.cycle == cycle_id)):
        abort(403, 'Forbidden, permission not allow')


def round2(*args):
    return round(sum(float(arg) for arg in args), 2)


@get('/api/v1/bomber/<cycle_id:int>/members-repayment-amount/summary')
def members_repayment_amount_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find member bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    member_summary = []

    edit_ended_date, edit_started_date = adjust_duration_date(category,
                                                              start_date,
                                                              end_date)

    # Find repayment log
    repayment_log = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.cleared_amount.alias('data'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_ended_date)
    )
    logging.debug('repayment_log: ' + str(repayment_log))

    if category == "month":

        # Adjust to start end day of month
        for each_bomber in bombers:
            work_ind = (SummaryBomber
                        .filter(SummaryBomber.bomber_id == each_bomber.id,
                                SummaryBomber.call_cnt > 0,
                                SummaryBomber.time >= start_date,
                                SummaryBomber.time <= end_date)).exists()
            if work_ind:
                year_month = None
                for single_date in date_range(edit_started_date,
                                              edit_ended_date):
                    year_month = single_date.strftime("%Y-%m")
                default_dict = {
                    "name": each_bomber.name,
                    "date": year_month,
                    "total": 0.0
                }
                member_summary.append(default_dict)

        for log in repayment_log:
            for each_dict in member_summary:
                if log.bomber_id.name == each_dict['name']:
                    each_dict['total'] = log.data
                    each_dict['total'] = round2(each_dict['total'])

    elif category == "week":

        for each_bomber in bombers:
            work_ind = (SummaryBomber
                        .filter(SummaryBomber.bomber_id == each_bomber.id,
                                SummaryBomber.call_cnt > 0,
                                SummaryBomber.time >= start_date,
                                SummaryBomber.time <= end_date)).exists()
            if work_ind:
                date_label = None
                # Put default days.
                for single_date in date_range(edit_started_date,
                                              edit_ended_date,
                                              add_one_day=1):
                    year_month = single_date.strftime("%Y-%m")
                    month_week = week_of_month(single_date)
                    date_label = '{}-W{}'.format(year_month, month_week)
                default_dict = {
                    "name": each_bomber.name,
                    "date": date_label,
                    "total": 0.0
                }
                member_summary.append(default_dict)

        for log in repayment_log:
            for each_dict in member_summary:
                if log.bomber_id.name == each_dict['name']:
                    each_dict['total'] = round2(each_dict['total'], log.data)
                    each_dict['total'] = round2(each_dict['total'])
    return member_summary


members_repayment_amount_summary.permission = get_permission('report',
                                                             'leader')


@get('/api/v1/bomber/<cycle_id:int>/members-ptp/summary')
def members_ptp_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.promised_cnt.alias('data'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
    )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    for bomber_name, member_phone_calls in member_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:

                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


members_ptp_summary.permission = get_permission('report', 'leader')


def update_call_action_members_rate(call_action, summary):
    cnt = call_action.all_cnt
    call = call_action.call_cnt
    rate = round((call / cnt if cnt else 1) * 100, 2)
    data = rate if rate < 100 else 100

    current_each_day_set = {
        "week_day": call_action.time.isoweekday(),
        "month_day": call_action.time.date().day,
        "date": call_action.time.strftime("%Y-%m-%d"),
        "total": data
    }
    found = False
    for each_day_set in summary:
        if each_day_set["date"] == current_each_day_set["date"]:
            each_day_set["total"] += current_each_day_set["total"]
            found = True
    if not found:
        summary.append(current_each_day_set)


@get('/api/v1/bomber/<cycle_id:int>/members-new/summary')
def members_new_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.unfollowed_cnt.alias('all_cnt'),
                SummaryBomber.unfollowed_call_cnt.alias('call_cnt'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
    )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    for bomber_name, member_phone_calls in member_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


members_new_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/members-next/summary')
def members_next_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.ptp_next_cnt.alias('all_cnt'),
                SummaryBomber.ptp_next_call_cnt.alias('call_cnt'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
    )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    for bomber_name, member_phone_calls in member_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


members_next_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/members-today/summary')
def members_today_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.ptp_today_cnt.alias('all_cnt'),
                SummaryBomber.ptp_today_call_cnt.alias('call_cnt'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
    )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    for bomber_name, member_phone_calls in member_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                update_call_action_members_rate(call_action, summary)

            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


members_today_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/members-calls/summary')
def members_calls_summary(bomber, cycle_id):
    cycle_id,group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))
    if group_id > 0 and cycle_id in GroupCycle.values():
        bombers = get_bombers(cycle_id, group_id)

    # bombers_dict = bomber_role_serializer.dump(bombers, many=True).data

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.call_cnt.alias('data'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
        )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    # 5. Run for-loop in given bomber.id and period of time.
    for bomber_name, member_phone_calls in member_summary.items():
        logging.debug('Phone call count: {}'
                      .format(str(len(member_phone_calls))))
        if category == "month":
            logging.debug('Start {} end {}'.format(str(edit_started_date),
                                                   str(edit_end_date)))
            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:

                logging.debug('Default date: {}'.format(
                    call_action.time.strftime("%Y-%m-%d")))
                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False

                for each_day_set in summary:
                    logging.debug('each_day_set: ' + str(each_day_set))
                    if each_day_set['date'] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

                logging.debug('after summary: ' + str(summary))
            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


@get('/api/v1/bomber/<cycle_id:int>/members-connected/summary')
def members_connected_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Find bombers
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id))

    if group_id > 0 and cycle_id == Cycle.C3.value:
        bombers = get_bombers(cycle_id, group_id)


    # bombers_dict = bomber_role_serializer.dump(bombers, many=True).data

    member_summary = OrderedDict()
    for each_bomber in bombers:
        work_ind = (SummaryBomber
                    .filter(SummaryBomber.bomber_id == each_bomber.id,
                            SummaryBomber.call_cnt > 0,
                            SummaryBomber.time >= start_date,
                            SummaryBomber.time <= end_date)).exists()
        if work_ind:
            member_summary[each_bomber.name] = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.case_connect_cnt.alias('data'),
                Bomber.name,
                SummaryBomber.bomber_id)
        .join(Bomber, on=SummaryBomber.bomber_id == Bomber.id)
        .where(SummaryBomber.bomber_id != cycle_id,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date,
               Bomber.group_id == group_id)
        )

    for phone_call in phone_calls:
        if phone_call.bomber_id.name not in member_summary:
            member_summary[phone_call.bomber_id.name] = []
        member_summary[phone_call.bomber_id.name].append(phone_call)

    # 5. Run for-loop in given bomber.id and period of time.
    for bomber_name, member_phone_calls in member_summary.items():
        logging.debug('Phone call count: {}'
                      .format(str(len(member_phone_calls))))
        if category == "month":
            logging.debug('Start {} end {}'.format(str(edit_started_date),
                                                   str(edit_end_date)))
            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:

                logging.debug('Default date: {}'.format(
                    call_action.time.strftime("%Y-%m-%d")))
                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            member_summary[bomber_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                current_each_day_set = {
                    "week_day": call_action.time.isoweekday(),
                    "month_day": call_action.time.date().day,
                    "date": call_action.time.strftime("%Y-%m-%d"),
                    "total": call_action.data
                }
                found = False

                for each_day_set in summary:
                    logging.debug('each_day_set: ' + str(each_day_set))
                    if each_day_set['date'] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

                logging.debug('after summary: ' + str(summary))
            member_summary[bomber_name] = summary

    # 6. Return final data.
    return member_summary


def adjust_duration_date(category, start_date, end_date):
    edit_started_date = None
    edit_end_date = None
    if category == "month":
        # Adjust to start first day of month
        edit_started_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day = calendar.monthrange(end_date.year, end_date.month)[1]

        edit_end_date = end_date.replace(day=last_day) + timedelta(days=1)

    elif category == "week":

        # Adjust to start first day of week
        edit_started_date = start_date
        while edit_started_date.weekday():
            edit_started_date -= timedelta(days=1)

        edit_end_date = end_date
        while edit_end_date.weekday() != 6:
            edit_end_date += timedelta(days=1)
    return edit_end_date, edit_started_date


members_calls_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-calls/summary')
def leader_calls_summary(bomber, cycle_id):
    cycle_id,group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)
    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id,group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.call_cnt.alias('data'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_calls_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-connected/summary')
def leader_connected_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.case_connect_cnt.alias('data'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_connected_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-new/summary')
def leader_new_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.unfollowed_cnt.alias('all_cnt'),
                SummaryBomber.unfollowed_call_cnt.alias('call_cnt'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0,
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_rate(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_rate(call, summary, summary_label)

    summary = update_call_actions_by_total_rate(summary)
    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_new_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-today/summary')
def leader_today_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.ptp_today_cnt.alias('all_cnt'),
                SummaryBomber.ptp_today_call_cnt.alias('call_cnt'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_rate(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_rate(call, summary, summary_label)
    summary = update_call_actions_by_total_rate(summary)
    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_today_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-next/summary')
def leader_next_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.ptp_next_cnt.alias('all_cnt'),
                SummaryBomber.ptp_next_call_cnt.alias('call_cnt'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_rate(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "cnt": 0,
                "call": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_rate(call, summary, summary_label)

    summary = update_call_actions_by_total_rate(summary)
    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_next_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-ptp/summary')
def leader_ptp_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]

    phone_calls = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.promised_cnt.alias('data'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(edit_started_date, edit_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.time.strftime("%Y-%m")
            month_week = week_of_month(call.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


leader_ptp_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<cycle_id:int>/leader-repayment-amount/summary')
def leader_repayment_amount_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # 获取本小组的成员
    bombers = get_bombers(cycle_id=cycle_id, group_id=group_id)
    bids = [b.id for b in bombers]


    repayment_log = (
        SummaryBomber
        .select(SummaryBomber.time.alias('time'),
                SummaryBomber.cleared_amount.alias('data'))
        .where(SummaryBomber.bomber_id << bids,
               SummaryBomber.cycle == cycle_id,
               SummaryBomber.time >= edit_started_date,
               SummaryBomber.time <= edit_end_date)
    )
    logging.debug('repayment_log: ' + repayment_log)

    summary = summary_for_repayment(repayment_log, category, edit_started_date,
                                    edit_end_date)

    return summary


leader_repayment_amount_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<bomber_id:int>/department-calls/summary')
def department_calls_summary(bomber_id):
    selected_bombers = Bomber.select(Bomber.id,
                                     Bomber.name,
                                     Role.weight).join(
        Role).where(
        Bomber.role == Role.id,
        Bomber.id == bomber_id
    ).first()
    if not selected_bombers:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    # 2. Query summary table first.

    ended_date, started_date = adjust_duration_date(category,
                                                    form_start_date,
                                                    form_end_date)

    # 3. If data doesn't exist, run calculate function.
    # 4. If data is exist, just return.
    if selected_bombers.role.weight == RoleWeight.DEPARTMENT.value:
        phone_calls = (
            AutoCallActions.filter(
                AutoCallActions.created_at >= started_date,
                AutoCallActions.created_at <= ended_date
            )
        )
    else:
        phone_calls = []

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":

        # Put default days.
        for single_date in date_range(started_date, ended_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.created_at.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(started_date, ended_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.created_at.strftime("%Y-%m")
            month_week = week_of_month(call.created_at)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


department_calls_summary.permission = get_permission('report', 'department')


@get('/api/v1/bomber/<bomber_id:int>/department-repayment-amount/summary')
def department_repayment_amount_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    ended_date, started_date = adjust_duration_date(category, form_start_date,
                                                    form_end_date)

    if bomber.role.weight == RoleWeight.DEPARTMENT.value:
        repayment_log = (
            RepaymentLog.filter(
                                RepaymentLog.current_bomber.is_null(False),
                                RepaymentLog.repay_at >= started_date,
                                RepaymentLog.repay_at <= ended_date,
                                )
        )
    else:
        repayment_log = []

    logging.debug('repayment_log count: '.format(str(len(repayment_log))))
    summary = summary_for_repayment(repayment_log, category, started_date,
                                    ended_date)

    return summary


department_repayment_amount_summary.permission = get_permission(
    'report', 'department')


@get('/api/v1/bomber/<bomber_id:int>/department-ptp/summary')
def department_ptp_summary(bomber_id):
    bomber = Bomber.filter(Bomber.id == bomber_id).first()
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    ended_date, started_date = adjust_duration_date(category, form_start_date,
                                                    form_end_date)

    phone_calls = []
    if bomber.role.weight == RoleWeight.DEPARTMENT.value:
        phone_calls = (
            AutoCallActions.filter(
                AutoCallActions.created_at >= started_date,
                AutoCallActions.created_at <= ended_date,
                AutoCallActions.result == AutoCallResult.PTP.value
            )
        )

    summary = OrderedDict()

    # 5. Run for-loop in given bomber.id and period of time.

    if category == "month":
        # Put default days.
        for single_date in date_range(started_date, ended_date):
            year_month = single_date.strftime("%Y-%m")
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if year_month not in summary:
                summary[year_month] = []
            summary[year_month].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.created_at.strftime("%Y-%m")
            update_call_actions_by_date(call, summary, year_month)
    elif category == "week":

        # Put default days.
        for single_date in date_range(started_date, ended_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)
            default = {
                "week_day": single_date.isoweekday(),
                "month_day": single_date.day,
                "date": single_date.strftime("%Y-%m-%d"),
                "total": 0
            }
            if summary_label not in summary:
                summary[summary_label] = []
            summary[summary_label].append(default)

        for call in phone_calls:
            # Bring date to year month
            year_month = call.created_at.strftime("%Y-%m")
            month_week = week_of_month(call.created_at)
            summary_label = '{}-W{}'.format(year_month, month_week)
            update_call_actions_by_date(call, summary, summary_label)

    # 6. Return final data.
    OrderedDict(sorted(summary.items(), key=lambda t: t[0]))
    return summary


department_ptp_summary.permission = get_permission('report', 'department')


@get('/api/v1/bomber/<bomber_id:int>/leaders-calls/summary')
def leaders_calls_summary(bomber_id):
    bomber = (Bomber
              .select(Bomber, Role)
              .join(Role)
              .where(
                  Bomber.id == bomber_id,
                  Bomber.role == Role.id
              )
              .first())
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find matching call actions
    if bomber.role.weight == RoleWeight.DEPARTMENT.value:
        phone_calls = (AutoCallActions
                       .select(AutoCallActions, Bomber, Role)
                       .join(Bomber)
                       .join(Role)
                       .where(
                              AutoCallActions.created_at >= edit_started_date,
                              AutoCallActions.created_at <= edit_end_date,
                              Bomber.role == Role.id
                              )
                       )
        logging.debug('phone_calls: ' + str(phone_calls))
    else:
        return

    cycles_summary = OrderedDict()
    for phone_call in phone_calls:
        cycle_name = 'Cycle ' + str(phone_call.cycle)
        # Find cycle
        if cycle_name not in cycles_summary:
            cycles_summary[cycle_name] = []
        cycles_summary[cycle_name].append(phone_call)

    # 5. Run for-loop in given bomber.id and period of time.
    if category == "month":
        # Adjust to start first day of month
        edit_started_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day = calendar.monthrange(end_date.year, end_date.month)[1]

        edit_end_date = end_date.replace(day=last_day) + timedelta(days=1)

    elif category == "week":

        # Adjust to start first day of week
        edit_started_date = start_date
        while edit_started_date.weekday():
            edit_started_date -= timedelta(days=1)

        edit_end_date = end_date
        while edit_end_date.weekday() != 6:
            edit_end_date += timedelta(days=1)

    for cycle_name, member_phone_calls in cycles_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:

                current_each_day_set = {
                    "week_day": call_action.created_at.isoweekday(),
                    "month_day": call_action.created_at.date().day,
                    "date": call_action.created_at.strftime("%Y-%m-%d"),
                    "total": 1
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            cycles_summary[cycle_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                current_each_day_set = {
                    "week_day": call_action.created_at.isoweekday(),
                    "month_day": call_action.created_at.date().day,
                    "date": call_action.created_at.strftime("%Y-%m-%d"),
                    "total": 1
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            cycles_summary[cycle_name] = summary

    # 6. Return final data.
    cycles_summary = OrderedDict(sorted(cycles_summary.items(),
                                        key=lambda t: t[0]))
    return cycles_summary


leaders_calls_summary.permission = get_permission('report', 'department')


@get('/api/v1/bomber/<bomber_id:int>/leaders-repayment-amount/summary')
def leaders_repay_amounts(bomber_id):
    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    start_date = form["start_date"]
    end_date = form["end_date"]
    category = form["category"]

    # Get selected bomber
    bomber = (Bomber
              .select(Bomber, Role)
              .join(Role)
              .where(Bomber.id == bomber_id,
                     Bomber.role == Role.id)
              .first())
    if not bomber:
        abort(404, 'bomber not found')

    cycles_summary = []

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            start_date,
                                                            end_date)

    # Find repayment log
    repayment_log = (RepaymentLog
                     .select(RepaymentLog, Bomber, Role)
                     .join(Bomber)
                     .join(Role)
                     .where(RepaymentLog.current_bomber == Bomber.id,
                            Bomber.role == Role.id,
                            RepaymentLog.created_at >= edit_started_date,
                            RepaymentLog.created_at <= edit_end_date
                            )
                     )
    logging.debug('repayment_log: ' + str(repayment_log))

    if category == "month":
        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = \
            calendar.monthrange(end_date.year, end_date.month)[1]

        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        for log in repayment_log:

            cycle_name = 'Cycle ' + str(log.cycle)
            # Find cycle
            not_found = True
            for each_repayment_set in cycles_summary:
                if cycle_name == each_repayment_set['name']:
                    not_found = False

            if not_found:
                year_month = None
                for single_date in date_range(start_date, end_date):
                    year_month = single_date.strftime("%Y-%m")
                default_dict = {
                    "name": cycle_name,
                    "date": year_month,
                    "total": 0.0
                }
                cycles_summary.append(default_dict)

            for each_dict in cycles_summary:
                if cycle_name == each_dict['name']:
                    each_dict['total'] = round2(each_dict['total'],
                                                log.principal_part,
                                                log.late_fee_part)

    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday() != 0:
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        for log in repayment_log:
            cycle_name = 'Cycle ' + str(log.cycle)

            # Find cycle
            not_found = True
            for each_repayment_set in cycles_summary:
                if cycle_name == each_repayment_set['name']:
                    not_found = False

            if not_found:
                summary_label = None
                for single_date in date_range(adjust_start_date,
                                              adjust_end_date,
                                              add_one_day=1):
                    year_month = single_date.strftime("%Y-%m")
                    month_week = week_of_month(single_date)
                    summary_label = '{}-W{}'.format(year_month, month_week)
                default_dict = {
                    "name": cycle_name,
                    "date": summary_label,
                    "total": 0.0
                }
                cycles_summary.append(default_dict)

            for each_dict in cycles_summary:
                if cycle_name == each_dict['name']:
                    each_dict['total'] = round2(each_dict['total'],
                                                log.principal_part,
                                                log.late_fee_part)

    cycles_summary = sorted(cycles_summary, key=lambda k: k['name'])
    return cycles_summary


leaders_repay_amounts.permission = get_permission('report', 'department')


@get('/api/v1/bomber/<bomber_id:int>/leaders-ptp/summary')
def leaders_ptp_summary(bomber_id):
    bomber = (Bomber
              .select(Bomber, Role)
              .join(Role)
              .where(
                     Bomber.id == bomber_id,
                     Bomber.role == Role.id
              )
              .first())
    if not bomber:
        abort(404, 'bomber not found')

    # 1. Prepare each metric query method.
    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # Find matching call actions
    if bomber.role.weight == RoleWeight.DEPARTMENT.value:
        phone_calls = (AutoCallActions
                       .select(AutoCallActions, Bomber, Role)
                       .join(Bomber, on=AutoCallActions.bomber == Bomber.id)
                       .join(Role, on=Bomber.role == Role.id)
                       .where(
                              AutoCallActions.created_at >= edit_started_date,
                              AutoCallActions.created_at <= edit_end_date,
                              AutoCallActions.result ==
                              AutoCallResult.PTP.value
                              )
                       )
        logging.debug('phone_calls: ' + str(phone_calls))
    else:
        return

    cycles_summary = OrderedDict()
    for phone_call in phone_calls:
        cycle_name = 'Cycle ' + str(phone_call.cycle)
        # Find cycle
        if cycle_name not in cycles_summary:
            cycles_summary[cycle_name] = []
        cycles_summary[cycle_name].append(phone_call)

    for cycle_name, member_phone_calls in cycles_summary.items():
        if category == "month":

            summary = get_default_days_array(edit_started_date, edit_end_date)

            for call_action in member_phone_calls:

                current_each_day_set = {
                    "week_day": call_action.created_at.isoweekday(),
                    "month_day": call_action.created_at.date().day,
                    "date": call_action.created_at.strftime("%Y-%m-%d"),
                    "total": 1
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            cycles_summary[cycle_name] = summary

        elif category == "week":
            summary = get_default_weeks_array(edit_started_date,
                                              edit_end_date)

            for call_action in member_phone_calls:
                current_each_day_set = {
                    "week_day": call_action.created_at.isoweekday(),
                    "month_day": call_action.created_at.date().day,
                    "date": call_action.created_at.strftime("%Y-%m-%d"),
                    "total": 1
                }
                found = False
                for each_day_set in summary:
                    if each_day_set["date"] == current_each_day_set["date"]:
                        each_day_set["total"] += current_each_day_set["total"]
                        found = True
                if not found:
                    summary.append(current_each_day_set)

            cycles_summary[cycle_name] = summary

    # 6. Return final data.
    cycles_summary = OrderedDict(sorted(cycles_summary.items(),
                                        key=lambda t: t[0]))
    return cycles_summary


leaders_ptp_summary.permission = get_permission('report', 'department')


def find_leader(bomber, leaders):
    for each_leader in leaders:
        if bomber.role.cycle == each_leader.role.cycle:
            return each_leader
    return None


def summary_for_repayment(repayment_log, category, start_date, end_date):
    summary = []
    if category == "month":
        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = calendar.monthrange(end_date.year, end_date.month)[1]

        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            if not any(year_month == each_dict['date']
                       for each_dict in summary):
                summary.append({
                    "date": year_month,
                    "total": 0.0,
                    "paid_count": 0
                })

        for log in repayment_log:
            # Bring date to year month
            year_month = log.time.strftime("%Y-%m")

            for date_dict in summary:
                if year_month == date_dict['date']:
                    total_in = log.data
                    logging.debug('before total: ' + str(date_dict['total']))
                    logging.debug('total_in: ' + str(total_in))
                    date_dict["paid_count"] += 1
                    date_dict['total'] += total_in
                    date_dict['total'] = round2(date_dict['total'])
                    logging.debug('after total: ' + str(date_dict['total']))

    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday() != 0:
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)

            if not any(summary_label == each_dict['date']
                       for each_dict in summary):
                summary.append({
                    "date": summary_label,
                    "total": 0.0,
                    "paid_count": 0
                })

        for log in repayment_log:
            # Bring date to year month
            year_month = log.time.strftime("%Y-%m")
            month_week = week_of_month(log.time)
            summary_label = '{}-W{}'.format(year_month, month_week)
            for date_dict in summary:
                if summary_label == date_dict['date']:
                    date_dict["paid_count"] += 1
                    date_dict['total'] += log.data
                    date_dict['total'] = round2(date_dict['total'])
    return summary


def summary_for_application(application, category, start_date, end_date):
    summary = []
    if category == "month":
        # Adjust to start first day of month
        start_date = start_date.replace(day=1)

        # Adjust to start end day of month
        last_day_month = calendar.monthrange(end_date.year, end_date.month)[1]

        end_date = end_date.replace(day=last_day_month) + timedelta(days=1)

        for single_date in date_range(start_date, end_date):
            year_month = single_date.strftime("%Y-%m")
            if not any(year_month == each_dict['date']
                       for each_dict in summary):
                summary.append({
                    "date": year_month,
                    "bomber_total": 0.0,
                    "bomber_count": 0
                })

        for log in application:
            # Bring date to year month
            year_month = log.created_at.strftime("%Y-%m")

            for date_dict in summary:
                if year_month == date_dict['date']:
                    total_in = round2(log.amount)
                    logging.debug('before total: ' +
                                  str(date_dict['bomber_total']))
                    logging.debug('total_in: ' + str(total_in))
                    date_dict["bomber_count"] += 1
                    date_dict['bomber_total'] += total_in
                    date_dict['bomber_total'] = round2(
                        date_dict['bomber_total'])
                    logging.debug('after total: ' +
                                  str(date_dict['bomber_total']))

    elif category == "week":

        # Adjust to start first day of week
        adjust_start_date = start_date
        while adjust_start_date.weekday() != 0:
            adjust_start_date -= timedelta(days=1)

        adjust_end_date = end_date
        while adjust_end_date.weekday() != 6:
            adjust_end_date += timedelta(days=1)

        # Put default days.
        for single_date in date_range(adjust_start_date, adjust_end_date,
                                      add_one_day=1):
            year_month = single_date.strftime("%Y-%m")
            month_week = week_of_month(single_date)
            summary_label = '{}-W{}'.format(year_month, month_week)

            if not any(summary_label == each_dict['date']
                       for each_dict in summary):
                summary.append({
                    "date": summary_label,
                    "bomber_total": 0.0,
                    "bomber_count": 0
                })

        for log in application:
            # Bring date to year month
            year_month = log.created_at.strftime("%Y-%m")
            month_week = week_of_month(log.created_at)
            summary_label = '{}-W{}'.format(year_month, month_week)
            for date_dict in summary:
                if summary_label == date_dict['date']:
                    date_dict['bomber_count'] += 1
                    date_dict['bomber_total'] += round2(log.amount)
                    date_dict['bomber_total'] = \
                        round2(date_dict['bomber_total'])
    return summary


def date_range(start_date, end_date, add_one_day=0):
    for n in range((end_date - start_date).days + add_one_day):
        yield start_date + timedelta(n)


def update_call_actions_by_date(call_action, summary, summary_label):
    """By gaven label, check each day object, and pass into given label."""
    if summary_label not in summary:
        summary[summary_label] = []

    current_each_day_set = {
        "week_day": call_action.time.isoweekday(),
        "month_day": call_action.time.date().day,
        "date": call_action.time.strftime("%Y-%m-%d"),
        "total": call_action.data
    }
    found = False
    for each_day_set in summary[summary_label]:
        if each_day_set["date"] == current_each_day_set["date"]:
            each_day_set["total"] += current_each_day_set["total"]
            found = True
            break
    if not found:
        summary[summary_label].append(current_each_day_set)


def update_call_actions_by_rate(call_action, summary, summary_label):
    """By gaven label, check each day object, and pass into given label."""
    if summary_label not in summary:
        summary[summary_label] = []

    cnt = call_action.all_cnt
    call = call_action.call_cnt
    current_each_day_set = {
        "week_day": call_action.time.isoweekday(),
        "month_day": call_action.time.date().day,
        "date": call_action.time.strftime("%Y-%m-%d"),
        "cnt": cnt,
        "call": call,
    }
    found = False
    for each_day_set in summary[summary_label]:
        if each_day_set["date"] == current_each_day_set["date"]:
            each_day_set["cnt"] += current_each_day_set["cnt"]
            each_day_set["call"] += current_each_day_set["call"]
            found = True
            break
    if not found:
        summary[summary_label].append(current_each_day_set)

# 计算出最后的占比
def update_call_actions_by_total_rate(summary):
    for key,values in summary.items():
        for each_day_set in values:
            cnt = each_day_set.get("cnt")
            call = each_day_set.get("call")
            rate = round((call / cnt if cnt else 1) * 100, 2)
            each_day_set["total"] = rate if rate < 100 else 100
    return summary



def week_of_month(dt):
    """ Returns the week of the month for the specified date."""
    # https://stackoverflow.com/questions/3806473/python-week-number-of-the-month
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
    from math import ceil
    return int(ceil(adjusted_dom / 7.0))


def get_default_weeks(adjust_start_date, adjust_end_date):
    # Put default days.
    summary = OrderedDict()
    for single_date in date_range(adjust_start_date, adjust_end_date,
                                  add_one_day=1):
        year_month = single_date.strftime("%Y-%m")
        month_week = week_of_month(single_date)
        summary_label = '{0}-W{1}'.format(year_month, month_week)
        default = {
            "week_day": single_date.isoweekday(),
            "month_day": single_date.day,
            "date": single_date.strftime("%Y-%m-%d"),
            "total": 0
        }
        if summary_label not in summary:
            summary[summary_label] = []
        summary[summary_label].append(default)
    return summary


def get_default_days(start_date, end_date):
    summary = OrderedDict()
    # Put default days.
    for single_date in date_range(start_date, end_date):
        year_month = single_date.strftime("%Y-%m")
        default = {
            "week_day": single_date.isoweekday(),
            "month_day": single_date.day,
            "date": single_date.strftime("%Y-%m-%d"),
            "total": 0
        }
        if year_month not in summary:
            summary[year_month] = []
        summary[year_month].append(default)
    return summary


def get_default_weeks_array(adjust_start_date, adjust_end_date):
    # Put default days.
    summary = []
    for single_date in date_range(adjust_start_date, adjust_end_date,
                                  add_one_day=1):
        logging.debug('single_date: ' + str(single_date))
        default = {
            "week_day": single_date.isoweekday(),
            "month_day": single_date.day,
            "date": single_date.strftime("%Y-%m-%d"),
            "total": 0
        }
        summary.append(default)
    return summary


def get_default_days_array(start_date, end_date):
    summary = []
    # Put default days.
    for single_date in date_range(start_date, end_date):
        default = {
            "week_day": single_date.isoweekday(),
            "month_day": single_date.day,
            "date": single_date.strftime("%Y-%m-%d"),
            "total": 0
        }
        summary.append(default)
    return summary


@get('/api/v1/bomber/<cycle_id:int>/leader-overdue-amount/summary')
def leader_calls_summary(bomber, cycle_id):
    cycle_id, group_id = get_cycle_and_group_id(cycle_id)
    check_role_cycle_allow(bomber, cycle_id)

    form = report_summary_validator(plain_query())
    form_start_date = form["start_date"]
    form_end_date = form["end_date"]
    category = form["category"]

    edit_end_date, edit_started_date = adjust_duration_date(category,
                                                            form_start_date,
                                                            form_end_date)

    # Repayment record
    repayment_log = (
        RepaymentLog.filter(
            RepaymentLog.cycle == cycle_id,
            RepaymentLog.current_bomber.is_null(False),
            RepaymentLog.repay_at >= edit_started_date,
            RepaymentLog.repay_at <= edit_end_date,
        )
    )
    logging.debug('repayment_log: ' + repayment_log)

    repayment_summary = summary_for_repayment(repayment_log, category,
                                              edit_started_date, edit_end_date)

    # Total records
    application = (
        Application.filter(
            Application.cycle == cycle_id,
            Application.created_at >= edit_started_date,
            Application.created_at <= edit_end_date,
            Application.status << [
                    ApplicationStatus.PROCESSING.value,
                    ApplicationStatus.UNCLAIMED.value]

        )
    )
    logging.debug('application_log: ' + application)

    summary = summary_for_application(application, category,
                                      edit_started_date, edit_end_date)

    # Calculating ratio
    for rs in repayment_summary:
        for s in summary:
            if rs["date"] == s["date"]:
                rs.update(s)

        if rs["paid_count"]:
            count = float(rs["paid_count"]) / float(rs["bomber_count"])
        else:
            count = 0.00

        if rs["total"]:
            total = float(rs["total"]) / float(rs["bomber_total"])
        else:
            total = 0.00

        count_percent = float('{:.1f}'.format(Decimal(count * 100)))
        total_percent = float('{:.1f}'.format(Decimal(total * 100)))
        rs["count_percent"] = count_percent
        rs["total_percent"] = total_percent

        rs.update(rs)

    total_summary = repayment_summary

    # Calculate the maximum value of the chart y-axis
    max_bomber_count = max(total_summary, key=lambda x: x["bomber_count"])[
        "bomber_count"]
    max_count_percent = max(total_summary, key=lambda x: x["count_percent"])[
        "count_percent"]
    max_bomber_total = max(total_summary, key=lambda x: x["bomber_total"])[
        "bomber_total"]
    max_total_percent = max(total_summary, key=lambda x: x["total_percent"])[
        "total_percent"]

    summary_total = []
    for each in total_summary:
        each["max_bomber_count"] = max_bomber_count
        each["max_count_percent"] = max_count_percent
        each["max_bomber_total"] = max_bomber_total
        each["max_total_percent"] = max_total_percent
        each.update(each)
        summary_total.append(each)

    return summary_total


leader_calls_summary.permission = get_permission('report', 'leader')


@get('/api/v1/bomber/<start_date>/members/<end_date>')
def get_members(bomber, start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    result = []
    for cycle in range(1, 5):
        # 得到每种cycle的一个统计情况
        members = []
        repayments = []
        week = []
        proportion = []
        date = start

        while date <= (end - timedelta(days=6)):
            # member = get_week_members(date, cycle)
            # members.append(member)
            # repayment = get_week_repayment(date, cycle)
            # repayments.append(repayment)

            member = 0
            members.append(0)
            repayment = 0
            repayments.append(0)

            week.append(date.strftime('%m/%d'))
            if member:
                proportion.append(int((repayment/member) * 100))
            else:
                proportion.append(0)
            date += timedelta(days=7)
        result.append([week, members, repayments, proportion])
    return result


get_members.permission = get_permission('report', 'bombers')


def get_week_members(date, cycle):
    end = str(date + timedelta(days=1) - timedelta(seconds=1))
    week = str(date + timedelta(days=7) - timedelta(seconds=1))
    start = str(date)

    cycle_conf = {
        1: [4, 10],
        2: [11, 30],
        3: [31, 60],
        4: [61, 90],
    }

    # 周一时逾期达到本cycle的所有件
    week_members = '''
                   select count(user_id)
                   from repayment.overdue as d
                   where d.created_at between "%s" and "%s"
                   and d.overdue_days between "%d" and "%d"
                   ''' % (start, end, cycle_conf[cycle][0],
                          cycle_conf[cycle][1])

    # 周一所有件中因ptp存在不属于本cycle的件
    not_belong_ptp = '''
                         select count(distinct d.external_id)
                         from repayment.overdue as d
                         inner join bomber.auto_call_actions as b on
                         d.external_id = b.application_id
                         inner join repayment.bill2 as b2 on
                         b2.external_id = b.application_id
                         where d.created_at between "%s" and "%s"
                         and d.overdue_days between '%d' and '%d'
                         and b.created_at < "%s" and cycle != "%d" and
                         (b.promised_date > "%s" or (b.promised_date > "%s"
                         and b2.finished_at < b.promised_date));
                         ''' % (start, end, cycle_conf[cycle][0],
                                cycle_conf[cycle][1], start, cycle, week,
                                start)

    # 周一到周末逾期日期新进入本cycle的件
    new = '''
          select count(DISTINCT d.external_id) from repayment.overdue as d
          where d.created_at between "%s" and "%s" and d.overdue_days = "%d"
          ''' % (end, week, cycle_conf[cycle][0])

    # 新进件中因ptp存在不属于本cycle的件
    new_ptp = '''
              select count(distinct d.external_id) from repayment.overdue as d
              inner join bomber.auto_call_actions as b on
              d.external_id = b.application_id
              where d.created_at between "%s" and "%s" and
              d.overdue_days = "%d"
              and b.promised_date > "%s" and b.cycle != "%d"
              ''' % (end, week, cycle_conf[cycle][0], end, cycle)

    # 逾期日期处于其他cycle但是属于本cycle的件
    belong_ptp = '''
                 select count(DISTINCT d.external_id)
                 from repayment.overdue as d
                 inner join bomber.auto_call_actions as b on
                 d.external_id = b.application_id
                 where d.created_at between "%s" and "%s"
                 and d.overdue_days > "%d"
                 and b.created_at < "%s" and b.cycle = "%d"
                 and b.promised_date > "%s"
                 ''' % (start, end, cycle_conf[cycle][1], start, cycle, week)

    try:
        cursor = readonly_db.get_cursor()
        cursor.execute(week_members)
        members = cursor.fetchone()

        cursor.execute(not_belong_ptp)
        not_belong = cursor.fetchone()

        cursor.execute(new)
        get_new = cursor.fetchone()

        cursor.execute(new_ptp)
        get_new_ptp = cursor.fetchone()

        cursor.execute(belong_ptp)
        belong_ptp = cursor.fetchone()
        result = (members[0] + get_new[0] - get_new_ptp[0] -
                  not_belong[0] + belong_ptp[0]
                  )
        return result
    except Exception as e:
        logging.info('get members error: %s' % str(e))
        return 0


def get_week_repayment(date, cycle):
    week = str(date + timedelta(days=7) - timedelta(seconds=1))
    start = str(date)

    cycle_conf = {
        1: [4, 10],
        2: [11, 30],
        3: [31, 60],
        4: [61, 90],
    }

    sql = '''
          select count(DISTINCT id) from repayment.bill2 as b2
          where b2.finished_at between "%s" and "%s" and b2.overdue_days
          between "%s" and "%s"
          ''' % (start, week, cycle_conf[cycle][0], cycle_conf[cycle][1])
    try:
        cursor = readonly_db.get_cursor()
        cursor.execute(sql)
        repayment = cursor.fetchone()
        return repayment[0]
    except Exception as e:
        logging.info('get repayment error: %s' % str(e))
        return 0


@get('/api/v1/bomber/<start_date>/<type>/money/<end_date>/<contain>')
def get_money(bomber, start_date, type, end_date, contain):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    result = []
    for cycle in range(0,5):
        members,repayments,week,proportion = [],[],[],[]
        date = start
        if cycle in (0,1):
            contain_out = 0
        else:
            contain_out = contain
        while date <= (end - timedelta(days=6)):
            reports = (RepaymentReport.select()
                       .where(RepaymentReport.cycle == cycle,
                              RepaymentReport.time == date,
                              RepaymentReport.contain_out == contain_out))
            if int(type) != 2:
                reports = reports.where(RepaymentReport.type == type)
            money = repayment = pro = 0
            for report in reports:
                money += report.all_money
                repayment += report.repayment
            if int(money):
                pro = round(repayment/money*100, 2)
            week.append(date.strftime('%m/%d'))
            proportion.append(str(pro))
            members.append(str(money))
            repayments.append(str(repayment))
            date += timedelta(days=7)
        result.append([week, members, repayments, proportion])
    return result
            
get_money.permission = get_permission('report', 'bombers')

@get('/api/v1/bomber/<start_date>/<is_first>/money/<end_date>/<contain>/into')
def get_money_into(bomber, start_date, is_first, end_date, contain):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    ripe = RipeInd.RIPE.value
    if end > datetime.now():
        end = datetime.now()
    result = {0: [], 1: [], 2: [], 3: [], 4: [], 'all_month': []}
    while start <= end or start.month <= end.month and start.year <= end.year:
        month_begin = start.strftime('%Y-%m-%d')[:-3]
        if start.month == 1:
            month_end = (start + timedelta(days=28)).strftime('%Y-%m-%d')[:-3]
        elif start.month in (3, 5, 8, 10):
            month_end = (start + timedelta(days=30)).strftime('%Y-%m-%d')[:-3]
        else:
            month_end = (start + timedelta(days=31)).strftime('%Y-%m-%d')[:-3]

        for cycle in range(0, 5):
            # 得到每种cycle的一个统计情况
            contain_out = contain
            if cycle in (0, Cycle.C1A.value, Cycle.C3.value):
                contain_out = 1
            m_data = (RepaymentReportInto.select()
                      .where(RepaymentReportInto.cycle == cycle,
                             RepaymentReportInto.time >= month_begin,
                             RepaymentReportInto.time < month_end,
                             RepaymentReportInto.contain_out == contain_out,
                             RepaymentReportInto.is_first_loan == is_first,
                             RepaymentReportInto.ripe_ind == ripe)
                      .group_by(RepaymentReportInto.time)
                      .order_by(RepaymentReportInto.time)
                     )
            if is_first == '2':
                m_data = (RepaymentReportInto.select(
                          fn.DATE(RepaymentReportInto.time),
                          fn.SUM(RepaymentReportInto.all_money),
                          fn.SUM(RepaymentReportInto.repayment)
                          )
                          .where(RepaymentReportInto.cycle == cycle,
                                 RepaymentReportInto.time >= month_begin,
                                 RepaymentReportInto.time < month_end,
                                 RepaymentReportInto.contain_out == contain_out,
                                 RepaymentReportInto.ripe_ind == ripe)
                          .group_by(RepaymentReportInto.time)
                          .order_by(RepaymentReportInto.time)
                          )
                month_result = []
                for d_data in m_data:
                    month_pro = 0
                    if d_data.repayment:
                        month_pro = (d_data.repayment / d_data.all_money) * 100
                    month_result.append(str(month_pro)[:5])
                result[cycle].append({
                    'name': month_begin,
                    'type': 'line',
                    'data': month_result
                })
            else:
                month_result = []
                for d_data in m_data:
                    month_result.append(str(d_data.proportion))
                result[cycle].append({
                    'name': month_begin,
                    'type': 'line',
                    'data': month_result
                })
        result['all_month'].append(month_begin)
        if start.month == 1:
            start = start + timedelta(days=28)
        elif start.month in (3, 5, 8, 10):
            start = start + timedelta(days=30)
        else:
            start = start + timedelta(days=31)
    result[0].reverse()
    result[1].reverse()
    result[2].reverse()
    result[3].reverse()
    result[4].reverse()
    return result


get_money_into.permission = get_permission('report', 'bombers')


def get_all_money(date, cycle):
    end = str(date + timedelta(days=1) - timedelta(seconds=1))
    week = str(date + timedelta(days=7) - timedelta(seconds=1))
    start = str(date)

    cycle_conf = {
        1: [4, 10],
        2: [11, 30],
        3: [31, 60],
        4: [61, 90],
    }

    # 得到周一时逾期日期在本cycle的所有逾期件的金额
    old_sql = '''
              select sum(principal_pending + interest_pending +
              late_fee_pending)
              from (
                   select external_id,principal_pending,interest_pending
                   from repayment.overdue as d
                   where d.created_at between "%s" and "%s"
                        and d.overdue_days between "%d" and "%d"
                   ) as new
              left join (
                        select max(d.late_fee_pending) as late_fee_pending,
                               d.external_id
                        from repayment.overdue as d
                        where d.created_at between "%s" and "%s"
                        GROUP BY external_id
                        ) as late
              on new.external_id = late.external_id
              ''' % (start, end, cycle_conf[cycle][0], cycle_conf[cycle][1],
                     start, week)

    # 得到本周逾期日期数达到本cycle但实际不属于本cycle的件的金额
    old_ptp_not_sql = '''
                     select sum(principal_pending + interest_pending +
                              late_fee_pending)
                     from (
                          select d.external_id,d.principal_pending,
                                d.interest_pending
                          from repayment.overdue as d
                          inner join bomber.auto_call_actions as b on
                                d.external_id = b.application_id
                          inner join repayment.bill2 as b2 on
                                b2.external_id = b.application_id
                          where d.created_at between "%s" and "%s"
                                and d.overdue_days between "%d" and "%d"
                                and b.created_at < "%s" and cycle != "%d" and
                                (b.promised_date > "%s" or (b.promised_date >
                                "%s"
                                and b2.finished_at < b.promised_date))
                          ) as new
                     left join (
                               select max(d.late_fee_pending) as
                                      late_fee_pending,d.external_id
                               from repayment.overdue as d
                               where d.created_at between "%s" and "%s"
                               group by external_id
                               ) as late
                     on new.external_id = late.external_id
                     ''' % (start, end, cycle_conf[cycle][0],
                            cycle_conf[cycle][1], start, cycle, week, start,
                            start, week)
    # 得到本周每天新进入本cycle的件
    new_over_sql = '''
                   select sum(principal_pending + interest_pending +
                         late_fee_pending)
                  from (
                      select d.external_id,d.principal_pending,
                            d.interest_pending
                   from repayment.overdue as d
                   where d.created_at between "%s" and "%s" and
                   d.overdue_days = "%d"
                       ) as new
                  left join (
                            select max(d.late_fee_pending) as late_fee_pending,
                                   d.external_id
                            from repayment.overdue as d
                            where d.created_at between "%s" and "%s"
                            group by external_id
                            ) as late
                  on new.external_id = late.external_id
                   ''' % (end, week, cycle_conf[cycle][0], start, week)
    # 新进件中因ptp存在不属于本cycle的
    new_over_ptp_sql = '''
                       select sum(principal_pending + interest_pending +
                                  late_fee_pending)
                      from (
                          select d.external_id,d.principal_pending,
                          d.interest_pending
                       from repayment.overdue as d
                       inner join bomber.auto_call_actions as b on
                       d.external_id = b.application_id
                       where d.created_at between "%s" and "%s" and
                       d.overdue_days = "%d" and b.promised_date > "%s"
                       and b.cycle != "%d" and b.created_at < d.created_at
                           ) as new
                      left join (
                                select max(d.late_fee_pending) as
                                       late_fee_pending,d.external_id
                                from repayment.overdue as d
                                where d.created_at between "%s" and "%s"
                                group by external_id
                                ) as late
                      on new.external_id = late.external_id
                       ''' % (end, week, cycle_conf[cycle][0], end, cycle,
                              start, week)
    # 得到因ptp存在属于本cycle的金额
    ptp_belong_sql = '''
                     select sum(principal_pending + interest_pending +
                                late_fee_pending)
                     from (
                         select d.external_id,d.principal_pending,
                                d.interest_pending
                         from repayment.overdue as d
                         inner join bomber.auto_call_actions as b
                           on d.external_id = b.application_id
                         where d.created_at between "%s" and "%s"
                           and d.overdue_days > "%d" and b.created_at < "%s"
                           and b.cycle = "%d" and b.promised_date > "%s"
                       ) as new
                     left join (
                            select max(d.late_fee_pending) as late_fee_pending,
                                  d.external_id
                            from repayment.overdue as d
                            where d.created_at between "%s" and "%s"
                            group by external_id
                            ) as late
                     on new.external_id = late.external_id
                     ''' % (start, end, cycle_conf[cycle][1], start, cycle,
                            start, start, week)
    result = 0
    try:
        cursor = readonly_db.get_cursor()
        cursor.execute(old_sql)
        old_over = cursor.fetchone()[0]
        cursor.execute(old_ptp_not_sql)
        old_ptp_not = cursor.fetchone()[0]
        cursor.execute(new_over_sql)
        new_over = cursor.fetchone()[0]
        cursor.execute(new_over_ptp_sql)
        new_over_ptp = cursor.fetchone()[0]
        cursor.execute(ptp_belong_sql)
        ptp_belong = cursor.fetchone()[0]
        if old_sql:
            result += int(old_over)
        if new_over:
            result += int(new_over)
        if old_ptp_not:
            result += int(old_ptp_not)
        if new_over_ptp:
            result += int(new_over_ptp)
        if ptp_belong:
            result += int(ptp_belong)
        return result
    except Exception as e:
        logging.info('get money error: %s' % str(e))
        return result


# 得到每周所还金额
def get_repayment(date, cycle):
    week = str(date + timedelta(days=7) - timedelta(seconds=1))
    start = str(date)

    '''
    sql =
          select sum(amount)
          from (
               select user_id
               from repayment.overdue as d
               where d.created_at between "%s" and
               "%s" and d.overdue_days
               between "%d" and "%d"
               ) as new
            inner join (
                       select user_id,amount
                       from repayment.bill2_log
                       where created_at between "%s" and "%s"
                       )as d
            on new.user_id = d.user_id
           % (start, end, cycle_conf[cycle][0], cycle_conf[cycle][1], start
                 , week)

    new_sql =
              select sum(amount)
              from (
                   select d.user_id
                   from repayment.overdue as d
                   inner join bomber.auto_call_actions as b on
                   d.external_id = b.application_id
                   where d.created_at between "%s" and "%s" and
                   d.overdue_days = "%d"
                   ) as new
                inner join (
                           select user_id,amount
                           from repayment.bill2_log
                           where created_at between "%s" and "%s"
                           )as d
                on new.user_id = d.user_id
              % (start, end, cycle_conf[cycle][0], start, week)
    '''

    sql = '''
          SELECT sum(a.principal_part + a.late_fee_part)
          FROM bomber.repayment_log as a
          where a.created_at between '%s' and '%s' and cycle='%d'
                and a.current_bomber_id is not NULL
          ''' % (start, week, cycle)
    result = 0
    try:
        cursor = readonly_db.get_cursor()
        cursor.execute(sql)
        repayment = cursor.fetchone()[0]
        if repayment:
            result += int(repayment)
        return result
    except Exception as e:
        logging.info('get repayment error: %s' % str(e))
        return result


@get('/api/v1/bomber/collections')
def get_collections():
    form = report_collections_date_validator(plain_query())
    start_date = form.get("start_date")
    end_date = form.get("end_date")

    if start_date and end_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    new_case, claimed_cnt = defaultdict(list), defaultdict(list)
    case_made_rate, case_connect_rate = defaultdict(list), defaultdict(list)
    call_cnt, call_connect_cnt = defaultdict(list), defaultdict(list)
    call_case, call_connect_case = defaultdict(list), defaultdict(list)
    date = []
    cycles = [Cycle.C1A.value, Cycle.C1B.value, Cycle.C2.value, Cycle.C3.value]

    for cycle in cycles:
        history = (SummaryBomber.select(
                      SummaryBomber.time,
                      SummaryBomber.new_case_cnt,
                      SummaryBomber.claimed_cnt,
                      SummaryBomber.case_made_rate,
                      SummaryBomber.case_connect_rate,
                      SummaryBomber.call_cnt,
                      SummaryBomber.call_connect_cnt,
                      SummaryBomber.case_made_cnt,
                      SummaryBomber.case_connect_cnt)
                   .where(SummaryBomber.time >= start_date,
                          SummaryBomber.time < end_date,
                          SummaryBomber.cycle == SummaryBomber.bomber_id,
                          SummaryBomber.cycle == cycle)
                   .group_by(SummaryBomber.time)
                   .order_by(SummaryBomber.time))

        for h in history:
            new_case[cycle].append(str(h.new_case_cnt))
            claimed_cnt[cycle].append(str(h.claimed_cnt))
            case_made_rate[cycle].append(str(h.case_made_rate))
            case_connect_rate[cycle].append(str(h.case_connect_rate))
            call_cnt[cycle].append(str(h.call_cnt))
            call_connect_cnt[cycle].append(str(h.call_connect_cnt))
            call_case[cycle].append(str(h.case_made_cnt))
            call_connect_case[cycle].append(str(h.case_connect_cnt))
            if cycle == Cycle.C3.value:
                date.append(h.time.strftime('%Y-%m-%d'))

    return {'new_case': new_case, 'claimed_cnt': claimed_cnt, 'date': date,
            'made_rate': case_made_rate, 'connect_rate': case_connect_rate,
            'call_cnt': call_cnt, 'connect_cnt': call_connect_cnt,
            'call_case': call_case, 'call_connect_case': call_connect_case}


get_collections.permission = get_permission('report', 'collections')


@get('/api/v1/bomber/collections_table')
def get_collections_table():
    form = report_collections_date_validator(plain_query())
    start_date = form.get("start_date")
    end_date = form.get("end_date")

    if start_date and end_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    history = (SummaryBomber.select(
                SummaryBomber.cycle,
                fn.SUM(SummaryBomber.new_case_cnt).alias('new'),
                fn.SUM(SummaryBomber.new_case_call_cnt).alias('new_call'),
                fn.SUM(SummaryBomber.case_made_cnt).alias('case_made'),
                fn.SUM(SummaryBomber.case_connect_cnt).alias('connect_cnt'),
                fn.SUM(SummaryBomber.new_case_amount_sum).alias('amount_sum'),
                fn.SUM(SummaryBomber.new_case_cleared_sum).alias('cleared_sum'),
                fn.SUM(SummaryBomber.promised_cnt).alias('promised_cnt'),
                fn.SUM(SummaryBomber.ptp_today_cnt).alias('ptp_today'),
                fn.SUM(SummaryBomber.ptp_today_call_cnt).alias('today_call'),
                fn.SUM(SummaryBomber.ptp_next_cnt).alias('ptp_next'),
                fn.SUM(SummaryBomber.ptp_next_call_cnt).alias('next_call'))
               .where(SummaryBomber.time >= start_date,
                      SummaryBomber.time < end_date,
                      SummaryBomber.cycle == SummaryBomber.bomber_id)
               .group_by(SummaryBomber.cycle))

    claimed = new_claimed(start_date, end_date, [])

    summary = {
        i: {
            'cycle': i,
            'claimed': 0,
            'new': 0,
            'new_call': 0,
            'case_made': 0,
            'connect_cnt': 0,
            'amount_sum': 0,
            'cleared_sum': 0,
            'promised_cnt': 0,
            'ptp_today': 0,
            'today_call': 0,
            'ptp_next': 0,
            'next_call': 0,
        }
        for i in (1, 2, 3, 4)
    }

    for i in history:
        summary[i.cycle]['new'] += int(i.new)
        summary[i.cycle]['new_call'] += int(i.new_call)
        summary[i.cycle]['case_made'] += int(i.case_made)
        summary[i.cycle]['connect_cnt'] += int(i.connect_cnt)
        summary[i.cycle]['amount_sum'] += int(i.amount_sum)
        summary[i.cycle]['cleared_sum'] += int(i.cleared_sum)
        summary[i.cycle]['promised_cnt'] += int(i.promised_cnt)
        summary[i.cycle]['ptp_today'] += int(i.ptp_today)
        summary[i.cycle]['today_call'] += int(i.today_call)
        summary[i.cycle]['ptp_next'] += int(i.ptp_next)
        summary[i.cycle]['next_call'] += int(i.next_call)

    for i in claimed:
        if i[0] in summary.keys():
            summary[i[0]]['claimed'] += int(i[1])

    result = []
    for cycle, data in summary.items():
        new_followed_rate = (data['new_call'] / data['new']
                             if data['new'] else 1)
        new_followed_rate = str(round(new_followed_rate * 100, 2)) + '%'

        new_cleared_rate = (data['cleared_sum'] / data['amount_sum']
                            if data['amount_sum'] else 1)
        new_cleared_rate = str(round(new_cleared_rate * 100, 2)) + '%'

        today_due = data['ptp_today']
        today_follow = data['today_call']
        today_follow_rate = today_follow / today_due if today_due else 1
        today_follow_rate = 1 if today_follow_rate > 1 else today_follow_rate
        today_follow_rate = str(round(today_follow_rate * 100, 2)) + '%'

        next_due = data['ptp_next']
        next_follow = data['next_call']
        next_followed_rate = next_follow / next_due if next_due else 1
        next_followed_rate = 1 if next_followed_rate > 1 else next_followed_rate
        next_followed_rate = str(round(next_followed_rate * 100, 2)) + '%'

        # 得到 日均每人拨打电话数 和 日均每人接通电话数 数据
        call_connect = (SummaryBomber.select(
                           fn.SUM(SummaryBomber.call_cnt).alias('call_cnt'),
                           fn.SUM(SummaryBomber.call_connect_cnt
                                  ).alias('connect_cnt'),
                           SummaryBomber.work_ind)
                        .where(SummaryBomber.time >= start_date,
                               SummaryBomber.time < end_date,
                               SummaryBomber.cycle == SummaryBomber.bomber_id,
                               SummaryBomber.cycle == cycle))
        call, connect, count = 0, 0, 0
        for c in call_connect:
            call_rate = c.call_cnt / c.work_ind if c.work_ind else 0
            connect_rate = c.connect_cnt / c.work_ind if c.work_ind else 0
            call += call_rate
            connect += connect_rate
            count += 1

        result.append({
            'cycle': str(data['cycle']),
            'claimed': str(data['claimed']),
            'new_case': str(data['new']),
            'new_followed_rate': new_followed_rate,
            'case_made': str(data['case_made']),
            'connect_cnt': str(data['connect_cnt']),
            'new_cleared_rate': new_cleared_rate,
            'promised': str(data['promised_cnt']),
            'Today_followed_rate': today_follow_rate,
            'nextday_followed_rate': next_followed_rate,
            'call_average': str(round((call / count if count else 0), 2)),
            'connect_average': str(round((connect / count if count else 0), 2))
        })
    return result


get_collections_table.permission = get_permission('report', 'collections')

# C3增加了小组，要group_id分
def get_cycle_and_group_id(cycle_id):
    if isinstance(cycle_id,str):
        cycle_id = int(cycle_id)
    group_id = cycle_id // 10
    cycle_id = cycle_id % 10
    return cycle_id,group_id

# 获取该cycle下的小组成员
def get_bombers(cycle_id,group_id):
    bombers = (Bomber
               .select(Bomber, Role)
               .join(Role)
               .where(Role.cycle == cycle_id,
                      Bomber.group_id == group_id,
                      Bomber.is_del == 0))
    return bombers


#获取日常数据
@get('/api/v1/summary/daily/<search_date>')
def summary_daily_data(bomber, search_date):
    search_date = datetime.strptime(search_date, '%Y-%m-%d')
    search_date = search_date.date()
    # 获取用户团队用户
    bombers = (Bomber.select(Bomber.id,Bomber.username,
                             Role.cycle,Bomber.group_id)
                     .join(Role, JOIN_LEFT_OUTER,
                           on = Bomber.role == Role.id)
                     .where(Bomber.is_del == 0,
                            Bomber.role_id != bomber.role.id,
                            Bomber.password.is_null(False),
                            Bomber.partner.is_null()))
    if bomber.role_id not in [0, 13]:
        bombers = bombers.where(Bomber.group_id == bomber.group_id,
                                Role.cycle == bomber.role.cycle)

    bombers = bombers.dicts()
    bids = [b["id"] for b in bombers]
    # 获取统计的所有数据
    summary_dailys = (SummaryDaily.select(SummaryDaily.bomber_id,
                                          fn.SUM(SummaryDaily.call_cnt),
                                          fn.SUM(SummaryDaily.ptp_cnt),
                                          fn.SUM(SummaryDaily.repayment))
                                  .where(SummaryDaily.bomber_id << bids,
                                    SummaryDaily.summary_date == search_date)
                                  .group_by(SummaryDaily.bomber_id))
    summary_dailys_dict = {s.bomber_id:s for s in summary_dailys}
    cycle_keys = ['C1A','C1B','C2','C3','M3']
    result = {}
    for bo in bombers:
        cycle = bo["cycle"]
        if cycle not in GroupCycle.values():
            continue
        group_id = bo["group_id"]
        key = cycle_keys[cycle-1]
        if group_id > 0:
            key = key+'-%s'%group_id
        daily_dict = {
            "bomber_id": bo["id"],
            "username": bo["username"],
            "group_id": group_id,
            "cycle": cycle,
            "call_cnt": '0',
            "ptp_cnt": '0',
            "repayment": '0'
        }
        if bo["id"] in summary_dailys_dict:
            daily = summary_dailys_dict[bo["id"]]
            daily_dict["call_cnt"] = str(daily.call_cnt)
            daily_dict["ptp_cnt"] = str(daily.ptp_cnt)
            daily_dict["repayment"] = str(daily.repayment)
        if key in result:
            result[key].append(daily_dict)
        else:
            result[key] = [daily_dict]
    return result
summary_daily_data.permission = get_permission('report', 'daily')