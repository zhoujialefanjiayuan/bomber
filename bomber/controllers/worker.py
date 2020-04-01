from bottle import post, get, request

from bomber.auth import get_api_user
from bomber.utils import plain_forms
from bomber.worker import (
    old_loan_application,
    cron_summary,
)
from bomber.worker import (
    bomber_dispatch_applications,
    recover_rate_week_money_into,
    calc_overdue_days_over,
    bomber_clear_overdue_ptp,
    bomber_auto_call_list,
    application_overdue,
    update_summary_new,
    summary_daily_data,
    summary_new_cycle,
    calc_overdue_days,
    summary_create,
    repair_bomber,
    bill_cleared,
    summary_new,
    modify_bill,
    bill_paid
)

from bomber.dispatch_app import mba_dispatch_apps

@post('/api/v1/bill-cleared')
def post_bill_cleared():
    get_api_user()
    bill_cleared(plain_forms(), None)
    return


@get('/api/v1/app_merge')
def get_app_merge():
    get_api_user()
    bomber_auto_call_list(None, None)
    return 'success'


@get('/api/v1/check')
def check():
    return 'check'


@post('/api/v1/application/overdue')
def post_application_overdue():
    get_api_user()
    application_overdue(request.json, None)
    return


@post('/api/v1/bill-paid')
def check():
    get_api_user()
    bill_paid(request.json, None)
    return 'done'


@post('/api/v1/cron/summary')
def post_cron_summary():
    get_api_user()
    cron_summary(None, None)


@post('/api/v1/old/loan/application')
def post_old_loan_application():
    get_api_user()
    old_loan_application(request.json, None)


@get('/api/v1/test_recover_rate')
def check():
    get_api_user()
    recover_rate_week_money_into(None, None)
    return 'success'


@get('/api/v1/test_summary_create')
def summary():
    get_api_user()
    summary_create(None, None)
    return 'success'


@get('/api/v1/test_summary_bomber')
def check_summary():
    get_api_user()
    summary_new(None, None)
    return 'success'


@get('/api/v1/test_update_summary_bomber')
def check_update_summary_new():
    get_api_user()
    update_summary_new(None, None)
    return 'success'


@get('/api/v1/test_summary_new_cycle')
def check_summary_new_cycle():
    get_api_user()
    summary_new_cycle(None, None)
    return 'success'


@get('/api/v1/calc_overdue_days')
def check_calc_overdue_days():
    get_api_user()
    calc_overdue_days(None, None)
    return 'success'


@get('/api/v1/calc_overdue_days_over')
def check_calc_overdue_days_over():
    get_api_user()
    calc_overdue_days_over(None, None)
    return 'success'


@post('/api/v1/bomber/clear/overdue')
def clear_overdue():
    get_api_user()
    bomber_clear_overdue_ptp(None, None)



@post('/api/v1/repair/bomber')
def test_repair_bomber():
    get_api_user()
    repair_bomber(request.json, None)


@post('/api/v1/modify/bill')
def test_modify_bill():
    get_api_user()
    modify_bill(request.json, None)


@get('/api/v1/dispatch_apps')
def dispatch_apps():
    get_api_user()
    bomber_dispatch_applications()

@get('/api/v1/summary_daily')
def summary_daily():
    get_api_user()
    summary_daily_data(None,None)

@get('/api/v1/mba')
def mba_diapatch():
    get_api_user()
    mba_dispatch_apps()

