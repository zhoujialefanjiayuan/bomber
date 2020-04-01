from datetime import datetime

from bottle import get, post, request, abort
from peewee import JOIN

from bomber.auth import check_api_user
from bomber.constant_mapping import (
    CallActionType,
    AutoListStatus,
    SpecialBomber,
    Cycle
)
from bomber.plugins import ip_whitelist_plugin
from bomber.db import db
from bomber.models import (
    ApplicationStatus,
    BombingHistory,
    AutoCallList,
    Application,
    CallActions,
    Bomber,
)
from bomber.serializers import (
    bombing_history_serializer,
    call_history_serializer,
)
from bomber.validator import collection_validator, cs_ptp_validator
from bomber.controllers.asserts import set_ptp_for_special_bomber
from bomber.utils import get_cycle_by_overdue_days


@get('/api/v1/applications/<app_id:int>/collection_history')
def get_collection_history(bomber, application):
    result = (CallActions.select(CallActions, Bomber)
              .join(Bomber, JOIN.INNER, on=(CallActions.bomber_id == Bomber.id)
                    .alias('operator'))
              .where(CallActions.application == application.id)
              .order_by(-CallActions.created_at))
    return call_history_serializer.dump(result, many=True).data


@get('/api/v1/applications/<app_id:int>/user_collection_history')
def get_user_collection_history(bomber, application):
    bombing_history = BombingHistory.filter(
        BombingHistory.application != application.id,
        BombingHistory.user_id == application.user_id,
    ).order_by(-BombingHistory.created_at)
    return bombing_history_serializer.dump(bombing_history, many=True).data


@post('/api/v1/applications/<app_id:int>/collection_history')
def add_collection_history(bomber, application):
    form = collection_validator(request.json)
    with db.atomic():
        promised_date = (form['promised_date']
                         if 'promised_date' in form else None)
        remark = form['remark'] if 'remark' in form else None
        promised_amount = (form['promised_amount']
                           if 'promised_amount' in form
                           else None)
        if promised_date:
            real_cycle = get_cycle_by_overdue_days(application.overdue_days)
            if real_cycle > application.cycle:
                abort(400, 'Can not extend PTP')
        bombing_history = BombingHistory.create(
            application=application.id,
            user_id=application.user_id,
            ektp=application.id_ektp,
            cycle=application.cycle,
            bomber=bomber.id,
            promised_amount=promised_amount,
            promised_date=promised_date,
            follow_up_date=form['follow_up_date'],
            result=form['result'],
            remark=remark,
        )
        CallActions.create(
            cycle=application.cycle,
            bomber_id=bomber.id,
            call_record_id=bombing_history.id,
            application=application.id,
            note=remark,
            promised_amount=promised_amount,
            promised_date=promised_date,
            follow_up_date=form['follow_up_date']
        )
        if 'promised_amount' in form:
            application.promised_amount = form['promised_amount']
        if 'promised_date' in form:
            if bomber.id == SpecialBomber.OLD_APP_BOMBER.value:
                set_ptp_for_special_bomber(application.id, promised_date)
            if application.cycle == Cycle.C1A.value:
                # 1a不允许员工续p
                if (application.promised_date and
                    application.promised_date.date() >= datetime.now().date()):
                    abort(400, "Can not extend PTP")
                # 1a p过期件给新下p的人
                if (application.promised_date and
                    application.promised_date.date() < datetime.now().date()):
                    application.latest_bomber = bomber.id
            # 下p时没有latest_bomber，件分给下p的人
            if not application.latest_bomber:
                application.latest_bomber = bomber.id
            application.promised_date = form['promised_date']
            application.latest_call = bomber.id
            application.ptp_bomber = bomber.id
            application.status = ApplicationStatus.PROCESSING.value
            if form['promised_date'] >= datetime.today().date():
                update_auto_call_list = (
                    AutoCallList
                    .update(status=AutoListStatus.REMOVED.value,
                            description='set ptp')
                    .where(AutoCallList.application == application.id)
                )
                update_auto_call_list.execute()

        application.follow_up_date = form['follow_up_date']

        # 更新自动呼出队列
        update_auto_call_list = (
            AutoCallList
            .update(follow_up_date=form['follow_up_date'],
                    description='set followed up date')
            .where(AutoCallList.application == application.id)
        )
        update_auto_call_list.execute()

        application.latest_bombing_time = datetime.now()
        application.save()
    return bombing_history_serializer.dump(bombing_history).data


@post('/api/v1/applications/<app_id:int>/cs-ptp', skip=[ip_whitelist_plugin])
def add_collection_history(app_id):
    check_api_user()
    form = cs_ptp_validator(request.json)

    application = (Application
                   .filter(Application.external_id == app_id,
                           Application.status != ApplicationStatus.REPAID.value)
                   .first())

    if not application:
        # 如果这个件 在 cs ptp 的时候还没有进入催收 则 直接添加记录
        bombing_history = BombingHistory.create(
            application=app_id,
            bomber=72,
            promised_date=form['promised_date'],
            follow_up_date=form['promised_date'],
        )
        return bombing_history_serializer.dump(bombing_history).data

    with db.atomic():
        bombing_history = BombingHistory.create(
            application=application.id,
            user_id=application.user_id,
            ektp=application.id_ektp,
            cycle=application.cycle,
            bomber=72,
            promised_date=form['promised_date'],
            follow_up_date=form['promised_date'],
        )
        CallActions.create(
            cycle=application.cycle,
            bomber_id=72,
            call_record_id=bombing_history.id,
            application=application.id,
            promised_date=form['promised_date'],
            follow_up_date=form['promised_date'],
        )
        application.promised_date = form['promised_date']

        if form['promised_date'] >= datetime.today().date():
            update_auto_call_list = (
                AutoCallList
                .update(status=AutoListStatus.REMOVED.value,
                        description='set ptp')
                .where(AutoCallList.application == application.id)
            )
            update_auto_call_list.execute()

        application.follow_up_date = form['promised_date']

        # 更新自动呼出队列
        update_auto_call_list = (
            AutoCallList
            .update(follow_up_date=form['promised_date'],
                    description='set followed up date')
            .where(AutoCallList.application == application.id)
        )
        update_auto_call_list.execute()
        if not application.latest_bomber:
            application.latest_bomber = 72
        if application.status == ApplicationStatus.UNCLAIMED.value:
            application.status = ApplicationStatus.PROCESSING.value
        application.save()
    return bombing_history_serializer.dump(bombing_history).data
