#-*- coding:utf-8 -*-

import json
import logging
from datetime import datetime

from peewee import fn

from bomber.models import (
    DispatchAppHistory,
    ApplicationStatus,
    DispatchAppLogs,
    Application,
    DispatchApp,
    Bomber
)
from bomber.db import db
from bomber.worker import new_in_record, out_and_in_record_instalment
from bomber.mab_aid import aids as MBA_IDS
from bomber.constant_mapping import Cycle,ApplicationType,DisAppStatus


def get_mba_apps():
    old_apps = (Application.select(Application.id,
                                   Application.promised_date,
                                   Application.latest_bomber)
                .where(Application.status != ApplicationStatus.REPAID.value,
                       Application.id << MBA_IDS)
                .dicts())
    return old_apps

def get_mba_logs(apps):
    if not apps:
        return False
    app_logs = {}
    for a in apps:
        bid = a["latest_bomber"]
        if not bid:
            bid = 2
        if bid in app_logs:
            app_logs[bid]["to_ids"].append(a["id"])
        else:
            app_logs[bid] = {
                "bomber_id": bid,
                 "to_ids": [a["id"]],
                 "np_ids": [],
                 "p_ids": []
            }
    return app_logs

# 把件分给mba
def mba_dispatch_apps():
    app_logs = {}
    try:
        apps = get_mba_apps()
        app_logs = get_mba_logs(apps)
        if not app_logs:
            logging.info("no get app")
            return
        aids = [a["id"] for a in apps]
        with db.atomic():
            for idx in range(0, len(aids), 5000):
                new_aid = aids[idx:idx+5000]
                q = (Application.update(latest_bomber=1001,
                                        ptp_bomber=None)
                     .where(Application.id << new_aid)
                     .execute())
                # 出案
                p = (DispatchAppHistory.update(out_at=fn.NOW())
                     .where(DispatchAppHistory.application << new_aid,
                            DispatchAppHistory.out_at.is_null(True)))
                p.execute()
                # 入案
                in_param = {
                    "cycle": Cycle.M3.value,
                    "dest_partner_id": 6,
                    "application_ids": new_aid,
                    "dest_bomber_id": 1001
                }
                new_in_record(**in_param)
    except Exception as e:
        logging.error("分件错了%s" % str(e))
    try:
        bomber_logs = []
        for k, v in app_logs.items():
            dispatch_app_log = {
                "bomber_id": v["bomber_id"],
                "need_num": len(v.get("to_ids", [])),
                "form_ids": json.dumps(v.get("form_ids", [])),
                "to_ids": json.dumps(v.get("to_ids", [])),
                "np_ids": json.dumps(v.get("np_ids", [])),
                "p_ids": json.dumps(v.get("p_ids", [])),
                "status": 1
            }
            bomber_logs.append(dispatch_app_log)
        DispatchAppLogs.insert_many(bomber_logs).execute()
    except Exception as e:
        logging.info("insert bomber logs error:%s" % str(e))


# c1a分件
def get_no_paid_apps(type=ApplicationType.CASH_LOAN.value):
    apps = (Application.select(Application.id)
            .where(Application.cycle == Cycle.C1B.value,
                   Application.status << [ApplicationStatus.UNCLAIMED.value,
                                          ApplicationStatus.PROCESSING.value],
                   Application.promised_date.is_null(True) |
                   (fn.DATE(
                       Application.promised_date) < datetime.today().date()),
                   Application.type == type))
    return apps

def dispatch_apps_to_bomber(apps,type=ApplicationType.CASH_LOAN.value):
    aids = [a.id for a in apps]
    if not aids:
        return
    bombers = (Bomber.select()
               .where(Bomber.role == 5,
                      Bomber.is_del == 0))
    if type == ApplicationType.CASH_LOAN.value:
        bombers = bombers.where(Bomber.instalment == 0)
        need_bids= {95:124,205:85,211:123,218:93,219:142,220:214,230:185,
                    319:189,327:110,328:193,352:189,354:114,374:84,375:170,
                    376:139,382:183,390:178,395:112,396:153,406:121,407:138,
                    408:192,420:171,440:175,441:192,294:161}
    else:
        bombers = bombers.where(Bomber.instalment == Cycle.C1B.value)
        need_bids = {256:114,383:140}
    end = 0
    for b in bombers:
        need_num = need_bids.get(b.id)
        if not need_num:
            return
        start = end
        end = start + need_num
        b_aids = aids[start:end]
        with db.atomic():
            q = (Application.update(latest_bomber=b.id,
                                    status=ApplicationStatus.AB_TEST.value,
                                    ptp_bomber=None)
                 .where(Application.id << b_aids)
                 .execute())
            if type == ApplicationType.CASH_LOAN.value:
                d = (DispatchAppHistory.update(out_at=fn.NOW())
                     .where(DispatchAppHistory.application << b_aids,
                            DispatchAppHistory.out_at.is_null(True))
                     .execute())
                in_param = {
                    "cycle": Cycle.C1B.value,
                    "application_ids": b_aids,
                    "dest_bomber_id": b.id
                }
                new_in_record(**in_param)
            else:
                params = {
                    "application_ids": b_aids,
                    "cycle": Cycle.C1B.value,
                    "dest_bomber_id": b.id
                }
                out_and_in_record_instalment(**params)


def c1a_dispatch():
    types = [ApplicationType.CASH_LOAN.value,
             ApplicationType.CASH_LOAN_STAGING.value]
    for tp in types:
        apps = get_no_paid_apps(type=tp)
        dispatch_apps_to_bomber(apps=apps,type=tp)

