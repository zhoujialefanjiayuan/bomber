#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

from bomber.models_readonly import RAgentTypeR
from bomber.models import CycleList

from .base import get_cycle


def get_source_data(start_time=None, end_time=None):
    q = RAgentTypeR.select().where(
        RAgentTypeR.START_TIME >= start_time,
        RAgentTypeR.START_TIME < end_time
    )

    df = pd.DataFrame(list(q.dicts()))
    if len(df.columns) == 0:
        return
    df.columns = df.columns.str.lower()

    user_list = df['user_name'].tolist()
    user_list = list(set(user_list))
    data = pd.DataFrame([], columns=['user_name', 'busy_time', 'busy_time_per'])
    for i in user_list:
        df_i = df[df['user_name'] == i]
        df_i_DND = df_i[df_i.type == 'DND'][['start_time', 'end_time', 'time_length']].reset_index(drop=True)
        df_i_busy = df_i[df_i.type == 'Busy'][['call_from', 'time_length']].reset_index(drop=True)
        df_i_busy['busy_time'] = df_i_busy.apply(_cal_busy_time,axis=1)

        busy_time_per = round(df_i_busy['busy_time'].sum() / df_i_busy['busy_time'].count(), 2)  # 每通电话的时长
        busy_time = df_i_busy['busy_time'].sum()  # 繁忙时间

        a = pd.Series([i, busy_time, busy_time_per], index=['user_name', 'busy_time', 'busy_time_per'])
        data = data.append(a,ignore_index=True)

    data['cycle'] = data.user_name.apply(get_cycle)
    data['calls'] = data.busy_time / data.busy_time_per
    return data[data['cycle'].isin(CycleList.values())]


def _cal_busy_time(input_args):
    call_from = input_args.get('call_from')
    time_length = input_args.get('time_length')
    if pd.notnull(call_from) and len(str(call_from)) > 4:
        return time_length
    else:
        return 0


def average_call_duration_team(start_time, end_time):
    dct = dict(zip(CycleList.values(), CycleList.sql_values()))
    data = get_source_data(start_time, end_time)
    if data is None:
        return {}
    d = data.groupby('cycle').agg({'busy_time': 'sum', 'calls': 'sum'})
    d['result'] = d.busy_time / d.calls
    lst = zip(d.index, d.result)
    return {dct[key]: val for key, val in lst}
