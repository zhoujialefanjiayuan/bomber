#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

from bomber.models_readonly import readonly_db
from bomber.models import CycleList
from .base import get_cycle, format_cycle


def get_agent():
    sql = """
        SELECT b.username, b.id, aca.cycle FROM bomber.auto_call_actions aca
        INNER JOIN bomber b ON aca.bomber_id = b.id
        WHERE DATE(aca.created_at) = CURDATE()
    """
    data = readonly_db.execute_sql(sql)
    data = list(data)
    if not data:
        return {}
    df = pd.DataFrame(data)

    df.columns = list('123')
    df['1'] = df['1'].apply(get_cycle)
    df['1'] = df['1'].apply(format_cycle)

    df = df[df['1'] == df['3']]
    df = df[df['1'].isin(CycleList.sql_values())]

    df2 = pd.DataFrame(df['2'].value_counts())
    df2 = df2[df2['2'] > 40]

    df = df[df['2'].isin(df2.index)]
    df = df.drop_duplicates(['2'])
    df3 = df['1'].value_counts()
    return dict(zip(df3.index, df3.tolist()))
