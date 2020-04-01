#!/usr/bin/env python
# -*- coding: utf-8 -*-
from bomber.models import CycleList


def get_cycle(s):
    suffix = s.split('_')[-1]
    for i in CycleList.values():
        if i in suffix.lower():
            return i
    return '0'


def format_cycle(s):
    dct = dict(zip(CycleList.values(), CycleList.sql_values()))
    return dct.get(s, '0')
