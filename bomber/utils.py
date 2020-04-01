import json
import os
import re
import sys
import time
import logging
from operator import itemgetter as _itemgetter
import heapq as _heapq
from dateutil import parser
from pytz import timezone
from collections import namedtuple

from datetime import datetime, date
from enum import Enum

from bottle import request
from decimal import Decimal

from bomber import snowflake

TIMEZONE = 'Asia/Shanghai'


def json_default(dt_fmt='%Y-%m-%d %H:%M:%S', date_fmt='%Y-%m-%d',
                 decimal_fmt=str):
    def _default(obj):
        if isinstance(obj, datetime):
            return obj.strftime(dt_fmt)
        elif isinstance(obj, date):
            return obj.strftime(date_fmt)
        elif isinstance(obj, Decimal):
            return decimal_fmt(obj)
        else:
            raise TypeError('%r is not JSON serializable' % obj)

    return _default


def json_dumps(obj, dt_fmt='%Y-%m-%d %H:%M:%S', date_fmt='%Y-%m-%d',
               decimal_fmt=str, ensure_ascii=False):
    return json.dumps(obj, ensure_ascii=ensure_ascii,
                      default=json_default(dt_fmt, date_fmt, decimal_fmt))


class PropDict(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        return self[name] if name in self else None


def _plain_args(d, list_fields=None):
    list_fields = list_fields or ()

    result = PropDict((key, getattr(d, key)) for key in d)
    for key in list_fields:
        result[key] = d.getall(key)

    return result


def plain_forms(list_fields=None):
    """ Plain POST data. """
    return _plain_args(request.forms, list_fields)


def plain_query(list_fields=None):
    """ Plain GET data """
    return _plain_args(request.query, list_fields)


def plain_params(list_fields=None):
    """ Plain all data """
    return _plain_args(request.params, list_fields)


id_generator = snowflake.generator(1, 1)


def idg():
    return next(id_generator)


def mask(s, start=0, end=None, fill_with='*'):
    """ 将指定范围内的字符替换成指定字符，范围规则与 list 切片一致 """
    sl = list(s)
    if end is None:
        end = len(sl)
    sl[start:end] = fill_with * len(sl[start:end])
    return ''.join(sl)


def env_detect():
    env = os.environ.get('APP_ENV')
    if env is None:
        test_commands = ('utrunner.py', 'nose', 'nose2', 'pytest')
        if os.path.basename(sys.argv[0]) in test_commands:
            env = 'TESTING'
        else:
            env = 'DEV'
    return env


def get_priority_language():
    accept_language = request.headers.get('Accept-Language', 'en-US,en;')
    if 'id' in accept_language:
        return 'id'
    return 'en'


# 通过KTP 计算出的出生日期
def birth_dt_ktp(ektp_number):
    """
    :param ektp_number:
    :return: date
    """
    if not ektp_number:
        return
    age_num = ektp_number[6:12]
    day = int(age_num[:2])
    month = int(age_num[2:4])
    year = int(age_num[4:6])

    # 女性大于40
    if day > 40:
        day -= 40

    now = datetime.now()

    if year < now.year % 100:
        year += 2000
    else:
        year += 1900

    try:
        return date(year, month, day)
    except ValueError:
        return None


# D238 通过用户填写的KTP号码推算出的性别
def gender_ktpnum(ektp_number):
    if not ektp_number:
        return None
    gender = int(ektp_number[6:8])
    return 'male' if gender < 40 else 'female'


def get_permission(role_group, role_action):
    return '%s^%s' % (role_group.strip(), role_action.strip())


def request_ip():
    full_ip = (request.environ.get('HTTP_X_FORWARDED_FOR') or
               request.environ.get('REMOTE_ADDR'))
    return (full_ip or '').split(',')[0].strip()


number_strip_re = re.compile(r'\d+')


def number_strip(m):
    # 印尼的号码有86开头，所以去掉中国 的 86 要跟着 + 一起
    m = m.replace('+86', '')

    # 以下规则，都不考虑中国号码

    # 先取纯数字
    number = ''.join(number_strip_re.findall(m))

    if number.startswith('62'):
        number = number[2:]

    # 所有开头的 0 都不要
    return number.lstrip('0')


under_score_re = re.compile(r'(.)([A-Z][a-z]+)')
under_score_re2 = re.compile(r'([a-z0-9])([A-Z])')


def camel_to_snake(s):
    subbed = under_score_re.sub(r'\1_\2', s)
    return under_score_re2.sub(r'\1_\2', subbed).lower()


def items(d):
    return d.items()


class ChoiceEnum(Enum):
    @classmethod
    def choices(cls):
        return [(e.name, e.value) for e in cls]

    @classmethod
    def values(cls):
        return [e.value for e in cls]

    @classmethod
    def keys(cls):
        return [e.name for e in cls]


def diff_end_time(start):
    end = time.time()
    time_diff = round(end - start, 3)
    return time_diff


class OperatedDict(dict):
    def __init__(*args, **kwargs):
        if not args:
            raise TypeError("descriptor '__init__' of 'Counter' object "
                            "needs an argument")
        self, *args = args
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        super(OperatedDict, self).__init__(*args, **kwargs)

    def __truediv__(self, other):
        if not isinstance(other, OperatedDict):
            return NotImplemented
        result = OperatedDict()
        for key, val in other.items():
            result[key] = self.get(key, 0) / val if int(val) != 0 else 0.0
        return result

    def __add__(self, other):
        if not isinstance(other, OperatedDict):
            return NotImplemented
        result = OperatedDict()
        for key in self.all(self, other):
            result[key] = self.get(key, 0) + other.get(key, 0)
        return result

    def __sub__(self, other):
        if not isinstance(other, OperatedDict):
            return NotImplemented
        result = OperatedDict()
        for key in self.all(self, other):
            result[key] = self.get(key, 0) - other.get(key, 0)
        return result

    @staticmethod
    def all(a, b):
        return set(list(a.keys()) + list(b.keys()))

    def __repr__(self):
        if not self:
            return '%s()' % self.__class__.__name__
        try:
            items = ', '.join(map('%r: %r'.__mod__, self.most_common()))
            return '%s({%s})' % (self.__class__.__name__, items)
        except TypeError:
            # handle case where values are not orderable
            return '{0}({1!r})'.format(self.__class__.__name__, dict(self))

    def most_common(self, n=None):
        if n is None:
            return sorted(self.items(), key=_itemgetter(1), reverse=True)
        return _heapq.nlargest(n, self.items(), key=_itemgetter(1))


def strptime(date, date_format='%Y-%m-%d'):
    return datetime.strptime(date, date_format)


def average_gen(gen, existing_list):
    """
    保证件平均分到每个人
    - gen: generator
    - existing_list: list
    """
    if len(existing_list) > 0:
        return existing_list.pop()
    return next(gen)


def str_no_utc_datetime(s, fmt='%Y-%m-%d %H:%M:%S'):
    if not s:
        return
    if not isinstance(s, str):
        raise TypeError('Invalid type')
    return parser.parse(s).strftime(fmt)


def no_utc_datetime(s):
    if not s:
        return
    if not isinstance(s, str):
        raise TypeError('Invalid type')
    return parser.parse(s).replace(tzinfo=None)


def utc_datetime(s, fmt='%Y-%m-%dT%H:%M:%S%z'):
    if not s:
        return
    if not isinstance(s, str):
        raise TypeError('Invalid type')
    tz = timezone(TIMEZONE)
    return parser.parse(s).replace(tzinfo=tz).strftime(fmt)[:-2]


def to_datetime(s, fmt="%Y-%m-%d %H:%M:%S"):
    if not s:
        return None
    return datetime.strptime(s, fmt)


def list_to_dict(pk, lst, pk_to_int=False):
    if pk_to_int:
        return {int(item[pk]): item for item in lst if item.get(pk)}
    return {item[pk]: item for item in lst}


def str_datetime(s, fmt='%Y-%m-%d %H:%M:%S'):
    if not s:
        return
    return s.strftime(fmt)

# 根据逾期天数计算获取对应的cycle
def get_cycle_by_overdue_days(overdue_days):
    cycle_days_map = {
        1: [1, 10],
        2: [11, 30],
        3: [31, 60],
        4: [61, 90],
        5: [91, 999999],
    }
    cycle = 0
    for k,v in cycle_days_map.items():
        if v[0] <= overdue_days <= v[1]:
            cycle = k
            break
    return cycle


def time_logger(func):
    def w(*args, **kwargs):
        s = time.time()
        r = func(*args, **kwargs)
        e = time.time()
        time_diff = round(e-s, 3)
        logging.info('function %s runtime: %s,department_summary_logging_time',
                     func.__name__, time_diff)
        return r
    return w