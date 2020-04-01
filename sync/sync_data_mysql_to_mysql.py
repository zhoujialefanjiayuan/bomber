#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import pymysql

OFFICE = 1
TABLE = 'TABLE'
JSON_TO_STRING = ["FIELD1", "FIELD2"]
# COUNT * STEP
COUNT = 1
STEP = 1

CONFIG = {
    'host': 'HOST',
    'database': 'DATABASE',
    'user': 'USER',
    'password': 'PASSWORD',
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor,

}

URL = 'http://127.0.0.1:1129/api/v1/mysql/data/sync'


REQ_SQL = """
    SELECT
         *
    FROM
        %s
    WHERE
        sync_status = 0
    ORDER BY
        id DESC
    LIMIT %s;
"""

RESP_SQL = """
    UPDATE %s
    SET sync_status = 1
    WHERE
        id in (%s)
    AND sync_status = 0;
"""


def sync_data(url, database):
    data_list = get_data(database)
    if not data_list:
        return
    for item in data_list:
        for field in JSON_TO_STRING:
            if field in item:
                item[field] = str(item[field]) if item[field] else None

        # 删除参考字段sync_status
        del item['sync_status']
    db_key = list(data_list[0].keys() if data_list else [])
    db_list = (list(map(lambda x: list(x.values()), data_list))
               if data_list else [])

    length = len(db_list)
    for index in range(0, length, STEP):
        print(index)
        post_dict = {
            'data_list': db_list[index:index + STEP],
            'data_key': db_key,
            'type': OFFICE,
            'table': TABLE
        }
        # post
        print(post_dict)
        try:
            r = requests.post(url, json=post_dict, timeout=300)
            id_list = r.json().get('data', [])
            update_status(database, id_list=id_list)
        except requests.ConnectTimeout:
            pass


def get_data(database):
    """
        从表中取出全部没有被更新的数据
    """
    with database.cursor() as cursor:
        try:
            cursor.execute(REQ_SQL % (TABLE, STEP))
            return cursor.fetchall()
        except Exception:
            raise
        finally:
            cursor.close()


def update_status(database, id_list=None):
    """
        更新同步成功的数据的状态
    """
    if not id_list:
        print('empty response')
        return
    with database.cursor() as cursor:
        try:
            cursor.execute(RESP_SQL % (TABLE, ','.join(map(str, id_list))))
            print('processing %s success' % len(id_list))
        except Exception:
            raise
        finally:
            db.commit()
            cursor.close()


if __name__ == '__main__':
    try:
        db = pymysql.connect(**CONFIG)
        for _ in range(COUNT):
            sync_data(URL, database=db)
    except Exception as err:
        print(err)
        raise
    finally:
        db.close()
