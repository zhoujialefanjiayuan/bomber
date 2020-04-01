import pymysql
import webtest


class TestApp(webtest.TestApp):
    def do_request(self, req, status=None, expect_errors=None):
        # 先不要检查错误
        res = super().do_request(req, status, expect_errors=True)

        if res.errors:
            # 打印错误
            print(res.errors)

        # 然后再检查错误
        if not expect_errors:
            self._check_status(status, res)
            self._check_errors(res)
        return res


def drop_and_create_database(config):
    mysql_drop_and_create_table(config)


def mysql_drop_and_create_table(config):
    """ 重建数据库 """
    database = config['db.database']
    assert database.startswith('test_')  # 任何时候不能注释、删除掉此行

    conn = pymysql.connect(
        host=config['db.host'],
        port=int(config['db.port']),
        user=config['db.user'],
        password=config['db.password'],
        charset=config['db.charset'],
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute('DROP DATABASE IF EXISTS %s ' % database)
            cursor.execute('CREATE DATABASE IF NOT EXISTS %s '
                           'CHARACTER SET utf8mb4 '
                           'COLLATE utf8mb4_unicode_ci ' % database)
    finally:
        conn.close()
