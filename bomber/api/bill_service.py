#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from decimal import Decimal
from datetime import datetime

from bottle import default_app, request
from peewee import SelectQuery

from .base import ServiceAPI
from bomber.models import ModelBase,ApplicationType
from bomber.plugins import PretreatmentPlugin
from bomber.utils import str_no_utc_datetime, no_utc_datetime, str_datetime

app = default_app()


def lambda_bill_dict(b, summary=False):
    """
    if summary is true:
      principal = principal + interest
      principal_paid = principal_paid + interest_paid

    :param b:
    :param summary:
    :return:
    """
    origin_due_at = no_utc_datetime(b.get('origin_due_at'))
    repay_at = no_utc_datetime(b.get('latest_repay_at'))

    interest = Decimal(str(b['interest']) if b.get('interest') else 0)
    interest_paid = Decimal(str(b['interest_paid'])
                            if b.get('interest_paid') else 0)
    principal = Decimal(str(b['principal']) if b.get('principal') else 0)
    principal_paid = (Decimal(str(b['principal_paid'])
                              if b.get('principal_paid') else 0))
    late_fee_initial = (Decimal(str(b['late_fee_initial'])
                                if b.get('late_fee_initial')
                                else 0))
    late_fee = Decimal(str(b['late_fee']) if b.get('late_fee') else 0)
    late_fee_paid = (Decimal(str(b['late_fee_paid'])
                             if b.get('late_fee_paid') else 0))
    total = principal + late_fee + interest
    repaid = principal_paid + late_fee_paid + interest_paid
    unpaid = max(total - repaid, Decimal(0))

    b['bill_id'] = b.get('id')
    b['late_fee_rate'] = b.get('late_fee_rate')
    b['late_fee_initial'] = late_fee_initial
    b['late_fee'] = late_fee
    b['late_fee_paid'] = late_fee_paid
    b['interest'] = interest
    b['interest_paid'] = interest_paid
    b['due_at'] = no_utc_datetime(b.get('due_at'))
    b['repaid'] = repaid
    b['unpaid'] = unpaid
    b['origin_due_at'] = origin_due_at
    b['repay_at'] = repay_at
    b['disbursed_date'] = no_utc_datetime(b.get('created_at'))
    b['status'] = b.get('status')
    b['id'] = b.get('application_id')
    b['finished_at'] = no_utc_datetime(b.get('finished_at'))
    b['amount'] = principal + interest

    if summary:
        b['principal'] = principal + interest
        b['principal_paid'] = principal_paid + interest_paid
    else:
        b['principal'] = principal
        b['principal_paid'] = principal_paid

    return b


def lambda_bill_dict_serializer(b, summary=False):
    _b = lambda_bill_dict(b, summary)

    _b['late_fee_initial'] = str(_b['late_fee_initial'])
    _b['principal'] = str(_b['principal'])
    _b['principal_paid'] = str(_b['principal_paid'])
    _b['late_fee'] = str(_b['late_fee'])
    _b['late_fee_paid'] = str(_b['late_fee_paid'])
    _b['due_at'] = str_datetime(_b['due_at'])
    _b['repaid'] = str(_b['repaid'])
    _b['unpaid'] = str(_b['unpaid'])
    _b['origin_due_at'] = str_datetime(_b['origin_due_at'])
    _b['repay_at'] = str_datetime(_b['repay_at'])
    _b['interest'] = str(_b['interest'])
    _b['interest_paid'] = str(_b['interest_paid'])
    _b['disbursed_date'] = str_datetime(_b['disbursed_date'])
    _b['id'] = str(_b['id'])
    _b['finished_at'] = str_datetime(_b['finished_at'])
    _b['amount_net'] = str(_b['principal'])
    _b['amount'] = str(_b['amount'])
    return _b


def lambda_bill_sub_dict_serializer(b):
    b['amount_net'] = b['amountNet']
    b['application_id'] = b['applicationId']
    b['application_status'] = b['applicationStatus']
    b['bill_id'] = b['billId']
    b['bill_status'] = b['billStatus']
    b['bill_sub_late_fee'] = b['billSubLateFee']
    b['bill_sub_status'] = b['billSubStatus']
    b['bill_type'] = b['billType']
    b['created_at'] = str_no_utc_datetime(b['createdAt'])
    b['due_at'] = str_no_utc_datetime(b['dueAt'])
    b['finished_at'] = b['finishedAt']
    b['late_fee'] = b['lateFee']
    b['late_fee_initial'] = b['lateFeeInitial']
    b['late_fee_paid'] = b['lateFeePaid']
    b['late_fee_rate'] = b['lateFeeRate']
    b['origin_due_at'] = str_no_utc_datetime(b['originDueAt'])
    b['overdue_days'] = b['overdueDays']
    b['overdue_late_fee'] = b['overdueLateFee']
    b['partner_args'] = b['partnerArgs']
    b['partner_bill_id'] = b['partnerBillId']
    b['partner_code'] = b['partnerCode']
    b['interest'] = b['interest']
    b['interest_paid'] = b['interestPaid']
    b['principal'] = b['principal'] + b['interest']
    b['principal_paid'] = b['principalPaid'] + b['interestPaid']
    b['stage_num'] = b['stageNum']
    b['stage_status'] = b['stageStatus']
    b['updated_at'] = str_no_utc_datetime(b['updatedAt'])
    b['user_id'] = b['userId']
    b['which_day_overdue'] = b['whichDayOverdue']
    return b


def lambda_bill_dict_summary(b):
    return lambda_bill_dict(b, summary=True)


def lambda_bill_dict_summary_serializer(b):
    return lambda_bill_dict_serializer(b, summary=True)

# 子账单数据格式化
def lambda_bill_sub_dict(b):
    origin_due_at = no_utc_datetime(b.get('origin_due_at'))

    interest = Decimal(str(b['interest']) if b.get('interest') else 0)
    interest_paid = Decimal(str(b['interest_paid'])
                            if b.get('interest_paid') else 0)
    principal = Decimal(str(b['principal']) if b.get('principal') else 0)
    principal_paid = (Decimal(str(b['principal_paid'])
                              if b.get('principal_paid') else 0))
    late_fee_initial = (Decimal(str(b['late_fee_initial'])
                                if b.get('late_fee_initial')
                                else 0))
    late_fee = Decimal(str(b['late_fee']) if b.get('late_fee') else 0)
    late_fee_paid = (Decimal(str(b['late_fee_paid'])
                             if b.get('late_fee_paid') else 0))
    if late_fee > 0:
        late_fee += late_fee_initial

    total = principal + late_fee + interest
    repaid = principal_paid + late_fee_paid + interest_paid
    unpaid = max(total - repaid, Decimal(0))
    # 分期期数
    periods = b.get('stage_num')

    b['late_fee_rate'] = b.get('late_fee_rate')
    b['late_fee_initial'] = late_fee_initial
    b['late_fee'] = late_fee
    b['late_fee_paid'] = late_fee_paid
    b['interest'] = interest
    b['interest_paid'] = interest_paid
    b['due_at'] = no_utc_datetime(b.get('due_at'))
    b['repaid'] = repaid
    b['unpaid'] = unpaid
    b['origin_due_at'] = origin_due_at
    b['status'] = b.get('status')
    b['finished_at'] = no_utc_datetime(b.get('finished_at'))
    b['amount'] = principal + interest
    b['amount_net'] = principal
    b['periods'] = periods
    b['principal_paid'] = principal_paid

    return b


def lambda_all_bill_sub_dict_serializer(b):
    b = lambda_bill_sub_dict(b)
    for k,v in b.items():
        if isinstance(v,Decimal):
            b[k] = str(v)
        if isinstance(v,datetime):
            b[k] = str(v)
    return b


class BillService(ServiceAPI):
    default_page = 1
    default_page_size = 20

    bill_fields = ['late_fee_rate',
                   'late_fee_initial',
                   'late_fee',
                   'interest',
                   'due_at',
                   'principal_paid',
                   'late_fee_paid',
                   'repaid',
                   'unpaid',
                   'origin_due_at',
                   'repay_at',
                   'interest',
                   'disbursed_date',
                   'amount_net']

    def default_token(self):
        return app.config['service.bill_service.token']

    def get_base_url(self):
        return app.config['service.bill_service.base_url']

    @PretreatmentPlugin('/bill/get_bill', 'GET')
    def _bill_dict(self, **params):
        pass

    @PretreatmentPlugin('/bill/getByApplicationIds', 'POST')
    def _bill_list(self, **params):
        pass

    def bill_dict(self, bill=None, **params):
        if not bill:
            bill = self._bill_dict(**params)
        return lambda_bill_dict(bill, summary=True)

    def bill_list(self, bills=None, **params):
        if not bills:
            bills = self._bill_list(**params)['data']
        return list(map(lambda_bill_dict_summary, bills))

    def bill_dict_serializer(self, bill=None, **params):
        if not bill:
            bill = self._bill_dict(**params)
        return lambda_bill_dict_serializer(bill, summary=True)

    def bill_list_serializer(self, bills=None, **params):
        if not bills:
            bills = self._bill_list(**params)['data']
        return list(map(lambda_bill_dict_summary_serializer, bills))

    def get_base_application(self, app_dict, serializer=None, base_app=None):
        if isinstance(app_dict, ModelBase) and serializer:
            app_dict = serializer.dump(app_dict).data

        if not app_dict:
            return {}

        if not base_app:
            application_id = int(app_dict['external_id'])
            base_app = self.bill_dict_serializer(application_id=application_id)

        app_dict.update({key: value for key, value in base_app.items()
                         if key in self.bill_fields})
        return app_dict

    @classmethod
    def query_page(cls, query, serializer, paginate):
        # return:  page, page_size, total_count, total_page, app_list
        if not paginate:
            result = serializer.dump(query, many=True).data
            return {
                "page": None,
                "page_size": None,
                "total_count": None,
                "total_page": None,
                "result": result
            }

        page = cls.default_page
        page_size = cls.default_page_size

        if request.query.page.isdigit():
            page = int(request.query.page)
        if request.query.page_size.isdigit():
            page_size = int(request.query.page_size)

        result = query.paginate(page, page_size)
        result = serializer.dump(result, many=True).data

        if len(result) < page_size:
            total_count = (page-1)*page_size + len(result)
        else:
            # todo caching
            total_count = query.count()

        return {
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_page": ((total_count or 1)-1) // page_size + 1,
            "result": result
        }

    @staticmethod
    def _nested_id_get(dct, keys=None):
        # 寻找嵌套字段类型
        if keys is None:
            keys = []

        keys = keys + ['external_id']

        for key in keys[:-1]:
            dct = dct.get(key, {})
        return dct.get(keys[-1])

    # @staticmethod
    # def _nested_set(dct, value, keys=None):
    #     for key in keys[:-1]:
    #         dct = dct.setdefault(key, {})
    #     dct[keys[-1]] = value

    def get_base_applications(self, app_list, serializer=None,
                              base_apps=None, paginate=False, level=1):
        result = None
        # TODO: 替代SelectQuery
        if isinstance(app_list, SelectQuery):
            # page, page_size, total_count, total_page, result
            result = self.query_page(app_list, serializer, paginate)
            app_list = result['result']

        if paginate and not app_list:
            return {
                'page': 1,
                'page_size': self.default_page_size,
                'total_count': 0,
                'total_page': 1,
                'result': [],
            }

        if not app_list:
            return []

        keys = []
        if level == 2:
            keys = ['application']

        if not base_apps:
            ids = [int(self._nested_id_get(item, keys)) for item in app_list]
            base_apps = self.bill_list_serializer(application_ids=ids)


        base_apps_dict = {int(ba['application_id']): ba for ba in base_apps}
        for a in app_list:
            application_id = int(self._nested_id_get(a, keys))
            b = base_apps_dict.get(application_id)
            if not b:
                continue
            if level == 2:
                a = a['application']

            a.update({key: value for key, value in b.items()
                      if key in self.bill_fields})

        if paginate:
            return {
                'page': result['page'],
                'page_size': result['page_size'],
                'total_count': result['total_count'],
                'result': app_list
            }
        return app_list

    def get_base_applications_v2(self, app_list, serializer=None,
                                 base_apps=None, paginate=False):


        return self.get_base_applications(app_list, serializer, base_apps,
                                          paginate, level=2)


    @PretreatmentPlugin('/bill/bill_relief', 'POST')
    def bill_relief(self, path_params, **params):
        """
        /api/v1/users/<user_id:int>/bill-history
        """
        pass

    @PretreatmentPlugin('/bill/external_bills/{bill_id}', 'GET')
    def bill_external(self, path_params, **params):
        """
        /api/v1/external_bills/<bill_id:int>
        """
        pass

    def external_sub_bills(self, bill_id):
        bill = self.bill_external(path_params={'bill_id': int(bill_id)})
        if not bill:
            return []
        bill_subs = bill['bill_subs']
        return list(map(lambda_bill_sub_dict_serializer, bill_subs))

    @PretreatmentPlugin('/bill/bills/page', 'POST')
    def get_bill_with_page(self, **params):
        pass

    def bill_pages(self, **params):
        resp = self.get_bill_with_page(**params)
        resp['result'] = list(map(lambda_bill_dict_summary_serializer,
                                  resp['result']))
        return resp

    @PretreatmentPlugin('/application/getIvrApplications', 'POST')
    def ivr_pages(self, **params):
        """
        :param params:
        :return:
        """
        pass

    @PretreatmentPlugin('/account/list', 'POST')
    def accounts_list(self, **params):
        """

        :param params:
        {
          "bank_code": "BCA",
          "bank_codes": [
            "BCA",
            "PERMATA"
          ],
          "id": 2273,
          "is_deprecated": false,
          "order_by": "bankCode",
          "partner_code": "DOKU",
          "sort": "acs",
          "user_id": 2273
        }
        :return:list
        """
        pass

    @PretreatmentPlugin('/bill_sub/get/list', 'GET')
    def _get_sub_bill(self, **params):
        """
        根据子账单id获取子账单信息
        bill_sub_ids = []
        :param params:
        :return:
        """
        pass

    def sub_bill_list(self, **params):
        sub_bill = self._get_sub_bill(**params)
        if not sub_bill:
            return []
        return list(map(lambda_bill_sub_dict, sub_bill))


    @PretreatmentPlugin('/bill_sub/get/all', 'GET')
    def _get_all_sub_bill(self, **params):
        """
        根据主账单id获取所有的子账单信息
        bill_id 主账单id
        :param params:
        :return:
        """
        pass

    def all_sub_bill(self, **params):
        sub_bill_list = self._get_all_sub_bill(**params)
        if not sub_bill_list:
            return []
        return list(map(lambda_all_bill_sub_dict_serializer, sub_bill_list))







