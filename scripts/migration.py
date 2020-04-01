import json
from datetime import datetime, date
from enum import unique, Enum

import sys
import time
import logging
import requests

from playhouse.db_url import connect
from peewee import (Model, R, BigIntegerField, SmallIntegerField, CharField,
                    DecimalField, DateTimeField, ForeignKeyField, TextField,
                    IntegerField, BooleanField, DateField, JOIN)


class BaseAPI(object):
    def _request(self, method, endpoint, **kwargs):
        kwargs['method'] = method

        request_url = '%s%s' % (self.get_base_url(), endpoint)
        kwargs['url'] = request_url

        self.before_request(kwargs)
        result = requests.request(**kwargs)
        if not result.ok:
            logging.error('request to %s (%s) failed: %s',
                          self.__class__.__name__, request_url, result.text)
        return result

    def get_base_url(self):
        raise NotImplementedError

    def before_request(self, kwargs):
        pass

    def get(self, endpoint, params=None, **kwargs):
        return self._request('GET', endpoint, params=params, **kwargs)

    def post(self, endpoint, data=None, json=None, **kwargs):
        return self._request('POST', endpoint, data=data, json=json, **kwargs)

    def put(self, endpoint, data=None, **kwargs):
        return self._request('PUT', endpoint, data=data, **kwargs)

    def options(self, endpoint, **kwargs):
        return self._request('OPTIONS', endpoint, **kwargs)

    def head(self, endpoint, **kwargs):
        return self._request('HEAD', endpoint, **kwargs)

    def patch(self, endpoint, data=None, **kwargs):
        return self._request('PATCH', endpoint, data=data, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self._request('DELETE', endpoint, **kwargs)


class BaseServiceAPI(BaseAPI):
    def __init__(self, token=None):
        super().__init__()
        self._token = token or self.default_token()

    def get_base_url(self):
        raise NotImplementedError

    def default_token(self):
        raise NotImplementedError

    def before_request(self, kwargs):
        auth = (self._token, None)
        kwargs.setdefault('auth', auth)


class GoldenEye(BaseServiceAPI):
    def default_token(self):
        return 'token_test'

    def get_base_url(self):
        return 'http://127.0.0.1:2220/api/v1'


"""
https://github.com/cablehead/python-snowflake
"""


log = logging.getLogger(__name__)


# Tue, 21 Mar 2006 20:50:14.000 GMT
twepoch = 1142974214000

worker_id_bits = 5
data_center_id_bits = 5
max_worker_id = -1 ^ (-1 << worker_id_bits)
max_data_center_id = -1 ^ (-1 << data_center_id_bits)
sequence_bits = 12
worker_id_shift = sequence_bits
data_center_id_shift = sequence_bits + worker_id_bits
timestamp_left_shift = sequence_bits + worker_id_bits + data_center_id_bits
sequence_mask = -1 ^ (-1 << sequence_bits)


def snowflake_to_timestamp(_id):
    _id >>= 22      # strip the lower 22 bits
    _id += twepoch  # adjust for twitter epoch
    _id /= 1000     # convert from milliseconds to seconds
    return _id


def generator(worker_id, data_center_id, sleep=lambda x: time.sleep(x/1000.0)):
    assert 0 <= worker_id <= max_worker_id
    assert 0 <= data_center_id <= max_data_center_id

    last_timestamp = -1
    sequence = 0

    while True:
        timestamp = int(time.time()*1000)

        if last_timestamp > timestamp:
            log.warning(
                "clock is moving backwards. waiting until %i" % last_timestamp)
            sleep(last_timestamp-timestamp)
            continue

        if last_timestamp == timestamp:
            sequence = (sequence + 1) & sequence_mask
            if sequence == 0:
                log.warning("sequence overrun")
                sequence = -1 & sequence_mask
                sleep(1)
                continue
        else:
            sequence = 0

        last_timestamp = timestamp

        yield (
            ((timestamp-twepoch) << timestamp_left_shift) |
            (data_center_id << data_center_id_shift) |
            (worker_id << worker_id_shift) |
            sequence)

id_generator = generator(1, 1)


def idg():
    return next(id_generator)

ge_conf = 'mysql://battlefront:TbVxjdXWDg@danacepat-develop-cluster.cluster-cxsokeog2chp.ap-southeast-1.rds.amazonaws.com:3306/golden_eye'
cl_conf = 'mysql://battlefront:TbVxjdXWDg@danacepat-develop-cluster.cluster-cxsokeog2chp.ap-southeast-1.rds.amazonaws.com:3306/collection'
# bb_conf = 'mysql://battlefront:TbVxjdXWDg@danacepat-develop-cluster.cluster-cxsokeog2chp.ap-southeast-1.rds.amazonaws.com:3306/bomber'
bb_conf = 'mysql://root@localhost:3306/bomber'

bb_db = connect(bb_conf, charset='utf8mb4')
cl_db = connect(cl_conf, charset='utf8mb4')
ge_db = connect(ge_conf, charset='utf8mb4')


class BaseEnum(Enum):
    @classmethod
    def choices(cls):
        return [e.value for e in cls]


class ClModelBase(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')],
                               default=datetime.now)
    updated_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP'),
                                            R('ON UPDATE CURRENT_TIMESTAMP')],
                               default=datetime.now)

    class Meta:
        database = cl_db
        only_save_dirty = True


class GeModelBase(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')],
                               default=datetime.now)
    updated_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP'),
                                            R('ON UPDATE CURRENT_TIMESTAMP')],
                               default=datetime.now)

    class Meta:
        database = ge_db
        only_save_dirty = True


class ModelBase(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')],
                               default=datetime.now)
    updated_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP'),
                                            R('ON UPDATE CURRENT_TIMESTAMP')],
                               default=datetime.now)

    class Meta:
        database = bb_db
        only_save_dirty = True


@unique
class OverdueApplicationStatus(BaseEnum):
    UNCLAIMED = 0  # 待领取
    REPAYING = 1  # 催收中
    REPAID = 2  # 还款完成
    BAD_DEBT = 3  # 坏账


@unique
class ApplicationStatus(BaseEnum):
    # 已经逾期 等待催收
    UNCLAIMED = 0
    # 已领取 催收中
    PROCESSING = 1
    # 还款完成
    REPAID = 2
    # 坏账
    BAD_DEBT = 3


@unique
class PhoneSource(BaseEnum):
    (EMERGENCY_CONTACT, CONTACT, CALL_LOG, SMS, COMPANY_PHONE,
     SIM_CARD, OTHER, SELF_PHONE) = range(8)


@unique
class RelationType(BaseEnum):
    (PARENTS, MATE, RELATION, COLLEAGUE, FRIEND, SCHOOLMATE,
     OTHER, SELF) = range(8)


@unique
class SubRelation(BaseEnum):
    # all
    UNKNOWN = 0
    # family
    HOME = 1
    EC = 2
    RELATIVE = 3
    # company
    OFFICE = 4
    COLLEAGUE = 5
    BOSS = 6
    HR = 7
    # suggested
    FRIEND = 8


@unique
class PhoneValidity(BaseEnum):
    (USEFUL, BUSY, NO_ANSWER, NOT_AVAILABLE, NOT_ACTIVE,
     WRONG_NUMBER, REJECT, OTHER) = range(8)


class CollectionNotes(ClModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application_id = BigIntegerField()
    collector_id = BigIntegerField()
    phone = CharField(max_length=20)
    is_answer = BooleanField()
    phone_source = SmallIntegerField(choices=PhoneSource.choices())
    relation = SmallIntegerField(choices=RelationType.choices())
    phone_validity = SmallIntegerField(choices=PhoneValidity.choices())
    note = TextField(null=True)
    overdue_reason = TextField(null=True)
    follow_up_date = DateField(null=True)
    promise_repay_date = DateField(null=True)
    promise_repay_amount = DecimalField(max_digits=15,
                                        decimal_places=2, null=True)

    class Meta:
        db_table = 'collection_notes'


class ClApplication(ClModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    user_id = BigIntegerField()
    user_mobile_no = CharField(max_length=16)
    user_name = CharField(max_length=128)
    device_no = CharField(max_length=64)
    app = CharField(max_length=64)
    amount = DecimalField(max_digits=15, decimal_places=2)
    amount_net = DecimalField(max_digits=15, decimal_places=2)
    term = IntegerField()
    due_at = DateTimeField()
    apply_at = DateTimeField()

    status = SmallIntegerField(
        choices=OverdueApplicationStatus.choices(),
        default=OverdueApplicationStatus.UNCLAIMED.value
    )
    collector_id = BigIntegerField()
    claimed_at = DateTimeField(default=datetime.now, null=True)
    latest_note = ForeignKeyField(CollectionNotes, 'applications',
                                  db_column='latest_note', null=True)
    overdue_days = IntegerField()
    is_old_user = BooleanField()
    loan_success_times = IntegerField()
    paid = DecimalField(max_digits=15, decimal_places=2)
    latest_paid_at = DateTimeField(null=True)

    class Meta:
        db_table = 'application'


class Application(ModelBase):
    # common info
    # user info
    id = BigIntegerField(default=idg, primary_key=True)
    user_id = BigIntegerField()
    user_mobile_no = CharField(max_length=16)
    user_name = CharField(max_length=128)
    app = CharField(max_length=64)
    device_no = CharField(max_length=64)
    contact = TextField(null=True)
    apply_at = DateTimeField(null=True)

    id_ektp = CharField(max_length=32, null=True)
    birth_date = DateField(null=True)
    gender = CharField(max_length=16, null=True)

    profile_province = TextField(null=True)
    profile_city = TextField(null=True)
    profile_district = TextField(null=True)
    profile_residence_time = SmallIntegerField(null=True)
    profile_residence_type = SmallIntegerField(default=0)
    profile_address = CharField(max_length=255, null=True)
    profile_education = SmallIntegerField(null=True)
    profile_college = TextField(null=True)

    # job info
    job_name = CharField(max_length=128, null=True)
    job_tel = CharField(max_length=32, null=True)
    job_bpjs = CharField(max_length=16, null=True)
    job_user_email = CharField(max_length=64, null=True)
    job_type = SmallIntegerField(null=True)
    job_industry = SmallIntegerField(null=True)
    job_department = SmallIntegerField(null=True)
    job_province = TextField(null=True)
    job_city = TextField(null=True)
    job_district = TextField(null=True)
    job_address = TextField(null=True)

    # loan info
    amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    amount_net = DecimalField(max_digits=15, decimal_places=2, null=True)
    interest_rate = DecimalField(max_digits=8, decimal_places=6, null=True)
    late_fee_rate = DecimalField(max_digits=8, decimal_places=6, null=True)
    late_fee_initial = DecimalField(max_digits=15, decimal_places=2, null=True)
    late_fee = DecimalField(max_digits=15, decimal_places=2, null=True)
    term = IntegerField(null=True)
    overdue_days = IntegerField(null=True)
    repaid = DecimalField(max_digits=15, decimal_places=2, null=True)
    unpaid = DecimalField(max_digits=15, decimal_places=2, null=True)

    # repayment bill
    principal_paid = DecimalField(max_digits=15, decimal_places=2, null=True)
    late_fee_paid = DecimalField(max_digits=15, decimal_places=2, null=True)
    origin_due_at = DateTimeField(null=True)
    due_at = DateTimeField(null=True)
    repay_at = DateTimeField(null=True)

    # bomber control
    is_rejected = BooleanField(default=False)
    cycle = IntegerField(default=1)
    loan_success_times = IntegerField(null=True)
    latest_bomber_id = BigIntegerField(null=True)
    last_bomber_id = BigIntegerField(null=True)
    status = SmallIntegerField(
        choices=ApplicationStatus.choices(),
        default=ApplicationStatus.UNCLAIMED.value,
    )
    latest_bombing_time = DateTimeField(null=True)
    arrived_at = DateTimeField(null=True)
    claimed_at = DateTimeField(null=True)
    promised_amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    promised_date = DateTimeField(null=True)
    follow_up_date = DateTimeField(null=True)
    finished_at = DateTimeField(null=True)

    class Meta:
        db_table = 'application'


class BombingHistory(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'bombed_histories')
    user_id = BigIntegerField(null=True)
    ektp = CharField(max_length=32)
    cycle = IntegerField(null=True)
    bomber_id = BigIntegerField()
    promised_amount = DecimalField(null=True, max_digits=15, decimal_places=2)
    promised_date = DateField(null=True)
    follow_up_date = DateTimeField(null=True)
    result = SmallIntegerField(null=True)
    remark = TextField(null=True)

    class Meta:
        db_table = 'bombing_history'


@unique
class ConnectType(BaseEnum):
    CALL = 0
    SMS = 1
    PAY_METHOD = 2


@unique
class ContactStatus(BaseEnum):
    (UNKNOWN, USEFUL, BUSY, NO_ANSWER, NOT_AVAILABLE, NOT_ACTIVE,
     WRONG_NUMBER, REJECT, OTHER) = range(9)


@unique
class Relationship(BaseEnum):
    APPLICANT = 0
    FAMILY = 1
    COMPANY = 2
    SUGGESTED = 3


class ConnectHistory(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'connect_history')
    type = SmallIntegerField(choices=ConnectType.choices())
    name = CharField(max_length=128, null=True)
    operator_id = BigIntegerField()
    number = CharField(max_length=64)
    relationship = SmallIntegerField(choices=Relationship.choices())
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    status = SmallIntegerField(
        choices=ContactStatus.choices(),
        default=ContactStatus.UNKNOWN.value,
    )
    result = SmallIntegerField(null=True)
    template_id = IntegerField(null=True)
    remark = TextField(null=True)
    record = TextField(null=True)

    class Meta:
        db_table = 'connect_history'


class Contact(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    user_id = BigIntegerField()
    name = CharField(max_length=128, null=True)
    number = CharField(max_length=64)
    source = CharField(max_length=128, null=True)
    relationship = SmallIntegerField(choices=Relationship.choices())
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    latest_status = SmallIntegerField(
        choices=ContactStatus.choices(),
        default=ContactStatus.UNKNOWN.value,
    )
    latest_remark = TextField(null=True)
    total_count = IntegerField(null=True)
    total_duration = IntegerField(null=True)

    class Meta:
        db_table = 'contact'


class Division(GeModelBase):
    id = IntegerField(primary_key=True)
    type = SmallIntegerField()
    name = CharField(max_length=64)
    name_raw = CharField(max_length=64)
    is_top = SmallIntegerField()
    code = IntegerField(null=True)
    parent_id = IntegerField(null=True)

    class Meta:
        db_table = 'division'


class College(GeModelBase):
    code = CharField(max_length=16)
    name = CharField(max_length=128)

    class Meta:
        db_table = 'college'


class GeApplication(GeModelBase):
    id = BigIntegerField(primary_key=True)
    profile_province = ForeignKeyField(Division,
                                       related_name="profile_province_id")
    profile_city = ForeignKeyField(Division,
                                   related_name="profile_city_id")
    profile_district = ForeignKeyField(Division,
                                       related_name="profile_district_id")
    profile_college = ForeignKeyField(College,
                                      related_name='applications', null=True)
    job_city = ForeignKeyField(Division, related_name="job_city_id")
    job_province = ForeignKeyField(Division, related_name="job_province_id")
    job_district = ForeignKeyField(Division, related_name="job_district_id")

    class Meta:
        db_table = 'application'


def migrate_application():
    print('--------- application --------')
    cycle_conf = {
        1: [1, 15],
        2: [16, 30],
        3: [31, 60],
        4: [61, 90],
        5: [91, 999999],
    }
    staff_conf = {
        1: 11,
        2: 3,
        3: 31,
        4: 41,
        5: None,
    }
    cl_apps = ClApplication.select(
        ClApplication,
        CollectionNotes
    ).join(CollectionNotes, join_type=JOIN.LEFT_OUTER)
    app_insert_params = []
    for cl_app in cl_apps:
        print(cl_app.id)
        latest_notes = cl_app.latest_note
        latest_bombing_time = latest_notes and latest_notes.created_at
        promised_amount = latest_notes and latest_notes.promise_repay_amount
        promised_date = latest_notes and latest_notes.promise_repay_date
        follow_up_date = latest_notes and latest_notes.follow_up_date

        app_cycle = 1
        for cycle, scope in cycle_conf.items():
            if scope[0] <= cl_app.overdue_days <= scope[1]:
                app_cycle = cycle
                break

        app_insert_params.append({
            'id': cl_app.id,
            'user_id': cl_app.user_id,
            'user_mobile_no': cl_app.user_mobile_no,
            'user_name': cl_app.user_name,
            'device_no': cl_app.device_no,
            'app': cl_app.app,
            'amount': cl_app.amount,
            'amount_net': cl_app.amount_net,
            'term': cl_app.term,
            'due_at': cl_app.due_at,
            'apply_at': cl_app.apply_at,

            'status': cl_app.status,
            'latest_bomber_id': (staff_conf[app_cycle]
                                 if cl_app.collector_id else None),
            'claimed_at': cl_app.claimed_at,
            'overdue_days': cl_app.overdue_days,
            'cycle': app_cycle,
            'loan_success_times': cl_app.loan_success_times,
            'repaid': cl_app.paid,

            'latest_bombing_time': latest_bombing_time,
            'promised_amount': promised_amount,
            'promised_date': promised_date,
            'follow_up_date': follow_up_date,

            'arrived_at': cl_app.created_at,

            'created_at': cl_app.created_at,
            'updated_at': cl_app.updated_at,
        })
    with bb_db.atomic():
        Application.insert_many(app_insert_params).execute()


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


def complemented_application_json_info():
    print('--------- complemented application json info --------')
    apps = Application.select().order_by(Application.status)
    division = Division.select()
    division_map = {d.id: d.name for d in division}
    print(division_map)
    with bb_db.atomic():
        for app in apps:
            print(app.id)
            app.birth_date = birth_dt_ktp(app.id_ektp)
            app.gender = gender_ktpnum(app.id_ektp)

            ge_app = GeApplication.select(
                GeApplication.profile_city,
                GeApplication.profile_college,
                GeApplication.profile_district,
                GeApplication.profile_province,

                GeApplication.job_city,
                GeApplication.job_district,
                GeApplication.job_province,
            ).where(GeApplication.id == app.id)
            if not ge_app.exists():
                continue

            ge_app = ge_app.get()
            app.profile_city = division_map[ge_app.profile_province_id]
            if ge_app.profile_college_id:
                app.profile_college = ge_app.profile_college.name
            app.profile_district = division_map[ge_app.profile_district_id]
            app.profile_province = division_map[ge_app.profile_province_id]

            app.job_city = division_map[ge_app.job_city_id]
            app.job_district = division_map[ge_app.job_district_id]
            app.job_province = division_map[ge_app.job_province_id]
            app.save()


def sync_application_info():
    '''
    sync_basic_info =
        update bomber.application ba, golden_eye.application ga
        set ba.contact = ga.contact,
        ba.id_ektp = ga.id_ektp,
        ba.profile_residence_time = ga.profile_residence_time,
        ba.profile_residence_type = ga.profile_residence_type,
        ba.profile_address = ga.profile_address,
        ba.profile_education = ga.profile_education,
        ba.job_name = ga.job_name,
        ba.job_tel = ga.job_tel,
        ba.job_bpjs = ga.job_bpjs,
        ba.job_user_email = ga.job_user_email,
        ba.job_type = ga.job_type,
        ba.job_industry = ga.job_industry,
        ba.job_department = ga.job_department,
        ba.job_address = ga.job_address,
        ba.interest_rate = ga.interest_rate
        where ba.id = ga.id
    sync_bill_info =
        update bomber.application ba, repayment.bill2 rb
        set ba.late_fee_rate = rb.late_fee_rate,
        ba.late_fee_initial = rb.late_fee_initial,
        ba.late_fee = rb.late_fee,
        ba.late_fee = rb.late_fee,
        ba.origin_due_at = rb.origin_due_at,
        ba.repay_at = rb.latest_repay_at,
        ba.principal_paid = rb.principal_paid,
        ba.late_fee_paid = rb.late_fee_paid,
        ba.repaid = rb.principal_paid+rb.late_fee_paid,
        ba.unpaid = (rb.principal + rb.late_fee -
        rb.principal_paid - rb.late_fee_paid)
        where ba.id = rb.external_id
    '''
    pass


def sync_contacts():
    print('--------- start sync contacts --------')
    apps = Application.select().order_by(Application.status)
    total = apps.count()
    completed = 0
    for app in apps:
        add_contact(app)
        completed += 1
        print('total %s  completed %s  |  %s %% percent' %
              (total, completed, round(completed/total*100, 2)))


def add_contact(application):
    print('--------- add contact for application %s --------', application.id)

    # 添加联系人信息（添加联系人时默认即为‘有用’）
    contacts = Contact.filter(
        Contact.user_id == application.user_id,
    )
    existing_numbers = {contact.number for contact in contacts}

    insert_contacts = list()

    # applicant
    if (application.user_mobile_no and
            application.user_mobile_no not in existing_numbers):
        insert_contacts.append({
            'user_id': application.user_id,
            'name': application.user_name,
            'number': application.user_mobile_no,
            'relationship': Relationship.APPLICANT.value,
            'source': 'apply info',
        })
        existing_numbers.add(application.user_mobile_no)

    extra_phone = GoldenEye().get(
        '/users/%s/extra-phone' % application.user_id
    )
    if not extra_phone.ok:
        extra_phone = []
        logging.error('get user %s extra contacts failed', application.user_id)
    else:
        extra_phone = extra_phone.json()['data']

    for i in extra_phone:
        if i['number'] in existing_numbers:
            continue
        insert_contacts.append({
            'user_id': application.user_id,
            'name': application.user_name,
            'number': i['number'][:64],
            'relationship': Relationship.APPLICANT.value,
            'source': 'extra phone',
        })
        existing_numbers.add(i['number'])

    # family
    # ec contact
    contact = json.loads(application.contact or '[]')
    for i in contact:
        if i['mobile_no'] not in existing_numbers:
            insert_contacts.append({
                'user_id': application.user_id,
                'name': i['name'],
                'number': i['mobile_no'],
                'relationship': Relationship.FAMILY.value,
                'source': 'ec',
            })
            existing_numbers.add(i['mobile_no'])
        if i['type'] != 1:
            continue
        if i['tel_no'] not in existing_numbers:
            insert_contacts.append({
                'user_id': application.user_id,
                'name': i['name'],
                'number': i['tel_no'],
                'relationship': Relationship.FAMILY.value,
                'source': 'ec',
            })
            existing_numbers.add(i['tel_no'])

    fm = GoldenEye().get(
        '/applications/%s/contact/family-member' % application.id
    )
    if not fm.ok:
        family = []
        logging.error('get application %s family-member info error',
                      application.id)
    else:
        family = fm.json()['data']

    for i in family:
        if i['numbers'] in existing_numbers:
            continue
        insert_contacts.append({
            'user_id': application.user_id,
            'name': i['name'][:128],
            'number': i['numbers'][:64],
            'relationship': Relationship.FAMILY.value,
            'source': 'family member',
        })
        existing_numbers.add(i['numbers'])

    # company
    if application.job_tel and application.job_tel not in existing_numbers:
        insert_contacts.append({
            'user_id': application.user_id,
            'name': None,
            'number': application.job_tel,
            'relationship': Relationship.COMPANY.value,
            'source': 'basic info job_tel',
        })
        existing_numbers.add(application.job_tel)

    # suggested

    sms_contacts = GoldenEye().get(
        '/applications/%s/sms-contacts' % application.id
    )
    if not sms_contacts.ok:
        sms_contacts = []
        logging.info('get user %s sms contacts failed' % application.id)
    else:
        sms_contacts = sms_contacts.json()['data']

    for i in sms_contacts:
        if i['number'] in existing_numbers:
            continue
        insert_contacts.append({
            'user_id': application.user_id,
            'name': i['name'][:128],
            'number': i['number'][:64],
            'relationship': Relationship.SUGGESTED.value,
            'source': 'sms contacts',
        })
        existing_numbers.add(i['number'])

    if insert_contacts:
        try:
            Contact.insert_many(insert_contacts).execute()
        except:
            print(insert_contacts)

    cf = GoldenEye().get(
        '/applications/%s/call/frequency' % application.id
    )
    if not cf.ok:
        call_frequency = []
        logging.error('get application %s call frequency error',
                      application.id)
    else:
        call_frequency = cf.json()['data']

    # 结构不一样，重新生成
    insert_contacts = []
    with bb_db.atomic():
        for i in call_frequency:
            if i['number'] in existing_numbers:
                (Contact
                 .update(total_count=i['total_count'],
                         total_duration=i['total_duration'])
                 .where(Contact.number == i['number'],
                        Contact.user_id == application.user_id))
                continue

            insert_contacts.append({
                'user_id': application.user_id,
                'name': i['name'][:128],
                'number': i['number'][:64],
                'relationship': Relationship.SUGGESTED.value,
                'total_count': i['total_count'],
                'total_duration': i['total_duration'],
                'source': 'call frequency',
            })
        if insert_contacts:
            try:
                Contact.insert_many(insert_contacts).execute()
            except:
                print(insert_contacts)


def sync_collection_history():
    print('--------- start sync collection notes --------')
    relation_map = {
        RelationType.PARENTS.value: Relationship.FAMILY.value,
        RelationType.MATE.value: Relationship.FAMILY.value,
        RelationType.RELATION.value: Relationship.FAMILY.value,
        RelationType.COLLEAGUE.value: Relationship.SUGGESTED.value,
        RelationType.FRIEND.value: Relationship.SUGGESTED.value,
        RelationType.SCHOOLMATE.value: Relationship.SUGGESTED.value,
        RelationType.OTHER.value: Relationship.SUGGESTED.value,
        RelationType.SELF.value: Relationship.APPLICANT.value,
        None: Relationship.SUGGESTED.value,
    }
    sub_relation_map = {
        RelationType.PARENTS.value: SubRelation.UNKNOWN.value,
        RelationType.MATE.value: SubRelation.UNKNOWN.value,
        RelationType.RELATION.value: SubRelation.RELATIVE.value,
        RelationType.COLLEAGUE.value: SubRelation.COLLEAGUE.value,
        RelationType.FRIEND.value: SubRelation.FRIEND.value,
        RelationType.SCHOOLMATE.value: SubRelation.FRIEND.value,
        RelationType.OTHER.value: SubRelation.UNKNOWN.value,
        RelationType.SELF.value: SubRelation.UNKNOWN.value,
        None: Relationship.SUGGESTED.value,
    }

    collections = CollectionNotes.select()
    insert_bombing = []
    insert_connect = []
    for c in collections:
        print(c.id)
        insert_bombing.append({
            'application': c.application_id,
            'bomber_id': c.collector_id,
            'promised_amount': c.promise_repay_amount,
            'promised_date': c.promise_repay_date,
            'follow_up_date': c.follow_up_date,
            'remark': c.overdue_reason,
            'created_at': c.created_at,
            'updated_at': c.updated_at,
        })
        insert_connect.append({
            'application': c.application_id,
            'type': ConnectType.CALL.value,
            'operator_id': c.collector_id,
            'number': c.phone,
            'relationship': relation_map[c.relation],
            'sub_relation': sub_relation_map[c.relation],
            'status': c.phone_validity + 1,
            'remark': c.note,
            'created_at': c.created_at,
            'updated_at': c.updated_at,
        })
    with bb_db.atomic():
        for idx in range(0, len(insert_bombing), 1000):
            (BombingHistory.insert_many(insert_bombing[idx:idx + 1000])
             .execute())

    with bb_db.atomic():
        for idx in range(0, len(insert_bombing), 1000):
            (ConnectHistory.insert_many(insert_connect[idx:idx + 1000])
             .execute())


if __name__ == '__main__':
    command = sys.argv[1]
    if command == '1':
        migrate_application()
    if command == '2':
        sync_application_info()
    if command == '3':
        complemented_application_json_info()
    if command == '4':
        sync_contacts()
    if command == '5':
        sync_collection_history()
