import json
import inspect
from decimal import Decimal
from datetime import (
    timedelta,
    datetime,
    tzinfo)

import bottle
import jwt
import mongoengine as mon

from peewee import (
    SmallIntegerField,
    BigIntegerField,
    ForeignKeyField,
    DateTimeField,
    BooleanField,
    DoesNotExist,
    IntegerField,
    DecimalField,
    CompositeKey,
    FloatField,
    CharField,
    TextField,
    DateField,
    Model,
    R,
)
from enum import Enum

from bomber.constant_mapping import (
    LastConnectionToApplicant,
    ApplicationStatus,
    RealRelationship,
    ConnectApplicant,
    CallActionCommit,
    ApplicationType,
    CallActionType,
    AutoListStatus,
    AutoCallResult,
    ApprovalStatus,
    EscalationType,
    ContactsUseful,
    PriorityStatus,
    OldLoanStatus,
    InboxCategory,
    OverdueReason,
    ContactStatus,
    ConnectResult,
    BombingResult,
    DisAppStatus,
    Relationship,
    ConnectType,
    InboxStatus,
    SubRelation,
    PhoneStatus,
    HelpWilling,
    CallAPIType,
    PayWilling,
    RoleStatus,
    RoleWeight,
    OfficeType,
    AdmitLoan,
    JobOption,
    Helpful,
    RipeInd,
    HasJob,
)
from bomber.db import db, db_auto_call
from bomber.utils import idg, request_ip, ChoiceEnum

app = bottle.default_app()


def get_ordered_models(module):
    """ 按代码出现的先后顺序获取某一 module 中所有的 peewee.Model 子类 """

    def is_model(m):
        return isinstance(m, type) and issubclass(m, Model) and m != Model

    members = inspect.getmembers(module, is_model)
    # 按代码中的先后顺序排序
    members.sort(key=lambda x: inspect.getsourcelines(x[1])[1])
    return [model for _, model in members]


class ModelBase(Model):
    created_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP')],
                               default=datetime.now)
    updated_at = DateTimeField(constraints=[R('DEFAULT CURRENT_TIMESTAMP'),
                                            R('ON UPDATE CURRENT_TIMESTAMP')],
                               default=datetime.now)

    class Meta:
        database = db
        only_save_dirty = True

    def update_dict(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    @classmethod
    def get_or_none(cls, *query, **kwargs):
        try:
            return cls.get(*query, **kwargs)
        except DoesNotExist:
            return None


class APIUser(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    name = CharField(max_length=64)
    token = CharField(max_length=64)
    expire_at = DateTimeField(null=True)

    class Meta:
        db_table = 'api_user'


class Role(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    name = CharField(max_length=64)
    permission = TextField()
    cycle = IntegerField()
    weight = SmallIntegerField(choices=RoleWeight.choices(),
                               default=RoleWeight.MEMBER.value)
    status = SmallIntegerField(choices=RoleStatus.choices(),
                               default=RoleStatus.NORMAL.value)

    class Meta:
        db_table = 'role'

    def get_leader_ids(self):
        return []


class Partner(ModelBase):
    id = IntegerField(primary_key=True)
    cycle = IntegerField()
    name = CharField(max_length=128)
    app_percentage = DecimalField(4, 2)
    status = SmallIntegerField()

    class Meta:
        db_table = 'partner'


class Bomber(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    ext = IntegerField(null=True)
    # call_log 工号gh字段
    employee_id = CharField(max_length=10)
    name = CharField(max_length=32)
    username = CharField(max_length=32)
    password = CharField(max_length=64)
    email = CharField(max_length=32, null=True)
    phone = CharField(max_length=16, null=True)
    last_active_at = DateField(null=True)
    role = ForeignKeyField(Role, related_name='bomber')
    partner = ForeignKeyField(Partner, related_name='bomber')
    status = IntegerField(null=False)
    type = IntegerField(default=CallAPIType.TYPE1.value, index=True)
    is_del = IntegerField(default=0, null=True)
    group_id = IntegerField(default=0, null=True)
    # 负责催收分期件的标志
    # 直接存储本负责分期的cycle
    instalment = IntegerField(default=0)
    auto_ext = IntegerField(null=True)

    class Meta:
        db_table = 'bomber'

    def logged_in(self, expire_days=8):
        headers = bottle.request.headers
        expire_at = datetime.now() + timedelta(days=expire_days)
        session = Session.create(bomber=self,
                                 ip=request_ip(),
                                 referer=headers.get('referer'),
                                 ua=headers.get('User-Agent'),
                                 expire_at=expire_at)
        BomberLoginLog.create(bomber=self,
                              ip=request_ip(),
                              referer=headers.get('referer'),
                              ua=headers.get('User-Agent'))
        return session


class BomberLog(ModelBase):
    id = IntegerField(primary_key=True)
    bomber_id = BigIntegerField()
    role_id = IntegerField()
    operation = IntegerField()
    operation_id = BigIntegerField()
    comment = TextField(null=True)

    class Meta:
        db_table = 'bomber_log'


class Session(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    bomber = ForeignKeyField(Bomber, related_name='sessions')
    ip = CharField(max_length=64)
    referer = CharField(max_length=512, null=True)
    ua = TextField(null=True)
    expire_at = DateTimeField()

    class Meta:
        db_table = 'session'

    def jwt_token(self):
        token = jwt.encode({
            'bomber_id': str(self.bomber.id),
            'session_id': str(self.id),
        }, app.config['user.secret'])
        return token.decode('utf-8')


class BomberLoginLog(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    bomber = ForeignKeyField(Bomber, related_name='login_logs')
    ip = CharField(max_length=64)
    referer = CharField(max_length=512, null=True)
    ua = TextField(null=True)

    class Meta:
        db_table = 'bomber_login_log'

# 分期上线之后application不再是已件的维度来，而是已催收单的维度类。
# application表名没变是为了兼容以前的系统。application现在代表的是催收单
class Application(ModelBase):
    # common info
    # user info
    # 催收单id，分期的id要自己创建
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
    # late_fee_rate = DecimalField(max_digits=8, decimal_places=6, null=True)
    # late_fee_initial = DecimalField(max_digits=15, decimal_places=2, null=True)
    # late_fee = DecimalField(max_digits=15, decimal_places=2, null=True)
    term = IntegerField(null=True)
    overdue_days = IntegerField(default=1)
    # repaid = DecimalField(max_digits=15, decimal_places=2, null=True)
    # unpaid = DecimalField(max_digits=15, decimal_places=2, null=True)

    # repayment bill
    interest = DecimalField(max_digits=15, decimal_places=2, default=0)
    # principal_paid = DecimalField(max_digits=15, decimal_places=2, null=True)
    # late_fee_paid = DecimalField(max_digits=15, decimal_places=2, null=True)
    origin_due_at = DateTimeField(null=True)
    # due_at = DateTimeField(null=True)
    repay_at = DateTimeField(null=True)

    # bomber control
    is_rejected = BooleanField(default=False)
    cycle = IntegerField(default=1)
    loan_success_times = IntegerField(default=1)
    latest_bomber = ForeignKeyField(Bomber,
                                    related_name='application', null=True)
    last_bomber = ForeignKeyField(Bomber, null=True)
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
    called_times = IntegerField(default=0)
    C1A_entry = DateTimeField(null=True)
    C1B_entry = DateTimeField(null=True)
    C2_entry = DateTimeField(null=True)
    C3_entry = DateTimeField(null=True)
    ptp_bomber = IntegerField(null=True)
    latest_call = IntegerField(null=True)
    # 标记这个件是否有效，(接收到银行还款有延迟，可能会造成没有逾期的件进入bomber)
    no_active = IntegerField(default=0)
    # external_id 记录申请件的id
    external_id = BigIntegerField(null=True)
    # 催收单的类型
    type = SmallIntegerField(
        choices=ApplicationType.choices(),
        default=ApplicationType.CASH_LOAN.value
    )
    # 账单id
    bill_id = BigIntegerField(null=True)
    dpd1_entry = DateTimeField(null=True)

    class Meta:
        db_table = 'application'


class RepaymentLog(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'repayment_log')
    is_bombed = BooleanField(null=True)
    current_bomber = ForeignKeyField(Bomber, 'repayment_log')
    cycle = IntegerField()
    principal_part = DecimalField(max_digits=15, decimal_places=2, null=True)
    late_fee_part = DecimalField(max_digits=15, decimal_places=2, null=True)
    repay_at = DateTimeField(null=True)
    ptp_bomber = IntegerField(null=True)
    latest_call = IntegerField(null=True)
    no_active = IntegerField(default=0)
    # 新增分期期数
    periods = IntegerField(null=True)
    overdue_bill_id = IntegerField(null=True)
    partner_bill_id = CharField(max_length=64, null=True)

    class Meta:
        db_table = 'repayment_log'


class BombingHistory(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'bombed_histories')
    user_id = BigIntegerField(null=True)
    ektp = CharField(max_length=32, null=True)
    cycle = IntegerField(null=True)
    bomber = ForeignKeyField(Bomber, 'bombed_histories')
    promised_amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    promised_date = DateField(null=True)
    follow_up_date = DateTimeField(null=True)
    result = SmallIntegerField(choices=BombingResult.choices(), null=True)
    remark = TextField(null=True)

    class Meta:
        db_table = 'bombing_history'


class Discount(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'discounts')
    operator = ForeignKeyField(Bomber, 'operated_discounts')
    reviewer = ForeignKeyField(Bomber, 'reviewed_discounts', null=True)
    reviewed_at = DateTimeField()
    status = SmallIntegerField(choices=ApprovalStatus.choices(),
                               default=ApprovalStatus.PENDING.value)
    discount_to = DecimalField(max_digits=15, decimal_places=2)
    effective_to = DateTimeField()
    reason = TextField(null=True)
    comment = TextField(null=True)
    overdue_bill_id = BigIntegerField(null=True)
    periods = IntegerField(null=True)

    class Meta:
        db_table = 'discount'


class Escalation(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'escalations')
    current_bomber = ForeignKeyField(Bomber, 'operated_escalations', null=True)
    reviewer = ForeignKeyField(Bomber, 'reviewed_escalations', null=True)
    reviewed_at = DateTimeField(null=True)
    type = SmallIntegerField(choices=EscalationType.choices(),
                             default=EscalationType.AUTOMATIC.value)
    status = SmallIntegerField(choices=ApprovalStatus.choices(),
                               default=ApprovalStatus.PENDING.value)
    current_cycle = IntegerField()
    escalate_to = IntegerField()
    reason = TextField(null=True)
    comment = TextField(null=True)

    class Meta:
        db_table = 'escalation'


class RejectList(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'reject_list')
    operator = ForeignKeyField(Bomber, 'operated_reject_list')
    reviewer = ForeignKeyField(Bomber, 'reviewed_reject_list', null=True)
    reviewed_at = DateTimeField(null=True)
    status = SmallIntegerField(choices=ApprovalStatus.choices(),
                               default=ApprovalStatus.PENDING.value)
    reason = TextField(null=True)
    comment = TextField(null=True)

    class Meta:
        db_table = 'reject_list'


class Transfer(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'transfers')
    operator = ForeignKeyField(Bomber, 'operated_transfers')
    reviewer = ForeignKeyField(Bomber, 'reviewed_transfers', null=True)
    reviewed_at = DateTimeField()
    current_bomber = ForeignKeyField(Bomber, 'current_bomber')
    transfer_to = ForeignKeyField(Bomber, 'transfer_to', null=True)
    status = SmallIntegerField(choices=ApprovalStatus.choices(),
                               default=ApprovalStatus.PENDING.value)
    reason = TextField(null=True)
    comment = TextField(null=True)

    class Meta:
        db_table = 'transfer'



#用户联系人
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
    useful = SmallIntegerField(default=ContactsUseful.NONE.value,
                               choices=ContactsUseful.choices())
    call_priority = SmallIntegerField(default=PriorityStatus.DEFAULT.value,
                                      choices=PriorityStatus.choices())
    real_relationship = SmallIntegerField(null=True)

    class Meta:
        db_table = 'contact'


class Template(ModelBase):
    type = SmallIntegerField(choices=ConnectType.choices())
    cycle = IntegerField()
    name = CharField(max_length=128)
    app = CharField(max_length=64, default='DanaCepat')
    text = TextField()

    @classmethod
    def get_auto_sms_tpl(cls, auto_sms_type):
        return {
            'T4_NEW': [135, 136, 137],
            'T6_NEW': [135, 136, 137],
            'T8_NEW': [135, 136, 137],
            'T10_NEW': [135, 136, 137],
            'T14_NEW': [135, 136, 137],

            'T4_OLD': [138, 139, 140],
            'T6_OLD': [138, 139, 140],
            'T8_OLD': [138, 139, 140],
            'T10_OLD': [138, 139, 140],
            'T14_OLD': [138, 139, 140],

            'T17_ALL': [141, 142, 143],
            'T21_ALL': [141, 142, 143],

            'T25_ALL': [144, 145, 146],
            'T29_ALL': [144, 145, 146],

            'REMIND_PROMISE_BEFORE': [147, 148, 149],
            'REMIND_PROMISE_AFTER': [150, 151, 152],

            'DISCOUNT_APPROVED': [153, 154, 155],
        }[auto_sms_type]

    @classmethod
    def get_daily_auto_sms_tpl(cls, auto_sms_type, app_name):
        return {
            ('T5_NEW', 'DanaCepat'): 102,
            ('T5_NEW', 'PinjamUang'): 103,
            ('T5_NEW', 'KtaKilat'): 104,
            ('T5_OLD', 'DanaCepat'): 105,
            ('T5_OLD', 'PinjamUang'): 106,
            ('T5_OLD', 'KtaKilat'): 107,
            ('T15_ALL', 'DanaCepat'): 108,
            ('T15_ALL', 'PinjamUang'): 109,
            ('T15_ALL', 'KtaKilat'): 110,
            ('T25_ALL', 'DanaCepat'): 111,
            ('T25_ALL', 'PinjamUang'): 112,
            ('T25_ALL', 'KtaKilat'): 113,
            ('T5_NEW', 'IkiModel'): 1001,
            ('T5_OLD', 'IkiModel'): 1002,
            ('T15_ALL', 'IkiModel'): 1003,
            ('T25_ALL', 'IkiModel'): 1004,
        }[(auto_sms_type, app_name)]

    @classmethod
    def get_daily_instalment_auto_sms_tpl(cls, overdue_days=None,loan_times=0):
        if not overdue_days:
            return False, ''
        # 逾期天数对应和对应的短信模板
        templates = {"SMS1": [1, 2, 3],
                     "SMS2": [4, 6, 8, 10, 14],
                     "SMS3": [17, 21],
                     "SMS4": [25, 29],
                     "SMS5": [31, 35, 39, 43, 47, 51, 55, 59],
                     "SMS6": [61, 65, 69, 73, 77, 81, 85, 89]}
        # 新老用户短信模板的id
        message_ids = {"SMS1-NEW": 401,
                       "SMS1-OLD": 402,
                       "SMS2-NEW": 403,
                       "SMS2-OLD": 404,
                       "SMS3-NEW": 405,
                       "SMS3-OLD": 406,
                       "SMS4-NEW": 406,
                       "SMS4-OLD": 406,
                       "SMS5-NEW": 407,
                       "SMS5-OLD": 407,
                       "SMS6-NEW": 408,
                       "SMS6-OLD": 408
                       }
        for sms_key, overdue in templates.items():
            if overdue_days in overdue:
                if loan_times == 0:
                    message_key = sms_key + '-NEW'
                else:
                    message_key = sms_key + '-OLD'
                message_id = message_ids.get(message_key)
                return message_id
        return False


class ConnectHistory(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'connect_history')
    type = SmallIntegerField(choices=ConnectType.choices())
    name = CharField(max_length=128, null=True)
    operator = ForeignKeyField(Bomber, 'operated_connect')
    number = CharField(max_length=64)
    relationship = SmallIntegerField(choices=Relationship.choices())
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    status = SmallIntegerField(
        choices=ContactStatus.choices(),
        default=ContactStatus.UNKNOWN.value,
    )
    result = SmallIntegerField(choices=ConnectResult.choices(), null=True)
    template = ForeignKeyField(Template, 'connect_history', null=True)
    remark = TextField(null=True)
    record = TextField(null=True)

    class Meta:
        db_table = 'connect_history'


class Inbox(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    title = CharField(max_length=128)
    content = TextField()
    receiver = ForeignKeyField(Bomber)
    status = SmallIntegerField(choices=InboxStatus.choices(),
                               default=InboxStatus.UNREAD.value)
    category = CharField(max_length=64, choices=InboxCategory.choices())

    class Meta:
        db_table = 'inbox'


class Summary(ModelBase):
    bomber = ForeignKeyField(Bomber, related_name='summary', null=True)
    cycle = IntegerField(null=True)
    claimed = IntegerField(default=0)
    completed = IntegerField(default=0)
    cleared = IntegerField(default=0)
    escalated = IntegerField(default=0)
    escalated_in = IntegerField(default=0)
    transferred = IntegerField(default=0)
    promised = IntegerField(default=0)
    amount_recovered = DecimalField(default=0)
    amount_recovered_total = DecimalField(default=0)
    calls_made = IntegerField(default=0)
    calls_connected = IntegerField(default=0)
    sms_sent = IntegerField(default=0)
    date = DateField()

    class Meta:
        db_table = 'summary'


class Summary2(ModelBase):
    bomber = ForeignKeyField(Bomber, related_name='summary2', null=True)
    cycle = IntegerField(null=True)
    answered_calls = IntegerField(default=0)
    ptp = IntegerField(default=0)
    follow_up = IntegerField(default=0)
    not_useful = IntegerField(default=0)
    cleared = IntegerField(default=0)
    amount_recovered = DecimalField(default=0)
    date = DateField(default=datetime.now)

    class Meta:
        db_table = 'summary2'


class CallLog(ModelBase):
    user_id = CharField(max_length=32, null=True)
    application_id = CharField(max_length=32, null=True)
    call_id = IntegerField()  # 编号
    record_id = IntegerField()
    time_start = DateTimeField()  # 通话起始时间
    time_end = DateTimeField()  # 通话结束时间
    talk_time = IntegerField(null=True)  # 呼叫时长
    cpn = CharField(max_length=50)  # 主叫号码
    cdpn = CharField(max_length=50)  # 被叫号码
    duration = IntegerField()  # 通话时长
    # 录音文件名，格式：年月日/主叫号码_被叫号码_年月日时分秒call_id 随机数_cd.wav或cg.wav
    recording = CharField(max_length=150)
    system_type = CharField(max_length=10, null=True)  # 系统
    xm = CharField(max_length=50, null=True)  # 姓名

    class Meta:
        db_table = 'call_log'


class AutoCallList(ModelBase):
    id = IntegerField(primary_key=True)
    application = ForeignKeyField(Application, 'auto_call')
    follow_up_date = DateTimeField(null=True)
    cycle = IntegerField()
    called_times = IntegerField(default=0)
    status = SmallIntegerField(choices=AutoListStatus.choices(),
                               default=AutoListStatus.PENDING.value)
    next_number = CharField(max_length=64, null=True)
    numbers = TextField(default='')
    called_counts = IntegerField(default=0)
    called_rounds = IntegerField(default=0)
    description = CharField(max_length=250)

    class Meta:
        db_table = 'auto_call_list'

#自动呼叫记录
class AutoCallActions(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'auto_call_actions')
    cycle = IntegerField(default=0)
    name = CharField(max_length=128, null=True)
    number = CharField(max_length=64)
    relationship = SmallIntegerField(choices=Relationship.choices())
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    bomber = ForeignKeyField(Bomber, 'auto_call_actions')
    result = SmallIntegerField(choices=AutoCallResult.choices(), null=True)
    reason = SmallIntegerField(null=True)
    promised_amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    promised_date = DateField(null=True)
    follow_up_date = DateTimeField(null=True)
    notes = TextField(null=True)
    auto_notes = TextField(null=True)
    call_id = CharField(max_length=60, null=True)

    class Meta:
        db_table = 'auto_call_actions'


class AutoCallActionsPopup(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    application = ForeignKeyField(Application, 'auto_call_actions_popup')
    cycle = IntegerField(default=0)
    name = CharField(max_length=128, null=True)
    number = CharField(max_length=64)
    relationship = SmallIntegerField(choices=Relationship.choices())
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    bomber = ForeignKeyField(Bomber, 'auto_call_actions_popup')

    class Meta:
        db_table = 'auto_call_actions_popup'


class IpWhitelist(ModelBase):
    ip = CharField(max_length=64, primary_key=True)

    class Meta:
        db_table = 'ip_whitelist'


#分件记录
class DispatchApp(ModelBase):
    application = ForeignKeyField(Application, 'dispatch_app')
    partner = ForeignKeyField(Partner, 'dispatch_app')
    bomber = ForeignKeyField(Bomber, 'dispatch_app')
    status = SmallIntegerField(choices=DisAppStatus.choices(),
                               default=DisAppStatus.NORMAL.value)

    class Meta:
        db_table = 'dispatch_app'


class Notes(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    groups = CharField()
    note = TextField()

    class Meta:
        db_table = 'notes'


class AutoCallListHistory(ModelBase):
    id = IntegerField(primary_key=True)
    list_id = IntegerField()
    application_id = IntegerField()
    follow_up_date = DateTimeField(null=True)
    cycle = IntegerField()
    called_times = IntegerField(default=0)
    status = SmallIntegerField(choices=AutoListStatus.choices(),
                               default=AutoListStatus.PENDING.value)
    next_number = CharField(max_length=64, null=True)
    numbers = TextField(default='')
    called_rounds = IntegerField(default=0)

    class Meta:
        db_table = 'auto_call_list_history'


class RepaymentReport(ModelBase):
    id = IntegerField(primary_key=True)
    cycle = IntegerField()
    time = DateTimeField()
    all_money = DecimalField(15, 2)
    repayment = DecimalField(15, 2)
    proportion = DecimalField(decimal_places=2, null=True)
    contain_out = IntegerField(default=0)
    type = IntegerField(default=0)

    class Meta:
        db_table = 'repayment_report'


class RepaymentReportInto(ModelBase):
    id = IntegerField(primary_key=True)
    cycle = IntegerField()
    time = DateTimeField()
    all_money = DecimalField(15, 3)
    repayment = DecimalField(15, 3)
    proportion = CharField(null=True)
    is_first_loan = IntegerField()
    contain_out = IntegerField()
    ripe_ind = IntegerField(default=RipeInd.NOTRIPE.value)

    class Meta:
        db_table = 'repayment_report_into'


class NewCdr(ModelBase):
    id = IntegerField()
    office_type = SmallIntegerField(default=OfficeType.DEFAULT.value)
    callid = CharField(max_length=30, null=True)
    timestart = DateTimeField(null=True)
    callfrom = CharField(max_length=20, null=True)
    callto = CharField(max_length=20, null=True)
    callto2 = CharField(max_length=20, null=True)
    callduraction = IntegerField(null=True)
    talkduraction = IntegerField(null=True)
    srctrunkname = CharField(max_length=50, null=True)
    dstrunkname = CharField(max_length=50, null=True)
    status = CharField(max_length=20, null=True)
    type = CharField(max_length=50, null=True)
    recording = CharField(max_length=100, null=True)
    UserNum = CharField(max_length=20, null=True)
    UserName = CharField(max_length=20, null=True)
    GroupID = IntegerField(null=True)
    GroupName = CharField(max_length=20, null=True)
    score = IntegerField(null=True)
    CustomerNumber = CharField(max_length=30, null=True)
    loanid = CharField(max_length=30, null=True)
    admin_id = IntegerField(null=True)
    surveyresult = IntegerField(default=0)
    didnumber = CharField(max_length=30, null=True)
    IsAnswer = IntegerField(default=0)
    OutCallType = CharField(max_length=50, null=True)
    tag = CharField(max_length=30, null=True)
    group_id = CharField(max_length=30, null=True)
    ext = CharField(max_length=10, null=True)
    agent_score = IntegerField(default=0)
    agent_score_addnum = CharField(max_length=10, null=True)
    agent_score_addtime = DateTimeField(null=True)
    is_play = IntegerField(default=0)
    remarks = CharField(max_length=255, null=True)


    class Meta:
        database = db_auto_call
        db_table = 'newcdr'
        primary_key = CompositeKey("id", "office_type")


class AgentStatus(ModelBase):
    id = IntegerField()
    office_type = SmallIntegerField(default=OfficeType.DEFAULT.value)
    datetime = DateTimeField(null=True)
    status = CharField(max_length=50, null=True)
    username = CharField(max_length=30, null=True)
    usernum = CharField(max_length=30, null=True)
    duration = IntegerField(null=True)

    class Meta:
        database = db_auto_call
        db_table = 'agent_status'
        primary_key = CompositeKey("id", "office_type")


class RAgentType(ModelBase):
    id = IntegerField(primary_key=True)
    JOB_NUMBER = CharField(max_length=50, null=True)
    USER_NAME = CharField(max_length=50, null=True)
    TYPE = CharField(max_length=50, null=True)
    START_TIME = DateTimeField(null=True)
    END_TIME = DateTimeField(null=True)
    TIME_LENGTH = IntegerField(null=True)
    EXT = CharField(max_length=10, null=True)
    USER_GROUP = CharField(max_length=10, null=True)
    CALL_FROM = CharField(max_length=30, null=True)
    CALL_TO = CharField(max_length=30, null=True)

    class Meta:
        database = db_auto_call
        db_table = 'r_agent_type'


class Record(ModelBase):
    id = IntegerField(primary_key=True)
    Cdrid = CharField(null=True)
    callid = IntegerField(null=True)
    visitor = CharField(null=True)
    outer = IntegerField(null=True)
    Type = CharField(null=True)
    Route = CharField(null=True)
    TimeStart = DateTimeField(null=True)
    TimeEnd = DateTimeField(null=True)
    TalkTime = IntegerField(null=True)
    CPN = CharField(null=True)
    CDPN = CharField(null=True)
    Duration = IntegerField(null=True)
    TrunkNumber = CharField(null=True)
    Recording = CharField(null=True)
    gh = CharField(null=True)
    xm = CharField(null=True)
    zuhao = IntegerField(null=True)
    RingTime = IntegerField(null=True)
    WaitTime = IntegerField(null=True)
    pingfen = IntegerField(null=True)
    AuditNumber = CharField(null=True)
    string1 = CharField(null=True)
    string2 = CharField(null=True)
    string3 = CharField(null=True)

    class Meta:
        database = db_auto_call
        indexes = (
            (('TimeStart', 'CPN'), False),
        )
        db_table = 'record'


class WorkerResult(ChoiceEnum):
    NONE = 0
    DONE = 1
    FAILED = 2


class WorkerLog(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    message_id = CharField(max_length=64)
    action = CharField(max_length=64)
    payload = TextField(null=True)
    result = SmallIntegerField(choices=WorkerResult.choices(),
                               default=WorkerResult.NONE.value)
    time_spent = DecimalField(15, 3)
    traceback = TextField(null=True)
    receipt_handle = TextField(null=True)

    class Meta:
        db_table = 'worker_log'


class OutsourcingApp(ModelBase):
    application_id = IntegerField(primary_key=True)
    status = SmallIntegerField()

    class Meta:
        db_table = 'outsourcing_app'


class AutoCallListRecord(ModelBase):
    id = IntegerField()
    application = BigIntegerField()
    follow_up_date = DateTimeField(null=True)
    cycle = IntegerField()
    called_times = IntegerField(default=0)
    status = SmallIntegerField(choices=AutoListStatus.choices(),
                               default=AutoListStatus.PENDING.value)
    next_number = CharField(max_length=64, null=True)
    numbers = TextField(default='')
    called_counts = IntegerField(default=0)
    called_rounds = IntegerField(default=0)

    class Meta:
        db_table = 'auto_call_list_record'


class CycleList(ChoiceEnum):
    cycle_1a = 'a', 1, '1A'
    cycle_1b = 'b', 2, '1B'
    cycle_2 = '2', 3, '2'
    cycle_3 = '3', 4, '3'

    @classmethod
    def values(cls):
        return [i.value[0] for i in cls]

    @classmethod
    def sql_values(cls):
        return [i.value[1] for i in cls]

    @classmethod
    def table_values(cls):
        return [i.value[2] for i in cls]


class ReportCollection(ModelBase):
    apply_date = DateField(null=False)
    cycle = CharField(max_length=10, null=False)
    all_overdue_loan = IntegerField(null=True)
    overdue_loans_entered_into_predict_call_system = FloatField(null=True)
    of_overdue_loans_entered_into_predict_call_system = FloatField(null=True)
    loans_completed = IntegerField(null=True)
    of_completed_loans_in_predict_call_system = FloatField(null=True)
    connected_calls_automatic = FloatField(null=True)
    connected_calls_automatic_completed_loans = FloatField(null=True)
    connected_calls_manual = IntegerField(null=True)
    agent = IntegerField(null=True)
    average_calls_agent = FloatField(null=True)
    average_call_duration_team = FloatField(null=True)

    class Meta:
        db_table = 'report_collection'
        primary_key = CompositeKey('apply_date', 'cycle')


class DispatchAppHistory(ModelBase):
    id = IntegerField(primary_key=True)
    application = ForeignKeyField(Application, 'dispatch_app_history')
    partner_id = IntegerField(null=True)
    bomber_id = IntegerField(null=False)
    entry_at = DateTimeField(null=False)
    entry_overdue_days = IntegerField(null=False)
    entry_principal_pending = DecimalField(null=False)
    entry_late_fee_pending = DecimalField(null=False)
    expected_out_time = DateField(null=False)
    out_at = DateTimeField(null=True)
    out_overdue_days = IntegerField(null=True)
    out_principal_pending = DecimalField(null=True)
    out_late_fee_pending = DecimalField(null=True)

    class Meta:
        db_table = 'dispatch_app_history'


class AutoIVRStatus(ChoiceEnum):
    AVAILABLE = 0
    PROCESSING = 1
    REPAID = 2
    SUCCESS = 3


class AutoIVR(ModelBase):
    id = IntegerField(null=False)
    application_id = BigIntegerField(null=False)
    user_id = BigIntegerField()
    numbers = TextField(default='')
    call_time = IntegerField(default=0)
    called = IntegerField(default=0)
    group = IntegerField(null=False)
    status = IntegerField(default=AutoIVRStatus.AVAILABLE.value)

    class Meta:
        db_table = 'auto_ivr'

    # dpd1-3的group
    @classmethod
    def dpd_groups(cls):
        dpd_groups = [3, 6, 9, 12, 15, 18, 19, 20, 22, 23,
                      25, 26, 28, 29, 30, 31, 32, 33]
        return dpd_groups


class IVRCallStatus(ChoiceEnum):
    FAILED = 0
    SUCCESS = 1
    # 需查application表确认是否已还款
    CALLBACKFAILEDEXCEPTION = 2
    # 需查application表确认是否已还款
    CALLBACKSUCCESSEXCEPTION = 3

    @classmethod
    def call_success(cls):
        return [cls.SUCCESS.value, cls.CALLBACKSUCCESSEXCEPTION.value]


class AutoIVRActions(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    callstate = IntegerField(default=IVRCallStatus.FAILED.value, index=True)
    group = IntegerField(null=False)
    customer_number = BigIntegerField()
    loanid = BigIntegerField()
    callid = CharField(index=True, null=True)
    timestart = DateTimeField(null=True)
    callfrom = CharField(null=True)
    callto = CharField(null=True)
    callduraction = IntegerField(default=0)
    talkduraction = IntegerField(default=0)
    dstrunkname = CharField(null=True)

    class Meta:
        db_table = 'auto_ivr_actions'


class ManualCallListStatus(ChoiceEnum):
    WAITING = 0
    FAILED = 1
    SUCCESS = 2

    @classmethod
    def available(cls):
        return [cls.WAITING.value, cls.FAILED.value]


class ManualCallList(ModelBase):
    id = BigIntegerField(primary_key=True)
    batch_id = BigIntegerField()
    # JSON, 数组
    application_ids = TextField()
    # sql更新成功的行数
    length = BigIntegerField(default=0)
    src_bomber_id = BigIntegerField()
    dest_bomber_id = BigIntegerField()
    dest_partner_id = IntegerField(null=True)
    # JSON, 对象
    src_params = TextField()
    # JSON, 对象
    dest_params = TextField()
    update_dispatch_app = BooleanField(default=False)
    status = IntegerField(default=ManualCallListStatus.WAITING.value,
                          index=True)

    class Meta:
        db_table = 'manual_call_list'


class CompanyContactType(ChoiceEnum):
    BASIC_INFO_JOB_TEL = 'basic info job_tel'


class FamilyContactType(ChoiceEnum):
    EC = 'ec'
    CALLEC = 'call ec'
    # 通话频率排名前5
    CALLTOP5 = 'call top5'
    CONTACTEC = 'contact ec'
    REPAIREC = 'repair ec'


    @classmethod
    def c1a_order(cls):
        # C1A ec和fc按下面顺序排序拨打电话
        # 暂时family只打ec
        return [
            cls.EC.value,
            # cls.CALLEC.value,
            # cls.CALLTOP5.value,
            cls.REPAIREC.value
        ]

    @classmethod
    def c1b_order(cls):
        # C1B ec和fc按下面顺序排序拨打电话
        # 暂时family只打ec
        return [
            cls.EC.value,
            # cls.CALLEC.value,
            # cls.CALLTOP5.value,
            cls.REPAIREC.value
        ]


class SCI(Enum):
    AB_TEST_C1B = 'AB_TEST_C1B', [97, 98, 99]
    AB_TEST_C2 = ('AB_TEST_C2',
                  [76, 100, 106, 107, 213, 215, 216, 221, 222, 223, 226, 235])
    AB_TEST_C3 = ('AB_TEST_C3',
                  [41, 64, 69, 102, 109, 224, 225, 232, 236])
    CYCLE_1_DISCOUNT = 'CYCLE_1_DISCOUNT', Decimal('0.3')
    CYCLE_2_DISCOUNT = 'CYCLE_2_DISCOUNT', Decimal('0.3')
    CYCLE_3_DISCOUNT = 'CYCLE_3_DISCOUNT', Decimal('0.35')
    CYCLE_4_DISCOUNT = 'CYCLE_4_DISCOUNT', Decimal('0.4')
    CYCLE_5_DISCOUNT = 'CYCLE_5_DISCOUNT', Decimal('0.5')
    OLD_APP_PERIOD = 'OLD_APP_PERIOD', 7

    def __init__(self, key, default_value=''):
        self.key = key
        self.default_value = default_value
        self.type = type(default_value)

    def typed_value(self, value):
        if self.type in (dict, list):
            return json.loads(value)
        return self.type(value)


class SystemConfig(ModelBase):
    key = CharField(primary_key=True)
    name = CharField(max_length=128)
    value = CharField(max_length=1024)

    @classmethod
    def prefetch(cls, *items):
        configs = cls.filter(cls.key << [i.key for i in items])

        result = {}
        for c in configs:
            sci = SCI[c.key]
            result[sci] = sci.typed_value(c.value)
        return result

    @classmethod
    def item(cls, item):
        c = cls.get(cls.key == item.key)
        return SCI[c.key].typed_value(c.value)

    class Meta:
        db_table = 'system_config'


class ChinaZone(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=+8)

    def tzname(self, dt):
        return "China"

    def dst(self, dt):
        return timedelta(0)


class ChinaDateTimeField(mon.DateTimeField):
    def to_python(self, value):
        value = super().to_python(value)
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        return value

    def to_mongo(self, value):
        value = super().to_mongo(value)
        if isinstance(value, datetime):
            return value.replace(tzinfo=ChinaZone())
        return value


class BaseDocument(mon.Document):
    meta = {
        'abstract': True,
    }
    created_at = ChinaDateTimeField(default=datetime.now, db_field='ct')
    updated_at = ChinaDateTimeField(default=datetime.now, db_field='ut')

    def save(self, *args, **kwargs):
        self.updated_at = datetime.now()
        return super().save(*args, **kwargs)

    @classmethod
    def create(cls, **kws):
        """兼容 peewee"""
        return cls(**kws).save()

    def delete_instance(self):
        """兼容 peewee"""
        return self.delete()


class DynamicDocument(BaseDocument):
    meta = {
        'abstract': True,
    }

    @classmethod
    def create(cls, **kwargs):
        """兼容peewee的方法"""
        return cls(**kwargs).save()

    def delete_instance(self):
        """兼容peewee的方法"""
        return self.delete()

    def save(self, *args, **kwargs):
        return super().save(*args, **kwargs)


class MonLogger(DynamicDocument):
    meta = {
        'collection': 'logger'
    }
    func_name = mon.StringField(db_field='func')
    r_url = mon.StringField(db_field='r_url')
    status = mon.StringField(db_field='status')
    time_diff = mon.FloatField(db_field='t_dif')
    params = mon.StringField(null=True, db_field='prms')
    args = mon.StringField(null=True, db_field='ags')
    kwargs = mon.StringField(null=True, db_field='kags')
    body = mon.StringField(null=True, db_field='bd')


#手动呼叫纪录
class CallActions(ModelBase):
    id = BigIntegerField(default=idg, primary_key=True)
    type = IntegerField(default=CallActionType.MANUAL.value)
    cycle = IntegerField(default=0)
    name = CharField(max_length=128, null=True)
    number = CharField(max_length=64, null=True)
    relationship = SmallIntegerField(choices=Relationship.choices(), null=True)
    sub_relation = SmallIntegerField(default=SubRelation.UNKNOWN.value,
                                     choices=SubRelation.choices())
    bomber_id = BigIntegerField(index=True, null=True)
    # 自动中该字段对应auto_call_actions.id,手动中对应connect_history.id
    call_record_id = BigIntegerField(index=True, null=True)
    contact_id = BigIntegerField(index=True, null=True)
    application = ForeignKeyField(Application, 'call_actions')
    phone_status = SmallIntegerField(index=True,
                                     null=True,
                                     choices=PhoneStatus.choices())
    real_relationship = SmallIntegerField(null=True,
                                          choices=RealRelationship.choices())
    admit_loan = SmallIntegerField(null=True, choices=AdmitLoan.choices())
    still_old_job = SmallIntegerField(null=True, choices=JobOption.choices())
    new_company = CharField(null=True)
    overdue_reason = SmallIntegerField(null=True,
                                       choices=OverdueReason.choices())
    overdue_reason_desc = CharField(null=True)
    pay_willing = SmallIntegerField(null=True, choices=PayWilling.choices())
    pay_ability = SmallIntegerField(null=True, choices=PayWilling.choices())
    note = CharField(null=True)
    commit = SmallIntegerField(null=True, choices=CallActionCommit.choices())
    connect_applicant = SmallIntegerField(null=True,
                                          choices=ConnectApplicant.choices())
    has_job = SmallIntegerField(null=True, choices=HasJob.choices())
    help_willing = SmallIntegerField(null=True, choices=HelpWilling.choices())
    no_help_reason = CharField(null=True)
    last_connection_to_applicant = SmallIntegerField(
        null=True,
        choices=LastConnectionToApplicant.choices())

    promised_amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    promised_date = DateField(null=True)
    follow_up_date = DateTimeField(null=True)
    call_id = CharField(max_length=60, null=True)
    helpful = SmallIntegerField(null=True, choices=Helpful.choices())

    class Meta:
        db_table = 'call_actions'


class IVRActionLog(ModelBase):
    id = BigIntegerField(primary_key=True)
    total_page = BigIntegerField(default=0)
    proc_date = DateField(null=True, index=True)
    current_page = BigIntegerField(default=0)
    page_size = BigIntegerField(default=0)

    class Meta:
        db_table = 'ivr_action_log'


class OldLoanApplication(ModelBase):
    id = BigIntegerField(primary_key=True, sequence=True)
    bomber_id = BigIntegerField(index=True, null=True)
    user_id = BigIntegerField(index=True, null=True)
    application_id = BigIntegerField()
    status = IntegerField(default=OldLoanStatus.WAITING.value,
                          index=True)
    numbers = TextField(default='')
    start_date = DateTimeField()
    end_date = DateTimeField()
    promised_date = DateTimeField()

    class Meta:
        db_table = 'old_loan_application'


class SummaryBomber(ModelBase):
    id = BigIntegerField(primary_key=True)  # 主键
    bomber_id = BigIntegerField(index=True, null=False)  # 催收员ID
    time = DateTimeField()  # 时间
    cycle = IntegerField()  # 催收员所在cycle
    claimed_cnt = IntegerField(default=0)    # 待催件
    new_case_amount_sum = BigIntegerField(default=0)  # 新件金额
    new_case_cleared_sum = BigIntegerField(default=0)  # 新件还款金额
    new_case_cleard_rate = DecimalField(decimal_places=2, null=True)  # 首催回款率
    case_made_cnt = IntegerField(default=0)  # 拨打件数
    case_made_rate = IntegerField(default=0)  # 触达率
    case_connect_cnt = IntegerField(default=0)  # 接通件数
    case_connect_rate = DecimalField(decimal_places=2, null=True)  # 接通率
    promised_cnt = IntegerField(default=0)  # ptp件数
    promised_amount = BigIntegerField(default=0)  # ptp金额
    cleared_cnt = IntegerField(default=0)  # 回款件数
    cleared_amount = BigIntegerField(default=0)  # 回款金额
    new_case_cnt = IntegerField(default=0)  # 新件数量
    new_case_call_cnt = IntegerField(default=0)  # 新件当日维护件数
    call_cnt = IntegerField(default=0)  # 拨打电话数
    sms_cnt = IntegerField(default=0)  # 发送短信数
    call_connect_cnt = IntegerField(default=0)  # 接通电话数
    calltime_case_sum = IntegerField(default=0)  # 接通件电话总时长
    calltime_case_cnt = IntegerField(default=0)  # 接通件数量
    calltime_case_avg = IntegerField(default=0)  # 件均通话时长
    calltime_no_case_sum = IntegerField(default=0)  # 未接通件等待总时长
    calltime_no_case_cnt = IntegerField(default=0)  # 未接通件个数
    calltime_no_case_avg = IntegerField(default=0)  # 未接通件均时长
    calltime_less5s_cnt = IntegerField(default=0)  # 5s挂断电话数
    calltime_less5s_rate = DecimalField(decimal_places=2, default=0)  # 5s挂断占比
    ptp_today_cnt = IntegerField(default=0)  # 当日ptp到期数
    ptp_today_call_cnt = IntegerField(default=0)  # 当日ptp到期当日维护过件数
    ptp_next_cnt = IntegerField(default=0)  # 次日ptp到期数
    ptp_next_call_cnt = IntegerField(default=0)  # 次日ptp到期当日维护件数
    KP_cleared_cnt = IntegerField(default=0)  # kp回款件数
    KP_today_cnt = IntegerField(default=0)  # 当日处于ptp件数
    KP_cleared_rate = DecimalField(decimal_places=2, null=True)  # 件kp率
    work_ind = IntegerField(default=0)  # 当日是否工作
    calltime_sum = IntegerField(default=0)  # 通话总时长 (接通 + 等待时长)
    work_time_sum = IntegerField(default=0)  # 工作时长
    unfollowed_cnt = IntegerField(default=0)
    unfollowed_call_cnt = IntegerField(default=0)

    class Meta:
        db_table = 'summary_bomber'


class CycleTarget(ModelBase):
    id = BigIntegerField(primary_key=True)
    cycle = IntegerField(null=False)
    target_amount = IntegerField(null=False)
    target_month = DateTimeField(null=False)

    class Meta:
        db_table = 'cycle_target'


class TotalContact(BaseDocument):
    meta = {
        'collection': 'total_contact'
    }

    src_number = mon.StringField(db_field='sn')
    dest_number = mon.StringField(db_field='dn')
    src_name = mon.StringField(db_field='sa')
    dest_name = mon.StringField(db_field='da')
    is_calc = mon.BooleanField(db_field='is_c', default=False)
    source = mon.IntField(db_field='s')
    total_count = mon.IntField(db_field='tc', default=1)
    total_duration = mon.IntField(db_field='td', default=0)

    @classmethod
    def available(cls):
        return [0, 1, 2, 20, 21, 22, 23, 40, 41, 60, 61, 62, 63, 64]

    @classmethod
    def relationship(cls, source):
        if source <= 19:
            return 0
        elif source <= 39:
            return 1
        elif source <= 59:
            return 2
        elif source <= 79:
            return 3
        else:
            return -1

    @classmethod
    def str_source(cls, source):
        return {
            0: 'ktp number',
            1: 'apply info',
            2: 'extra phone',
            3: 'applicant other phone',
            4: 'basic info job_tel',
            5: 'call ec',
            6: 'call frequency',
            7: 'call top5',
            8: 'company',
            9: 'ec',
            10: 'family member',
            11: 'my number',
            12: 'new applicant',
            13: 'online profile phone',
            14: 'sms contacts',
            18: '',
            19: None,

            20: 'ec',
            21: 'contact ec',
            22: 'call ec',
            23: 'call top5',

            24: 'applicant other phone',
            25: 'apply info',
            26: 'basic info job_tel',
            27: 'call frequency',
            28: 'company',
            29: 'extra phone',
            30: 'family member',
            31: 'ktp number',
            32: 'new applicant',
            33: 'online profile phone',
            34: 'sms contacts',
            38: '',
            39: None,

            40: 'basic info job_tel',
            41: 'company',

            42: 'apply info',
            43: 'call ec',
            44: 'call frequency',
            45: 'call top5',
            46: 'ec',
            47: 'extra phone',
            48: 'family member',
            49: 'ktp number',
            50: 'sms contacts',
            58: '',
            59: None,

            60: 'sms contacts',
            61: 'call frequency',
            62: 'online profile phone',
            63: 'my number',
            64: 'other_login',

            65: 'applicant other phone',
            66: 'apply info',
            67: 'basic info job_tel',
            68: 'call ec',
            69: 'call top5',
            70: 'company',
            71: 'contact ec',
            72: 'ec',
            73: 'extra phone',
            74: 'family member',
            75: 'ktp number',
            76: 'new applicant',
            78: '',
            79: None,
        }.get(source)


# 人员变动时分件的操作日志
class DispatchAppLogs(ModelBase):
    id = IntegerField(primary_key=True)
    bomber_id = BigIntegerField()
    need_num = IntegerField()
    # json字符串
    form_ids = TextField(null=True)
    to_ids = TextField(null=True)
    np_ids = TextField(null=True)
    p_ids = TextField(null=True)
    status = IntegerField(default=1)

    class Meta:
        db_table = 'dispatch_app_logs'

# 每天统计，上午下午个统计一次
class SummaryDaily(ModelBase):
    id = IntegerField(primary_key=True)
    bomber_id = BigIntegerField()
    ptp_cnt = IntegerField(default=0, null=True)
    call_cnt = IntegerField(default=0, null=True)
    cycle  = IntegerField(null=True)
    repayment = DecimalField(max_digits=15,decimal_places=3, default=0, null=True)
    summary_date = DateField()

    class Meta:
        db_table = 'summary_daily'



# 逾期子账单
class OverdueBill(ModelBase):
    id = IntegerField(primary_key=True)
    # 催收单的id(applcation.id)
    collection_id = BigIntegerField()
    bill_id = BigIntegerField()
    sub_bill_id = BigIntegerField()
    periods = IntegerField(null=True)
    overdue_days = IntegerField(default=1)
    status = SmallIntegerField(default=0)
    finished_at = DateTimeField(null=True)
    origin_due_at = DateTimeField(null=True)
    amount = DecimalField(max_digits=15, decimal_places=2, null=True)
    amount_net = DecimalField(max_digits=15, decimal_places=2, null=True)
    interest_rate = DecimalField(max_digits=8, decimal_places=6, null=True)
    no_active = IntegerField()
    external_id = BigIntegerField(null=True)

    class Meta:
        db_table = 'overdue_bill'


# 每天对催收单进行记录
class BomberOverdue(ModelBase):
    id = IntegerField(primary_key=True)
    external_id = BigIntegerField()
    collection_id = IntegerField(null=True)
    sub_bill_id = IntegerField(null=True)
    periods = IntegerField(null=True)
    cycle = IntegerField()
    promised_date = DateTimeField(null=True)
    ptp_bomber = IntegerField(null=True)
    follow_up_date = DateTimeField(null=True)
    which_day =  DateField()
    overdue_days = IntegerField()
    no_active = SmallIntegerField(default=0)

    class Meta:
        db_table = 'bomber_overdue'

class CDR(ModelBase):
    id = IntegerField()
    office_type = SmallIntegerField(default=OfficeType.DEFAULT.value)
    callid = CharField(max_length=30)
    start_stamp = DateTimeField()
    profile_start_stamp = DateTimeField()
    answer_stamp = DateTimeField()
    bridge_stamp = DateTimeField()
    end_stamp = DateTimeField()
    billsec = IntegerField(11)
    progressmsec = IntegerField(11)
    progress_mediamsec = IntegerField(11)
    callfrom = CharField(max_length=30)
    callto = CharField(max_length=30)
    ext = CharField(max_length=10)
    type = CharField(max_length=30)
    callroter = CharField(max_length=30)
    hangup_cause = CharField(max_length=30)
    hangup_cause_specific = CharField(max_length=30)
    agent_score = IntegerField(11)
    is_play = IntegerField(1, default=0)
    agent_id = CharField(max_length=30)
    agent_num = CharField(max_length=30)
    agent_name = CharField(max_length=30)
    agent_group = CharField(max_length=30)
    str1 = CharField(max_length=30)
    str2 = CharField(max_length=30)
    str3 = CharField(max_length=30)
    CustomerNumber = CharField(max_length=40)
    agent_score_addnum = CharField(max_length=10)
    agent_score_addtime = DateTimeField()
    remarks = CharField(max_length=255)
    recording = CharField(max_length=150)
    group_id = IntegerField(default=None,null=True)

    class Meta:
        database = db_auto_call
        db_table = 'cdr'
        primary_key = CompositeKey("id", "office_type")

# 员工下p记录表
class BomberPtp(ModelBase):
    id = IntegerField(primary_key=True)
    bomber_id = IntegerField()
    auto_ext = IntegerField(null=True)
    ptp_cnt= IntegerField(default=0,null=True)
    ptp_switch = SmallIntegerField(null=True)
    today_switch = SmallIntegerField(null=True)
    switch = SmallIntegerField(null=True)

    class Meta:
        db_table = 'bomber_ptp'

