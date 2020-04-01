import json

from marshmallow import Schema
from marshmallow import fields

from bomber.constant_mapping import Relationship


class JsonListField(fields.Field):
    def _serialize(self, value, attr, obj):
        return json.loads(value or '[]')


class JsonDictField(fields.Field):
    def _serialize(self, value, attr, obj):
        return json.loads(value or '{}')


class BaseSchema(Schema):
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    updated_at = fields.DateTime('%Y-%m-%d %H:%M:%S')


class BomberSerializer(Schema):
    id = fields.Int()
    name = fields.Str()
    username = fields.Str()


class ApplicationSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    user_id = fields.Int(as_string=True)
    user_mobile_no = fields.Str()
    user_name = fields.Str()
    app = fields.Str()
    device_no = fields.Str()
    contact = JsonListField()
    apply_at = fields.DateTime('%Y-%m-%d %H:%M:%S')

    id_ektp = fields.Str()
    birth_date = fields.Str()
    gender = fields.Str()

    profile_province = fields.Str()
    profile_city = fields.Str()
    profile_district = fields.Str()
    profile_residence_time = fields.Int()
    profile_residence_type = fields.Int()
    profile_address = fields.Str()
    profile_education = fields.Int()
    profile_college = fields.Str()

    amount = fields.Decimal(as_string=True)
    interest = fields.Decimal(as_string=True)
    amount_net = fields.Decimal(as_string=True)
    interest_rate = fields.Decimal(as_string=True)
    late_fee_rate = fields.Decimal(as_string=True)
    late_fee = fields.Decimal(as_string=True)
    late_fee_initial = fields.Decimal(as_string=True)
    term = fields.Int()
    origin_due_at = fields.DateTime(format="%Y-%m-%d")
    due_at = fields.DateTime(format="%Y-%m-%d %H:%M:%S")
    repay_at = fields.DateTime(format="%Y-%m-%d %H:%M:%S")
    overdue_days = fields.Int()
    repaid = fields.Decimal(as_string=True)
    principal_paid = fields.Decimal(as_string=True)
    late_fee_paid = fields.Decimal(as_string=True)
    unpaid = fields.Decimal(as_string=True)

    job_name = fields.Str()
    job_tel = fields.Str()
    job_bpjs = fields.Str()
    job_user_email = fields.Str()
    job_type = fields.Int()
    job_industry = fields.Int()
    job_department = fields.Int()
    job_province = fields.Str()
    job_city = fields.Str()
    job_district = fields.Str()
    job_address = fields.Str()

    is_rejected = fields.Bool()
    cycle = fields.Int()
    loan_success_times = fields.Int()
    latest_bomber = fields.Nested(BomberSerializer)
    last_bomber = fields.Nested(BomberSerializer)
    status = fields.Int()
    latest_bombing_time = fields.DateTime('%Y-%m-%d %H:%M:%S')
    arrived_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    claimed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    promised_amount = fields.Decimal(as_string=True)
    promised_date = fields.DateTime('%Y-%m-%d')
    follow_up_date = fields.DateTime('%Y-%m-%d')
    finished_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    called_times = fields.Int(as_string=True)

    # 新的催收界面添加的部分数据
    entry_at = fields.DateTime('%Y-%m-%d')
    expected_out_time = fields.DateTime('%Y-%m-%d')
    last_bomber_time = fields.DateTime('%Y-%m-%d %H:%M:%S')

    # 新增的分期字段
    type = fields.Int()
    external_id = fields.Int(as_string=True)
    bill_id = fields.Int(as_string=True)


class OverdueBillSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    collection_id = fields.Int(as_string=True)
    bill_id = fields.Int(as_string=True)
    sub_bill_id = fields.Int(as_string=True)
    periods = fields.Int()
    overdue_days = fields.Int()
    status = fields.Int()
    finished_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    origin_due_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    amount = fields.Decimal(as_string=True)
    amount_net = fields.Decimal(as_string=True)
    no_active = fields.Int()
    external_id = fields.Int(as_string=True)

class ApplicationBomberSerializer(ApplicationSerializer):
    bomber = fields.Nested(BomberSerializer)


class ApplicationMarkSerializer(ApplicationSerializer):
    cca = fields.Int()


class RoleSerializer(Schema):
    name = fields.Str()
    permission = JsonListField()
    cycle = fields.Int()

class RoleSystemSerializer(RoleSerializer):
    id = fields.Int(as_string=True)
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')

class PanrtnerSerializer(Schema):
    id = fields.Int(as_string=True)
    name = fields.Str()

class BombersListSerializer(Schema):
    id = fields.Int(as_string=True)
    name = fields.Str()
    role = fields.Nested(RoleSystemSerializer)
    phone = fields.Str()
    email = fields.Str()
    ext = fields.Str()
    auto_ext = fields.Str()
    type = fields.Int()
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    last_active_at = fields.DateTime('%Y-%m-%d')
    partner_id = fields.Str()


class BomberRoleSerializer(Schema):
    id = fields.Int()
    name = fields.Str()
    username = fields.Str()
    ext = fields.Int()
    employee_id = fields.Str()
    role = fields.Nested(RoleSerializer)
    type = fields.Int()
    group_id = fields.Int()


class ContactSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    name = fields.Str()
    number = fields.Str()
    relationship = fields.Int()
    sub_relation = fields.Int()
    latest_status = fields.Int()
    latest_remark = fields.Str()
    source = fields.Str()
    available = fields.Method('is_ec_or_applicant')

    @staticmethod
    def is_ec_or_applicant(obj):
        return (obj.relationship == Relationship.APPLICANT.value or
                obj.source == 'ec' or obj.source == 'repair ec')


class TemplateSerializer(BaseSchema):
    id = fields.Int()
    type = fields.Int()
    name = fields.Str()
    text = fields.Str()


class ConnectHistorySerializer(BaseSchema):
    name = fields.Str()
    number = fields.Str()
    relationship = fields.Int()
    sub_relation = fields.Int()
    status = fields.Int()
    result = fields.Int()
    type = fields.Int()
    template = fields.Nested(TemplateSerializer)
    operator = fields.Nested(BomberSerializer)
    remark = fields.Str()
    record = fields.Str()


class CallHistorySerializer(BaseSchema):
    name = fields.Str()
    application_id = fields.Int(as_string=True)
    relationship = fields.Int()
    sub_relation = fields.Int()
    phone_status = fields.Int()
    pay_willing = fields.Int()
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    remark = fields.Str()
    operator = fields.Nested(BomberSerializer)
    type = fields.Int()
    promised_date = fields.DateTime('%Y-%m-%d')
    promised_amount = fields.Decimal(as_string=True)
    cycle = fields.Int()
    number = fields.Str()
    help_willing = fields.Int()
    note = fields.Str()
    real_relationship = fields.Int()


class BombingHistorySerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    user_id = fields.Int(as_string=True)
    ektp = fields.Str()
    cycle = fields.Int()
    bomber = fields.Nested(BomberSerializer)
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    follow_up_date = fields.DateTime('%Y-%m-%d %H:%M:%S')
    promised_date = fields.Date('%Y-%m-%d')
    promised_amount = fields.Decimal(as_string=True)
    result = fields.Int()
    remark = fields.Str()


class EscalationSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    type = fields.Int()
    status = fields.Int()
    current_cycle = fields.Int()
    escalation_to = fields.Int()
    reason = fields.Str()
    comment = fields.Str()


class TransferSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    status = fields.Int()
    current_bomber = fields.Nested(BomberSerializer)
    transfer_to = fields.Nested(BomberSerializer)
    reason = fields.Str()
    comment = fields.Str()


class TransferApplicationSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application = fields.Nested(ApplicationSerializer)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    status = fields.Int()
    current_bomber = fields.Nested(BomberSerializer)
    transfer_to = fields.Nested(BomberSerializer)
    reason = fields.Str()
    comment = fields.Str()


class RejectListSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    status = fields.Int()
    reason = fields.Str()
    comment = fields.Str()


class RejectApplicationListSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application = fields.Nested(ApplicationSerializer)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    status = fields.Int()
    reason = fields.Str()
    comment = fields.Str()


class DiscountSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    discount_to = fields.Decimal(as_string=True)
    effective_to = fields.DateTime('%Y-%m-%d')
    status = fields.Int()
    reason = fields.Str()
    comment = fields.Str()
    overdue_bill_id = fields.Int()
    periods = fields.Int()


class DiscountApplicationSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application = fields.Nested(ApplicationSerializer)
    operator = fields.Nested(BomberSerializer)
    reviewer = fields.Nested(BomberSerializer)
    reviewed_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    discount_to = fields.Decimal(as_string=True)
    effective_to = fields.DateTime('%Y-%m-%d')
    status = fields.Int()
    reason = fields.Str()
    comment = fields.Str()
    overdue_bill_id = fields.Int()
    periods = fields.Int()


class CallLogSerializer(BaseSchema):
    call_id = fields.Int()
    time_start = fields.DateTime('%Y-%m-%d %H:%M:%S')
    time_end = fields.DateTime('%Y-%m-%d %H:%M:%S')
    talk_time = fields.Int()
    cpn = fields.Str()
    cdpn = fields.Str()
    duration = fields.Int()
    recording = fields.Str()
    gh = fields.Str()
    xm = fields.Str()
    user_id = fields.Str()
    application_id = fields.Str()


class AutoCallActionsSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    cycle = fields.Int()
    name = fields.Str()
    number = fields.Str()
    relationship = fields.Int()
    sub_relation = fields.Int()
    bomber = fields.Nested(BomberSerializer)
    result = fields.Int()
    reason = fields.Int()
    follow_up_date = fields.DateTime('%Y-%m-%d %H:%M:%S')
    promised_date = fields.Date('%Y-%m-%d')
    promised_amount = fields.Decimal(as_string=True)
    notes = fields.Str()
    auto_notes = fields.Str()
    call_id = fields.Str()


class CallActionsSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    application_id = fields.Int(as_string=True)
    cycle = fields.Int()
    relationship = fields.Int()
    sub_relation = fields.Int()
    bomber = fields.Nested(BomberSerializer)
    phone_status = fields.Int()
    real_relationship = fields.Int()
    call_type = fields.Int()
    admit_loan = fields.Int()
    changed_job = fields.Int()
    new_company = fields.Str()
    overdue_reason = fields.Int()
    overdue_reason_desc = fields.Str()
    pay_willing = fields.Int()
    pay_ability = fields.Int()
    note = fields.Str()
    commit = fields.Int()
    connect_applicant = fields.Int()
    has_job = fields.Int()
    help_willing = fields.Int()
    no_help_reason = fields.Str()
    last_connection_to_applicant = fields.Int()
    call_id = fields.Str()
    still_old_job = fields.Int()
    helpful = fields.Int()


class Contact2Serializer(BaseSchema):
    # add CallAction
    id = fields.Int(as_string=True)
    call_action = fields.Nested(CallActionsSerializer)
    name = fields.Str()
    number = fields.Str()
    relationship = fields.Int()
    sub_relation = fields.Int()
    latest_status = fields.Int()
    latest_remark = fields.Str()
    available = fields.Method('is_ec_or_applicant')

    @staticmethod
    def is_ec_or_applicant(obj):
        return (obj.relationship == Relationship.APPLICANT.value or
                obj.source == 'ec' or obj.source == 'repair ec')


class RepaymentLogSerializer(BaseSchema):
    cycle = fields.Int()
    # application = fields.Nested(ApplicationSerializer)
    principal_paid = fields.Decimal(as_string=True)
    late_fee_paid = fields.Decimal(as_string=True)
    current_bomber = fields.Nested(BomberSerializer)
    finished_at = fields.DateTime('%Y-%m-%d %H:%M:%S')
    periods = fields.Int()


class DispatchAppSerializer(BaseSchema):
    application = fields.Nested(ApplicationSerializer)


class AutoIVRActionSerializer(BaseSchema):
    id = fields.Int(as_string=True)
    callstate = fields.Int()
    group = fields.Int()
    customer_number = fields.Int(as_string=True)
    loanid = fields.Int(as_string=True)
    callid = fields.Str()
    timestart = fields.DateTime('%Y-%m-%d %H:%M:%S')
    callfrom = fields.Str()
    callto = fields.Str()
    callduraction = fields.Int()
    talkduraction = fields.Int()
    dstrunkname = fields.Str()


application_serializer = ApplicationSerializer(strict=True)
application_mark_serializer = ApplicationMarkSerializer(strict=True)
application_caller_serializer = ApplicationBomberSerializer(strict=True)
bomber_role_serializer = BomberRoleSerializer(strict=True)
contact_serializer = ContactSerializer(strict=True)
contact2_serializer = Contact2Serializer(strict=True)
connect_history_serializer = ConnectHistorySerializer(strict=True)
call_history_serializer = CallHistorySerializer(strict=True)
template_serializer = TemplateSerializer(strict=True)
bombing_history_serializer = BombingHistorySerializer(strict=True)
escalation_serializer = EscalationSerializer(strict=True)
transfer_serializer = TransferSerializer(strict=True)
transfer_application_serializer = TransferApplicationSerializer(strict=True)
reject_list_serializer = RejectListSerializer(strict=True)
reject_application_serializer = RejectApplicationListSerializer(strict=True)
discount_serializer = DiscountSerializer(strict=True)
discount_application_serializer = DiscountApplicationSerializer(strict=True)
call_log_serializer = CallLogSerializer(strict=True)
auto_call_actions_serializer = AutoCallActionsSerializer(strict=True)
call_actions_serializer = CallActionsSerializer(strict=True)
repayment_log_serializer = RepaymentLogSerializer(strict=True)
dispatch_app_serializer = DispatchAppSerializer(strict=True)
auto_ivr_action_serializer = AutoIVRActionSerializer(strict=True)
bombers_list_serializer = BombersListSerializer(strict=True)
role_list_serializer = RoleSystemSerializer(strict=True)
partner_list_serializer = PanrtnerSerializer(strict=True)
