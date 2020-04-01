from datetime import datetime
from decimal import Decimal
from voluptuous import (
    TrueInvalid,
    Required,
    message,
    Schema,
    Length,
    Coerce,
    All,
    Any,
    Optional)
from voluptuous.validators import truth

from bomber.constant_mapping import (
    LastConnectionToApplicant,
    CallActionCommit,
    RealRelationship,
    ConnectApplicant,
    ContactsUseful,
    CallActionType,
    ApprovalStatus,
    AutoCallResult,
    ContactStatus,
    BombingResult,
    OverdueReason,
    Relationship,
    PhoneStatus,
    SubRelation,
    HelpWilling,
    PayWilling,
    PayAbility,
    AdmitLoan,
    JobOption,
    Helpful,
    HasJob,
    Cycle,
)


def ListCoerce(t):
    return lambda vs: [t(v) for v in vs] if vs else []


def Strip(chars=None):
    return lambda v: v.strip(chars) if v else v


def Cut(end, start=0):
    return lambda v: v[start:end]


def Date(fmt='%Y-%m-%d'):
    return lambda v: datetime.strptime(v, fmt).date()


def Datetime(fmt='%Y-%m-%d %H:%M:%S'):
    return lambda v: datetime.strptime(v, fmt)


@message('Invalid digit', cls=TrueInvalid)
@truth
def IsDigit(s):
    return s.isdigit()


class ValidatorSchema(Schema):
    pass


login_validator = ValidatorSchema({
    Required('username'): Length(min=2, max=20,
                                 msg='username format error'),
    Required('password'): Length(min=6, msg='password format error'),
})

reset_password_validator = ValidatorSchema({
    Required('old_password'): Length(min=6, msg='password format error'),
    Required('new_password'): Length(min=6, msg='password format error'),
})

add_contact_validator = ValidatorSchema({
    Required('name'): Length(min=1, max=128, msg='name format error'),
    Required('number'): Length(min=6, max=64, msg='number format error'),
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    Required('remark'): Coerce(str),
})

contact_call_validator = ValidatorSchema({
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    Required('status'): All(Coerce(int), Any(*ContactStatus.values())),
    'result': Coerce(int),
    Required('remark'): Coerce(str),
})

contact_sms_validator = ValidatorSchema({
    Required('template_id'): Coerce(int),
    Required('text'): Coerce(str),
})

collection_validator = ValidatorSchema({
    'promised_amount': Coerce(Decimal),
    'promised_date': Date(),
    Required('follow_up_date'): Datetime(),
    Required('result'): All(Coerce(int), Any(*BombingResult.values())),
    'remark': Coerce(str),
})

transfer_validator = ValidatorSchema({
    Required('application_list'): Coerce(list),
    Required('transfer_to'): Coerce(int),
    Required('reason'): Coerce(str),
})

review_transfer_validator = ValidatorSchema({
    Required('transfer_list'): Coerce(list),
    Required('status'): All(Coerce(int), Any(*ApprovalStatus.review_values())),
})

reject_validator = ValidatorSchema({
    Required('reason'): Coerce(str),
})

review_reject_validator = ValidatorSchema({
    Required('reject_list'): Coerce(list),
    Required('status'): All(Coerce(int), Any(*ApprovalStatus.review_values())),
})

discount_validator = ValidatorSchema({
    Required('discount_to'): Coerce(Decimal),
    Required('effective_to'): Date(),
    Required('reason'): Coerce(str),
    'periods': Any(Coerce(int), None),
    'overdue_bill_id': Any(Coerce(str), None)
})

review_discount_validator = ValidatorSchema({
    Required('discount_list'): Coerce(list),
    Required('status'): All(Coerce(int), Any(*ApprovalStatus.review_values())),
})

auto_call_phones_validator = ValidatorSchema({
    Required('group_number'): All(Coerce(int), Any(*Cycle.values())),
})

add_auto_call_action_validator = ValidatorSchema({
    Required('contact_id'): Coerce(int),
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    'result': All(Coerce(int), Any(*AutoCallResult.values())),
    Required('useful'): All(Coerce(int), Any(*ContactsUseful.values())),
    'reason': Coerce(int),
    'promised_amount': Coerce(Decimal),
    'promised_date': Date(),
    'follow_up_date': Datetime(),
    'notes': Coerce(str),
    'auto_notes': Coerce(str),
    Optional('call_id'): Length(max=60)
})
add_auto_call_action2_validator = ValidatorSchema({
    Required('contact_id'): Coerce(int),

    Required('useful'): All(Coerce(int), Any(*ContactsUseful.values())),
    'promised_amount': Coerce(Decimal),
    'promised_date': Date(),
    'follow_up_date': Datetime(),
    Optional('call_id'): Length(max=60),

    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    'phone_status': Any(*PhoneStatus.values(), None),
    'real_relationship': Any(*RealRelationship.values(), None),
    Optional('admit_loan'): Any(*AdmitLoan.values(), None),
    Optional('helpful'): Any(*Helpful.values(), None),
    Optional('still_old_job'): Any(*JobOption.values(), None),
    'new_company': Any(str, None),
    Optional('overdue_reason'): Any(*OverdueReason.values(), None),
    'overdue_reason_desc': Any(str, None),
    Optional('pay_willing'): Any(*PayWilling.values(), None),
    Optional('pay_ability'): Any(*PayAbility.values(), None),
    'note': Any(str, None),
    'commit': Any(*CallActionCommit.values(), None),
    Optional('connect_applicant'): Any(*ConnectApplicant.values(), None),
    Optional('has_job'): Any(*HasJob.values(), None),
    Optional('help_willing'): Any(*HelpWilling.values(), None),
    'no_help_reason': Any(str, None),
    'last_connection_to_applicant': Any(
        *LastConnectionToApplicant.values(), None),
    Optional('type'): All(Coerce(int), Any(*CallActionType.values())),
})

contact_call2_validator = ValidatorSchema({
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    'phone_status': Any(*PhoneStatus.values(), None),
    'real_relationship': Any(*RealRelationship.values(), None),
    Optional('admit_loan'): Any(*AdmitLoan.values(), None),
    Optional('still_old_job'): Any(*JobOption.values(), None),
    'new_company': Any(str, None),
    Optional('overdue_reason'): Any(*OverdueReason.values(), None),
    'overdue_reason_desc': Any(str, None),
    Optional('pay_willing'): Any(*PayWilling.values(), None),
    Optional('pay_ability'): Any(*PayAbility.values(), None),
    'note': Any(str, None),
    'commit': Any(*CallActionCommit.values(), None),
    Optional('connect_applicant'): Any(*ConnectApplicant.values(), None),
    Optional('has_job'): Any(*HasJob.values(), None),
    Optional('help_willing'): Any(*HelpWilling.values(), None),
    'no_help_reason': Any(str, None),
    'last_connection_to_applicant': Any(
        *LastConnectionToApplicant.values(), None),
}, extra=True)

contact_call2_validator = ValidatorSchema({
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values())),
    'phone_status': Any(*PhoneStatus.values(), None),
    'real_relationship': Any(*RealRelationship.values(), None),
    Optional('admit_loan'): Any(*AdmitLoan.values(), None),
    Optional('still_old_job'): Any(*JobOption.values(), None),
    'new_company': Any(str, None),
    Optional('overdue_reason'): Any(*OverdueReason.values(), None),
    'overdue_reason_desc': Any(str, None),
    Optional('pay_willing'): Any(*PayWilling.values(), None),
    Optional('pay_ability'): Any(*PayAbility.values(), None),
    'note': Any(str, None),
    'commit': Any(*CallActionCommit.values(), None),
    Optional('connect_applicant'): Any(*ConnectApplicant.values(), None),
    Optional('has_job'): Any(*HasJob.values(), None),
    Optional('type'): All(Coerce(int), Any(*CallActionType.values())),
    Optional('help_willing'): Any(*HelpWilling.values(), None),
    'no_help_reason': Any(str, None),
    'last_connection_to_applicant': Any(
        *LastConnectionToApplicant.values(), None),
    Optional('helpful'): Any(*Helpful.values(), None),
}, extra=True)

add_auto_call_popup_action_validator = ValidatorSchema({
    Required('contact_id'): Coerce(int),
    Required('relationship'): All(Coerce(int), Any(*Relationship.values())),
    Required('sub_relation'): All(Coerce(int), Any(*SubRelation.values()))
})

auto_call_no_answer_validator = ValidatorSchema({
    Required('customer_number'): Coerce(int),
})

report_summary_validator = ValidatorSchema({
    Required('category'): Coerce(str),
    Required('start_date'): Date(),
    Required('end_date'): Date()
})

cs_ptp_validator = ValidatorSchema({
    Required('promised_date'): Date(),
})

report_collections_date_validator = ValidatorSchema({
    Optional('start_date'): Coerce(str),
    Optional('end_date'): Coerce(str)
})

new_bomber_validator = ValidatorSchema({
    Required('id'): Coerce(int),
    Required('username'): Length(min=4, msg='Username too short'),
    Required('password'): Length(min=6, msg='Password too short'),
    Required('role'): Length(min=1, msg='Invalid Role'),
    Required('type'): Coerce(int),
    'ext': Any(Coerce(int), None),
    'email': Any(str, None),
    'phone': Any(str, None),
    'partner_id': Any(Coerce(int), None),
    'status': Any(Coerce(int), None),
    'instalment': Any(Coerce(int), None),
    'name': Any(str, None),
    'group_id': Any(Coerce(int), None),
    'auto_ext': Any(Coerce(int), None)
})

edit_bomber_validator = ValidatorSchema({
    'name': Length(min=4, msg='Username too short'),
    'password': Length(min=6, msg='Password too short'),
    'type': Any(Coerce(int), None),
    'ext': Any(str, None),
    'email': Any(str, None),
    'phone': Any(str, None),
    'auto_ext': Any(str, None)
})

new_role_validator = ValidatorSchema({
    Required('name'): Length(min=2, msg='name too short'),
    Required('cycle'): Coerce(int),
    'weight': Any(Coerce(int), None),
    'status': Any(Coerce(int), None)
})
