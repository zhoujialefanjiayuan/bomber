from enum import Enum, unique
from bomber.configuration import config_obj


class BaseEnum(Enum):
    @classmethod
    def choices(cls):
        return [(e.name, e.value) for e in cls]

    @classmethod
    def values(cls):
        return [e.value for e in cls]

    @classmethod
    def keys(cls):
        return [i.name for i in cls]


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
    # AB test中表示人工维护的件
    AB_TEST = 4


@unique
class ApplicationType(BaseEnum):
    # 不分期
    CASH_LOAN = 0
    # 分期
    CASH_LOAN_STAGING = 1


@unique
class AutoListStatus(BaseEnum):
    # 已经逾期 等待催收
    PENDING = 0
    # 已领取 催收中
    PROCESSING = 1
    # 还款完成 或 进入黑名单 或者 promised
    REMOVED = 2
    # 最后一次拨打是语音信箱状态
    MAILBOX = 6


@unique
class RoleStatus(BaseEnum):
    NORMAL, FORBIDDEN = range(2)


@unique
class RoleWeight(BaseEnum):
    MEMBER = 1
    LEADER = 2
    DEPARTMENT = 3

    @classmethod
    def gt_member(cls):
        return RoleWeight.LEADER.value, RoleWeight.DEPARTMENT.value


@unique
class ContactStatus(BaseEnum):
    (UNKNOWN, USEFUL, BUSY, NO_ANSWER, NOT_AVAILABLE, NOT_ACTIVE,
     WRONG_NUMBER, REJECT, OTHER, NO_USE) = range(10)

    @classmethod
    def no_use(cls):
        return [
            cls.NOT_AVAILABLE.value,
            cls.WRONG_NUMBER.value,
            cls.NO_USE.value,
        ]


@unique
class ConnectType(BaseEnum):
    CALL = 0
    SMS = 1
    PAY_METHOD = 2
    AUTO_SMS = 3
    VA_SMS = 4

    @classmethod
    def sms(cls):
        return [cls.SMS.value, cls.VA_SMS.value, cls.PAY_METHOD.value]

    @classmethod
    def va_sms(cls):
        return [cls.VA_SMS.value, cls.PAY_METHOD.value]


@unique
class Relationship(BaseEnum):
    APPLICANT = 0
    COMPANY = 2
    FAMILY = 1
    SUGGESTED = 3

    @classmethod
    def need_classify(cls):
        return [
            cls.APPLICANT.value,
            cls.COMPANY.value,
            cls.FAMILY.value,
        ]


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
class ApprovalStatus(BaseEnum):
    PENDING = 0
    APPROVED = 1
    REJECTED = 2

    @classmethod
    def review_values(cls):
        return [cls.APPROVED.value, cls.REJECTED.value]


@unique
class EscalationType(BaseEnum):
    MANUAL = 0
    AUTOMATIC = 1


@unique
class InboxStatus(BaseEnum):
    UNREAD, READ = range(2)


@unique
class InboxCategory(BaseEnum):
    CLEARED = 'CLEARED'
    REPAID = 'REPAID'
    TRANSFERRED_IN = 'TRANSFERRED_IN'
    APPROVED = 'APPROVED'

    @classmethod
    def default_msg(cls):
        return {
            i.value: 0
            for i in cls
        }


@unique
class BombingResult(BaseEnum):
    NO_PROGRESS = 0
    HAS_PROGRESS = 1


@unique
class ConnectResult(BaseEnum):
    NO_PROGRESS = 0
    HAS_PROGRESS = 1


@unique
class SmsChannel(BaseEnum):
    IMS, RAJA, NUSA, CLAN = range(1, 5)


@unique
class Cycle(BaseEnum):
    C1A, C1B, C2, C3, M3 = range(1, 6)


@unique
class GroupCycle(BaseEnum):
    C1A, C1B, C2, C3 = range(1, 5)


@unique
class AutoCallResult(BaseEnum):
    PTP = 0
    FOLLOW_UP = 1
    NOT_USEFUL = 2
    CONTINUE = 3
    MAIL_BOX = 4


@unique
class ContactsUseful(BaseEnum):
    NONE = 0
    AVAILABLE = 1
    INVALID = 2


@unique
class PartnerStatus(BaseEnum):
    OFFLINE = 0
    NORMAL = 1


@unique
class DisAppStatus(BaseEnum):
    ABNORMAL = 0
    NORMAL = 1


@unique
class AutoCallMessageCycle(BaseEnum):
    NEW_STAGE1 = {"type": "T5_NEW", "scope": (5, 15)}
    OLD_STAGE1 = {"type": "T5_OLD", "scope": (5, 15)}
    STAGE2 = {"type": "T15_ALL", "scope": (15, 25)}
    STAGE3 = {"type": "T25_ALL", "scope": (25, 30)}


@unique
class AppName(BaseEnum):
    if config_obj.get("project", "name") == "hadoop":
        DANACEPAT = 'DanaCepat'
        PINJAMUANG = 'PinjamUang'
        KTAKILAT = 'KtaKilat'
        DANAMALL = 'Danamall'
        HALORUPIA = 'HaloRupiah'

        @classmethod
        def trident_values(cls):
            return [cls.DANACEPAT.value, cls.PINJAMUANG.value]

        @classmethod
        def default(cls):
            return cls.DANACEPAT

    elif config_obj.get("project", "name") == "IKIDana":
        IKIDANA = 'Pinjaman Juanusa'
        IKMODEL = 'Rupiah Tercepat'

        @classmethod
        def trident_values(cls):
            return [cls.IKIDANA.value]

        @classmethod
        def default(cls):
            return cls.IKIDANA

    @classmethod
    def all(cls):
        return [i.value for i in cls]

    @classmethod
    def keys(cls):
        return [i.name for i in cls]

    @classmethod
    def all(cls):
        return [i.value for i in cls]


@unique
class PartnerType(BaseEnum):
    QINWEI = 1
    MBA = 2
    SIM = 3
    QINWEI_C1B = 5
    INDOJAYA_C3 = 7
    BARAMA_C1B = 10


@unique
class SIM(BaseEnum):
    LEADER = 183
    MEMBER1 = 181
    MEMBER2 = 182


@unique
class BomberStatus(BaseEnum):
    AUTO = 0
    AB_TEST = 1
    OUTER = 2
    OUTER_LEADER = 3


@unique
class PhoneStatus(BaseEnum):
    EMPTY_NUMBER = 1
    BUSY_NUMBER = 2
    NO_ANSWER = 3
    CONNECTED = 4
    INACTIVE_OR_NO_SERVICE = 5
    MAIL_BOX = 6
    NO_ANSWERED_HALFWAY = 7
    BE_BLOCKED = 8


@unique
class RealRelationship(BaseEnum):
    SELF = 1
    SPOUSE_OR_FAMILY = 2
    FRIEND = 3
    COLLEAGUE = 4
    NO_RECOGNIZE = 5
    UNWILLING_TO_TELL = 6

    @classmethod
    def user_values(cls):
        return [cls.SELF.value, cls.SPOUSE_OR_FAMILY.value,
                cls.FRIEND.value, cls.COLLEAGUE.value]


@unique
class AdmitLoan(BaseEnum):
    NO = 1
    YES = 2


@unique
class JobOption(BaseEnum):
    YES = 1
    CHANGED_WORK = 2
    UNEMPLOYEE = 3
    UNWILLING_TO_TELL = 4


@unique
class OverdueReason(BaseEnum):
    FORGET_OVERDUE_REASON = 1
    TOO_MUCH_DEBT = 2
    NO_RECEIVED = 3
    PLATFORM_REASON = 4
    OTHER = 5


@unique
class PayWilling(BaseEnum):
    UNWILLING_TO_PAY = 1
    WILLING_TO_PAY = 2
    PTP = 3


@unique
class PayAbility(BaseEnum):
    STABLE_SALARY = 1
    NO_JOB_BUT_HAS_INCOME = 2
    NO_INCOME = 3


@unique
class ConnectApplicant(BaseEnum):
    YES = 1
    NO = 2


@unique
class HasJob(BaseEnum):
    YES = 1
    NO = 2
    UNWILLING_TO_TELL = 3


@unique
class HelpWilling(BaseEnum):
    YES = 1
    NO = 2
    REIMBURSEMENT = 3


@unique
class LastConnectionToApplicant(BaseEnum):
    IN_THIS_WEEK = 1
    IN_THIS_MONTH = 2
    ONE_MONTH_AGO = 3
    NONE = 4


@unique
class CallActionCommit(BaseEnum):
    YES = 1
    NO = 2
    FOLLOW_UP = 3


@unique
class Helpful(BaseEnum):
    NO = 1
    YES = 2


@unique
class SourceOption(BaseEnum):
    APPLY_INFO = 'apply info'  # 本人填写
    EC = 'EC'  # 本人填写
    REPAIR_EC = 'REPAIR EC'
    BASIC_JOB_TEL = 'basic info job_tel'  # 本人填写
    NEW_APPLICANT = 'new applicant'
    NUMBER_FROM_OPERATOR = 'applicant new number from operator'

    @classmethod
    def need_collection(cls):
        return {
            Relationship.APPLICANT.value: [cls.APPLY_INFO.value,
                                           cls.NEW_APPLICANT.value,
                                           cls.NUMBER_FROM_OPERATOR.value],
            Relationship.FAMILY.value: [cls.EC.value, cls.REPAIR_EC.value],
            Relationship.COMPANY.value: [cls.BASIC_JOB_TEL.value],
            Relationship.SUGGESTED.value: []
        }


@unique
class CallActionType(BaseEnum):
    MANUAL = 0
    AUTO = 1
    WHATS_APP = 2


class CallAPIType(BaseEnum):
    # 外呼接口1
    # http://192.168.88.241:8260/index.html?auto=yes&ext=0&phone=123321&pw=yxsd&cmd=08
    TYPE1 = 0

    # 外呼接口2
    # http://149.129.227.153:90/index/ctiagent/callout?auto=yes&ext=1000&phone=123321
    TYPE2 = 1


class OldLoanStatus(BaseEnum):
    WAITING = 0
    PROCESSING = 1
    PAID = 2
    FINISHED = 3

    @classmethod
    def available(cls):
        return [cls.WAITING.value, cls.PROCESSING.value]

    @classmethod
    def no_available(cls):
        return [cls.PAID.value, cls.FINISHED.value]


class SpecialBomber(BaseEnum):
    OLD_APP_BOMBER = 500


class ApplicantSource(BaseEnum):
    NEW_APPLICANT = 'new applicant'


@unique
class ContainOut(BaseEnum):
    NOT_CONTAIN = 0
    CONTAIN = 1


@unique
class FIRSTLOAN(BaseEnum):
    NOT_FIRST_LOAN = 0
    IS_FIRST_LOAN = 1


class OfficeType(BaseEnum):
    DEFAULT = 0
    OLD_OFFICE = 1
    NEW_OFFICE = 2

    @classmethod
    def need_record(cls):
        return [cls.OLD_OFFICE.value, cls.NEW_OFFICE.value]


class BeforeInBomber(BaseEnum):
    """
    $and:
    $or:
    name:
    group:
    """
    DANACEPAT_0_DPD1 = {'$and': [{'venus_v2_score_int': {'$gt': 286}}],
                        'group': [1, 2, 21]}
    PINJAMUANG_0_DPD1 = {'$and': [{'venus_v2_score_int': {'$gt': 286}}],
                         'group': [4, 5, 24]}
    KTAKILAT_0_DPD1 = {'$and': [{'V221': {'$gt': 286}}],
                       'group': [7, 8, 27]}

    KTAKILAT_1_DPD1 = {'$and': [{'V229': {'$gt': 328}}], 'group': [16, 17]}
    PINJAMUANG_1_DPD1 = {'$and': [{'V229': {'$gt': 328}}], 'group': [13, 14]}
    DANACEPAT_1_DPD1 = {'$and': [{'V229': {'$gt': 328}}], 'group': [10, 11]}

    @classmethod
    def dpd1_decision(cls):
        return -1


class RipeInd(BaseEnum):
    NOTRIPE = 0
    RIPE = 1


class PriorityStatus(BaseEnum):
    DEFAULT = 0
    USEFUL = 1  # 有效接通过
    REPAY = 2  # 曾经回款时的最后一次电话
    LAST = 3  # 上次回款时的最后一次电话


class ContactType(BaseEnum):
    # applicant
    A_KTP_NUMBER = 0
    A_APPLY_INFO = 1
    A_EXTRA_PHONE = 2

    # family
    F_EC = 20
    F_CONTACT_EC = 21
    F_CALL_EC = 22
    F_CALL_TOP5 = 23

    # company
    C_BASIC_INFO_JOB_TEL = 40
    C_COMPANY = 41

    # suggested
    S_SMS_CONTACTS = 60
    S_CALL_FREQUENCY = 61
    S_ONLINE_PROFILE_PHONE = 62
    S_MY_NUMBER = 63
    S_OTHER_LOGIN = 64


class BomberCallSwitch(BaseEnum):
    ON = 1  # 允许接的自动外呼
    OFF = 0  # 不予许接自动外呼
