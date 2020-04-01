from peewee import ForeignKeyField, CompositeKey
from bomber.db import readonly_db, readonly_db_auto_call
from bomber.models import (
    Role,
    NewCdr,
    Bomber,
    Contact,
    Partner,
    Template,
    BomberPtp,
    RAgentType,
    DispatchApp,
    Application,
    CallActions,
    OverdueBill,
    RepaymentLog,
    BombingHistory,
    ConnectHistory,
    AutoCallActions,
    OldLoanApplication,
    DispatchAppHistory,
)


class ApplicationR(Application):
    latest_bomber = ForeignKeyField(Bomber,
                                    related_name='application_r', null=True)
    last_bomber = ForeignKeyField(Bomber, null=True)

    class Meta:
        database = readonly_db
        db_table = 'application'


class AutoCallActionsR(AutoCallActions):
    application = ForeignKeyField(Application, 'auto_call_actions_r')
    bomber = ForeignKeyField(Bomber, 'auto_call_actions_r')

    class Meta:
        database = readonly_db
        db_table = 'auto_call_actions'


class ContactR(Contact):
    class Meta:
        database = readonly_db
        db_table = 'contact_copy'


class RAgentTypeR(RAgentType):
    class Meta:
        database = readonly_db_auto_call
        db_table = 'r_agent_type'


class BomberR(Bomber):
    role = ForeignKeyField(Role, related_name='bomber_r')
    partner = ForeignKeyField(Partner, related_name='bomber_r')

    class Meta:
        database = readonly_db
        db_table = 'bomber'


class CallActionsR(CallActions):
    application = ForeignKeyField(Application, 'call_actions_r')

    class Meta:
        database = readonly_db
        db_table = 'call_actions'


class DispatchAppHistoryR(DispatchAppHistory):
    application = ForeignKeyField(Application, 'dispatch_app_history_r')

    class Meta:
        database = readonly_db
        db_table = 'dispatch_app_history'


class ConnectHistoryR(ConnectHistory):
    application = ForeignKeyField(Application, 'connect_history_r')
    operator = ForeignKeyField(Bomber, 'connect_history_r')
    template = ForeignKeyField(Template, 'connect_history_r', null=True)

    class Meta:
        database = readonly_db
        db_table = 'connect_history'


class BombingHistoryR(BombingHistory):
    application = ForeignKeyField(Application, 'bombed_histories_r')
    bomber = ForeignKeyField(Bomber, 'bombed_histories_r')

    class Meta:
        database = readonly_db
        db_table = 'bombing_history'


class RepaymentLogR(RepaymentLog):
    application = ForeignKeyField(Application, 'repayment_log_r')
    current_bomber = ForeignKeyField(Bomber, 'repayment_log_r')

    class Meta:
        database = readonly_db
        db_table = 'repayment_log'


class NewCdrR(NewCdr):

    class Meta:
        database = readonly_db_auto_call
        db_table = 'newcdr'
        primary_key = CompositeKey("id", "office_type")


class DispatchAppR(DispatchApp):
    application = ForeignKeyField(Application, 'dispatch_app_r')
    partner = ForeignKeyField(Partner, 'dispatch_app_r')
    bomber = ForeignKeyField(Bomber, 'dispatch_app_r')

    class Meta:
        database = readonly_db
        db_table = 'dispatch_app'


class OldLoanApplicationR(OldLoanApplication):

    class Meta:
        database = readonly_db
        db_table = 'old_loan_application'

class OverdueBillR(OverdueBill):
    class Meta:
        database = readonly_db
        db_table = 'overdue_bill'

class BomberPtpR(BomberPtp):
    class Meta:
        database = readonly_db
        db_table = 'bomber_ptp'