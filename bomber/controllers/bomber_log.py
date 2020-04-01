from bottle import get

from bomber.auth import check_api_user
from bomber.constant_mapping import ConnectType
from bomber.plugins import ip_whitelist_plugin
from bomber.models import AutoIVRActions, BombingHistory, AutoCallActions, \
    ConnectHistory
from bomber.serializers import auto_ivr_action_serializer, \
    bombing_history_serializer, auto_call_actions_serializer, \
    connect_history_serializer
from bomber.utils import ChoiceEnum


class BomberLogService(ChoiceEnum):
    AUTO_IVR = 'auto_ivr_actions'
    MANUAL_CALL = 'manual_call'
    AUTO_CALL = 'auto_call'
    SMS = 'sms'


@get('/api/v1/bomber-log/<app_id:int>/<service>', skip=[ip_whitelist_plugin])
def get_bomber_log(app_id, service):
    check_api_user()

    if service not in BomberLogService.values():
        return

    if service == BomberLogService.AUTO_IVR.value:
        actions = (AutoIVRActions.filter(AutoIVRActions.loanid == app_id))
        json_data = auto_ivr_action_serializer.dump(actions, many=True).data
        return json_data
    elif service == BomberLogService.MANUAL_CALL.value:
        history = (BombingHistory
                   .filter(BombingHistory.application_id == app_id))
        return bombing_history_serializer.dump(history, many=True).data
    elif service == BomberLogService.AUTO_CALL.value:
        logs = (AutoCallActions
                .filter(AutoCallActions.application_id == app_id))
        return auto_call_actions_serializer.dump(logs, many=True).data
    elif service == BomberLogService.SMS.value:
        content_type = ConnectType.sms().append(ConnectType.AUTO_SMS.value)
        logs = (ConnectHistory
                .filter(ConnectHistory.application_id == app_id,
                        ConnectHistory.type.in_(content_type)))
        return connect_history_serializer.dump(logs, many=True).data
