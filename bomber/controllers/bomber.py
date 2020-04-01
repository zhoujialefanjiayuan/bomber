import logging

from bottle import get

from bomber.constant_mapping import Cycle, RoleWeight, GroupCycle
from bomber.models import Bomber, Role
from bomber.serializers import bomber_role_serializer
from bomber.utils import plain_query
from datetime import date, timedelta


@get('/api/v1/bombers')
def bombers(bomber):
    logging.debug('bomber: ' + str(bomber.id))
    active = date.today() - timedelta(days=7)

    _bombers = (
        Bomber
        .select(Bomber.id, Bomber.name, Role, Bomber.username, Bomber.group_id)
        .join(Role)
        .where(Role.cycle <<
               ([bomber.role.cycle] if bomber.role.cycle else Cycle.values()),
               Bomber.last_active_at >= active)
        .order_by(Bomber.username)
    )

    # 每个cycle的leader只看到自己组内的成员
    if bomber.role.cycle in GroupCycle.values():
        _bombers = _bombers.where(Bomber.group_id == bomber.group_id)
    result = bomber_role_serializer.dump(_bombers, many=True).data
    args = plain_query()
    if 'category' not in args:
        return result
    result = []
    category = args['category']
    logging.info('selected_bomber: ' + str(bomber))
    current_role = (Bomber
                    .select(Bomber, Role)
                    .join(Role)
                    .where(Bomber.name == Role.id)
                    .first())

    if category in ('employee', 'member'):
        if current_role.role.weight in RoleWeight.gt_member():
            result = _bombers.where(Role.weight == RoleWeight.MEMBER.value)

    elif category == 'leader':
        if RoleWeight.DEPARTMENT.value == current_role.role.weight:
            result = _bombers.where(Role.weight == RoleWeight.LEADER.value)

    result = bomber_role_serializer.dump(result, many=True).data
    return result
