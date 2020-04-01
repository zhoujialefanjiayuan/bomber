import logging

from bottle import post, request
from peewee import IntegrityError

from bomber.plugins import ip_whitelist_plugin
from bomber.models import (
    db_auto_call,
    AgentStatus,
    NewCdr,
    CDR,
)
from bomber.constant_mapping import OfficeType


def sync_data(data_list, model, column_list):
    data_list = list(map(lambda x: dict(zip(column_list, x)), data_list))
    length = len(data_list)
    failed_list = []
    for i in range(0, length, 50):
        try:
            model.insert_many(data_list[i:i + 50]).execute()
            db_auto_call.commit()
        except Exception:
            db_auto_call.rollback()
            for item in data_list[i:i + 50]:
                try:
                    model.insert(item).execute()
                    db_auto_call.commit()
                except IntegrityError:
                    pass
                except Exception as err:
                    logging.error('When insert data has some error: %s' % err)
                    db_auto_call.rollback()
                    failed_list.append(item['id'])
    src_id = [entry['id'] for entry in data_list]
    return list(set(src_id) - set(failed_list))


@post('/api/v1/mysql/data/sync', skip=[ip_whitelist_plugin])
def data_to_sync():
    model_dict = {
        'cc_newcdr': NewCdr,
        'cc_agent_status': AgentStatus,
        'cc_cdr': CDR,
    }
    data = request.json

    data_key = data.get('data_key', [])
    office_type = data.get('type')
    data_list = data.get('data_list', [])
    model = data.get('table')

    if not data_key:
        return []
    data_key.append('office_type')

    if office_type in OfficeType.need_record():
        data_list = list(map(lambda item: item + [office_type], data_list))
    else:
        return []

    model = model_dict.get(model)
    if not model:
        return []
    return sync_data(data_list, model, data_key)
