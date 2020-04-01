from bottle import get
from peewee import fn

from bomber.constant_mapping import InboxStatus, InboxCategory
from bomber.models import Inbox


@get('/api/v1/inbox/unread')
def get_inbox_unread(bomber):
    inbox = (Inbox
             .select(Inbox.category, fn.COUNT(Inbox.id).alias('num'))
             .where(Inbox.status == InboxStatus.UNREAD.value,
                    Inbox.receiver == bomber.id)
             .group_by(Inbox.category))
    result = InboxCategory.default_msg()
    for i in inbox:
        result[i.category] += int(i.num)
    return result


@get('/api/v1/inbox/read/<category>')
def inbox_read(bomber, category):
    query = Inbox.update(
        status=InboxStatus.READ.value,
    ).where(
        Inbox.receiver == bomber.id,
        Inbox.category == category.upper()
    )
    num = query.execute()
    return {
        'updated_rows_count': num,
    }
