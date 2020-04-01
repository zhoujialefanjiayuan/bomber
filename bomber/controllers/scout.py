from bottle import get, abort

from bomber.api import Scout
from bomber.api import AuditService
from bomber.plugins import packing_plugin


@get('/api/v1/applications/<app_id:int>/questions/<cat>',
     skip=[packing_plugin])
def get_phone_verify(bomber, application, cat):

    resp = AuditService().bomber_get_audit(cat=cat,
                                           application_id=application.external_id)
    return resp
