from bottle import post, request, abort

from bomber.auth import check_api_user
from bomber.plugins import ip_whitelist_plugin
from bomber.models import IpWhitelist


@post('/api/v1/bomber/update/ip/whitelist', skip=[ip_whitelist_plugin])
def update_ip_whitelist():
    check_api_user()
    forms = request.json
    ip = forms['ip']

    exist_record = IpWhitelist.get_or_none(IpWhitelist.ip == ip)
    if exist_record:
        abort(400, "Existed ip")
    IpWhitelist.create(ip=ip)
    return True
