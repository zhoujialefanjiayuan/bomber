from bottle import get

from bomber.api import BillService
from bomber.utils import plain_query


@get('/api/v1/applications/<app_id:int>/loan-history')
def loan_history(bomber, application):
    args = plain_query()
    query_args = {
        'user_id': application.user_id,
        'query_type': 2,
        'page': args.page if args and 'page' in args else 0
    }
    return BillService().bill_pages(**query_args)
