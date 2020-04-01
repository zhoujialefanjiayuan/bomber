import json
import traceback

import logging
import time

import boto3
from bottle import default_app

from bomber.app import init_app
from bomber.db import db
from bomber.models import WorkerLog, WorkerResult
from bomber.utils import diff_end_time
from bomber.worker import actions

app = default_app()


def parse_message(raw_message):
    receipt_handle = raw_message['ReceiptHandle']

    body_json = json.loads(raw_message['Body'])

    if body_json.get('Type') == 'Notification':
        msg_id = body_json['MessageId']
        message = body_json['Message']
    else:
        msg_id = raw_message['MessageId']
        message = body_json

    return receipt_handle, msg_id, message


def loop():
    client = boto3.client('sqs')

    while True:
        query_url = app.config['aws.sqs.queue_url']
        wait_time_seconds = int(app.config['aws.sqs.wait_time_seconds'])

        resp = client.receive_message(
            QueueUrl=query_url,
            WaitTimeSeconds=wait_time_seconds)
        for raw_message in resp.get('Messages', []):
            try:
                receipt_handle, msg_id, message = parse_message(raw_message)
            except json.decoder.JSONDecodeError:
                receipt_handle = raw_message['ReceiptHandle']
                delete_message(client, query_url, receipt_handle)
                continue

            logging.debug('aws sqs message receive: %s %s', msg_id, message)

            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except json.decoder.JSONDecodeError:
                    delete_message(client, query_url, receipt_handle)
                    continue

            if 'action' in message:
                message_action = message.get('action', '').lower()
                message_payload = message.get('payload', {})

                action_funcs = actions.get(message_action, [])

                for action_func in action_funcs:
                    start = time.time()
                    logging.info('aws sqs message receive: %s %s', msg_id, message)
                    worker = worker_create(msg_id, message_action,
                                           message_payload, receipt_handle)
                    try:
                        action_func(message_payload, msg_id)
                        logging.info('message process done: %s func: %s',
                                     msg_id, action_func.__name__)
                        worker_done(worker, start)
                    except:  # noqa
                        logging.exception('message process failed: %s func: %s',
                                          msg_id, action_func.__name__)
                        worker_failed(worker, start,
                                      traceback.format_exc(), receipt_handle)
                        break
                    finally:
                        if not db.is_closed():
                            db.close()
                else:
                    delete_message(client, query_url, receipt_handle)


def delete_message(client, query_url, receipt_handle):
    delete_resp = client.delete_message(
        QueueUrl=query_url,
        ReceiptHandle=receipt_handle,
    )
    logging.info('delete sqs msg %s resp: %s', receipt_handle, delete_resp)


def worker_create(msg_id, message_action, message_payload, receipt_handle):
    worker = WorkerLog.create(message_id=msg_id,
                              action=message_action,
                              payload=str(message_payload),
                              receipt_handle=receipt_handle)
    return worker


def worker_failed(worker, start, error_traceback=None, receipt_handle=None):
    worker.time_spent = diff_end_time(start)
    worker.result = WorkerResult.FAILED.value
    worker.traceback = error_traceback
    worker.receipt_handle = receipt_handle
    worker.save()


def worker_done(worker, start):
    worker.time_spent = diff_end_time(start)
    worker.result = WorkerResult.DONE.value
    worker.save()


if __name__ == '__main__':
    init_app()
    logging.warning('worker start up success. version {}'.format('2.10.15'))
    loop()
