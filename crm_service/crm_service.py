import json
import logging
import signal
import datetime

from redis import StrictRedis

import requests

from ordershop.lib.event_store import EventStore


redis = StrictRedis(decode_responses=True, host='redis')
store = EventStore(redis)
logger = logging.getLogger()


def log_info(msg):
    logger.info('[{}] INFO in crm_service: {}'.format(datetime.time(), msg))


def log_error(msg):
    logger.info('[{}] ERROR in crm_service: {}'.format(datetime.time(), msg))


def customer_created(message):
    try:
        msg_data = json.loads(message['data'])
        msg = """Dear {}!

Welcome to Ordershop.

Cheers""".format(msg_data['name'])

        requests.post('http://msg-service:5000/email', json={
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(str(e))


def customer_deleted(message):
    try:
        msg_data = json.loads(message['data'])
        msg = """Dear {}!

Good bye, hope to see you soon again at Ordershop.

Cheers""".format(msg_data['name'])

        requests.post('http://msg-service:5000/email', json={
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(str(e))


def order_created(message):
    try:
        msg_data = json.loads(message['data'])

        customer = store.find_one('CUSTOMER', msg_data['customer_id'])
        products = [store.find_one('PRODUCT', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Thank you for buying following {} products from Ordershop:
{}

Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

        requests.post('http://msg-service:5000/email', json={
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(str(e))


def subscribe():
    store.subscribe('CUSTOMER', 'CREATED', customer_created)
    store.subscribe('CUSTOMER', 'DELETED', customer_deleted)
    store.subscribe('ORDER', 'CREATED', order_created)
    log_info('subscribed to channels')


def exit_gracefully(signum, frame):
    store.unsubscribe('CUSTOMER', 'CREATED')
    store.unsubscribe('CUSTOMER', 'DELETED')
    store.unsubscribe('ORDER', 'CREATED')
    log_info('unsubscribed from channels')


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

subscribe()
