import atexit
import json

import requests

from common.utils import log_info, log_error
from lib.event_store import EventStore


store = EventStore()


def customer_created(item):
    try:
        msg_data = json.loads(item[1][0][1]['entity'])
        msg = """Dear {}!

Welcome to Ordershop.

Cheers""".format(msg_data['name'])

        requests.post('http://msg-service:5000/email', json={
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def customer_deleted(item):
    try:
        msg_data = json.loads(item[1][0][1]['entity'])
        msg = """Dear {}!

Good bye, hope to see you soon again at Ordershop.

Cheers""".format(msg_data['name'])

        requests.post('http://msg-service:5000/email', json={
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def order_created(item):
    try:
        msg_data = json.loads(item[1][0][1]['entity'])
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Thank you for buying following {} products from Ordershop:
{}

Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

        requests.post('http://msg-service:5000/email', json={
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def subscribe_to_domain_events():
    store.subscribe('customer', 'created', customer_created)
    store.subscribe('customer', 'deleted', customer_deleted)
    store.subscribe('order', 'created', order_created)
    log_info('subscribed to domain events')


def unsubscribe_from_domain_events():
    store.unsubscribe('customer', 'created', customer_created)
    store.unsubscribe('customer', 'deleted', customer_deleted)
    store.unsubscribe('order', 'created', order_created)
    log_info('unsubscribed from domain events')


subscribe_to_domain_events()
atexit.register(unsubscribe_from_domain_events)
