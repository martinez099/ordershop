import atexit
import json

from redis import StrictRedis

import requests

import lib.common
from lib.event_store import EventStore


redis = StrictRedis(decode_responses=True, host='redis')
store = EventStore(redis)


def order_created(item):
    lib.common.log_info('ORDER CREATED')
    try:
        msg_data = json.loads(item[1][0][1]['entity'])
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Please transfer € {} with your favourite payment method.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        requests.post('http://msg-service:5000/email', json={
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        lib.common.log_error(e)


def subscribe():
    store.subscribe('order', 'created', order_created)
    lib.common.log_info('subscribed to channels')


def unsubscribe():
    store.unsubscribe('order', 'created', order_created)
    lib.common.log_info('unsubscribed from channels')


subscribe()
atexit.register(unsubscribe)
