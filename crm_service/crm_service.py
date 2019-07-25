import atexit
import json
from functools import partial

from common.utils import log_info, log_error, send_message
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


def customer_created(_mq, _item):
    try:
        msg_data = json.loads(_item.event_entity)
        msg = """Dear {}!

Welcome to Ordershop.

Cheers""".format(msg_data['name'])

        send_message(_mq, 'messaging-service', 'send_email', {
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def customer_deleted(_mq, _item):
    try:
        msg_data = json.loads(_item.event_entity)
        msg = """Dear {}!

Good bye, hope to see you soon again at Ordershop.

Cheers""".format(msg_data['name'])

        send_message(_mq, 'messaging-service', 'send_email', {
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def order_created(_mq, _item):
    try:
        msg_data = json.loads(_item.event_entity)
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Thank you for buying following {} products from Ordershop:
{}

Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

        send_message(_mq, 'messaging-service', 'send_email', {
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def subscribe_to_domain_events(_store, _mq):
    _store.subscribe('customer', 'created', partial(customer_created, _mq))
    _store.subscribe('customer', 'deleted', partial(customer_deleted, _mq))
    _store.subscribe('order', 'created', partial(order_created, _mq))
    log_info('subscribed to domain events')


def unsubscribe_from_domain_events(_store, _mq):
    _store.unsubscribe('customer', 'created', partial(customer_created, _mq))
    _store.unsubscribe('customer', 'deleted', partial(customer_deleted, _mq))
    _store.unsubscribe('order', 'created', order_created)
    log_info('unsubscribed from domain events')


store = EventStore()
mq = MessageQueue()

subscribe_to_domain_events(store, mq)
atexit.register(unsubscribe_from_domain_events, store, mq)
