import atexit
import json

from common.utils import log_info, log_error, do_send
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue

store = EventStore()
mq = MessageQueue()


def customer_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        msg = """Dear {}!

Welcome to Ordershop.

Cheers""".format(msg_data['name'])

        do_send('messaging-service', 'send-email', {
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def customer_deleted(item):
    try:
        msg_data = json.loads(item.event_entity)
        msg = """Dear {}!

Good bye, hope to see you soon again at Ordershop.

Cheers""".format(msg_data['name'])

        do_send('messaging-service', 'send-email', {
            "to": msg_data['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def order_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Thank you for buying following {} products from Ordershop:
{}

Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

        do_send('messaging-service', 'send-email', {
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
