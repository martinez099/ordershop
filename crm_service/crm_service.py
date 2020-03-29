import json
import logging
import signal

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import send_message


class CrmService(object):
    """
    CRM Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()

    def start(self):
        logging.info('starting ...')
        self.event_store.subscribe('billing', self.billing_created)
        self.event_store.subscribe('customer', self.customer_created)
        self.event_store.subscribe('customer', self.customer_deleted)
        self.event_store.subscribe('order', self.order_created)
        self.event_store.subscribe('shipping', self.shipping_created)

    def stop(self):
        self.event_store.unsubscribe('billing', self.billing_created)
        self.event_store.unsubscribe('customer', self.customer_created)
        self.event_store.unsubscribe('customer', self.customer_deleted)
        self.event_store.unsubscribe('order', self.order_created)
        self.event_store.unsubscribe('shipping', self.shipping_created)
        logging.info('stopped.')

    @staticmethod
    def customer_created(_item):
        if _item.event_action != 'entity_created':
            return

        customer = json.loads(_item.event_data)
        msg = """Dear {}!

Welcome to Ordershop.

Cheers""".format(customer['name'])

        send_message('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def customer_deleted(_item):
        if _item.event_action != 'entity_deleted':
            return

        customer = json.loads(_item.event_data)
        msg = """Dear {}!

Good bye, hope to see you soon again at Ordershop.

Cheers""".format(customer['name'])

        send_message('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def order_created(_item):
        if _item.event_action != 'entity_created':
            return

        order = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        rsp = send_message('read-model', 'get_entities', {'name': 'customer', 'id': cart['customer_id']})
        customer = rsp['result']
        rsp = send_message('read-model', 'get_entities', {'name': 'product', 'ids': cart['product_ids']})
        products = rsp['result']
        msg = """Dear {}!

Please transfer € {} with your favourite payment method.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        send_message('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def billing_created(_item):
        if _item.event_action != 'entity_created':
            return

        billing = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': billing['order_id']})
        order = rsp['result']
        rsp = send_message('read-model', 'get_entities', {'name': 'customer', 'id': order['customer_id']})
        customer = rsp['result']
        rsp = send_message('read-model', 'get_entities', {'name': 'product', 'ids': order['product_ids']})
        products = rsp['result']
        msg = """Dear {}!

We've just received € {} from you, thank you for your transfer.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        send_message('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def shipping_created(_item):
        if _item.event_action != 'entity_created':
            return

        shipping = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': shipping['order_id']})
        order = rsp['result']
        rsp = send_message('read-model', 'get_entities', {'name': 'customer', 'id': order['customer_id']})
        customer = rsp['result']
        msg = """Dear {}!

We've just shipped order {}. It will be soon delivered to you.

Cheers""".format(customer['name'], order['entity_id'])

        send_message('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

c = CrmService()

signal.signal(signal.SIGINT, lambda n, h: c.stop())
signal.signal(signal.SIGTERM, lambda n, h: c.stop())

c.start()
