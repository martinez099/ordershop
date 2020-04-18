import json
import logging
import signal

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import send_message, send_message_async


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
        self.event_store.subscribe('order', self.order_updated)
        self.event_store.subscribe('shipping', self.shipping_created)

    def stop(self):
        self.event_store.unsubscribe('billing', self.billing_created)
        self.event_store.unsubscribe('customer', self.customer_created)
        self.event_store.unsubscribe('customer', self.customer_deleted)
        self.event_store.unsubscribe('order', self.order_updated)
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

        send_message_async('mail-service', 'send', {
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

        send_message_async('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def order_updated(_item):
        if _item.event_action != 'entity_updated':
            return

        order = json.loads(_item.event_data)
        if order['status'] != 'IN_STOCK':
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        if not cart:
            logging.error('could not find cart {} for order {}'.format(order['cart_id'], order['entity_id']))
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'customer', 'id': cart['customer_id']})
        customer = rsp['result']
        if not customer:
            logging.error('could not find customer {} for cart {}'.format(cart['customer_id'], cart['entity_id']))
            return

        rsp = send_message('read-model', 'get_entities', {'name': 'product', 'ids': cart['product_ids']})
        products = rsp['result']
        if not all(products) and not len(products) == len(cart['product_ids']):
            logging.error('could not find all products for cart {}'.format(cart['entity_id']))
            return

        msg = """Dear {}!

Please transfer € {} with your favourite payment method.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        send_message_async('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def billing_created(_item):
        if _item.event_action != 'entity_created':
            return

        billing = json.loads(_item.event_data)

        rsp = send_message('read-model', 'get_entity', {'name': 'order', 'id': billing['order_id']})
        order = rsp['result']
        if not order:
            logging.error('could not find order {} for billing {}'.format(billing['order_id'], billing['entity_id']))
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        if not cart:
            logging.error('could not find cart {} for order {}'.format(order['cart_id'], order['entity_id']))
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'customer', 'id': cart['customer_id']})
        customer = rsp['result']
        if not customer:
            logging.error('could not find customer {} for cart {}'.format(cart['customer_id'], cart['entity_id']))
            return

        msg = """Dear {}!

We've just received € {} from you, thank you for your transfer.

Cheers""".format(customer['name'], billing['amount'])

        send_message_async('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })

    @staticmethod
    def shipping_created(_item):
        if _item.event_action != 'entity_created':
            return

        shipping = json.loads(_item.event_data)

        rsp = send_message('read-model', 'get_entity', {'name': 'order', 'id': shipping['order_id']})
        order = rsp['result']
        if not order:
            logging.error('could not find order {} for shipping {}'.format(shipping['order_id'], shipping['entity_id']))
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        if not cart:
            logging.error('could not find cart {} for order {}'.format(order['cart_id'], order['entity_id']))
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'customer', 'id': cart['customer_id']})
        customer = rsp['result']
        if not customer:
            logging.error('could not find customer {} for cart {}'.format(cart['customer_id'], cart['entity_id']))
            return

        msg = """Dear {}!

We've just shipped order {}. It will be soon delivered to you.

Cheers""".format(customer['name'], order['entity_id'])

        send_message_async('mail-service', 'send', {
            "to": customer['email'],
            "msg": msg
        })


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

c = CrmService()

signal.signal(signal.SIGINT, lambda n, h: c.stop())
signal.signal(signal.SIGTERM, lambda n, h: c.stop())

c.start()
