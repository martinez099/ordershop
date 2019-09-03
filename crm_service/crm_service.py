import atexit
import json
import logging

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import send_message


class CrmService(object):

    def __init__(self):
        self.es = EventStoreClient()

    def start(self):
        logging.info('starting ...')
        self.subscribe_to_domain_events()
        atexit.register(self.unsubscribe_from_domain_events)

    def order_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            customer = self.es.find_one('customer', msg_data['customer_id'])
            products = [self.es.find_one('product', product_id) for product_id in msg_data['product_ids']]
            msg = """Dear {}!

    Please transfer € {} with your favourite payment method.

    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message('mail-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(f'order_created error: {e}')

    def billing_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            order = self.es.find_one('order', msg_data['order_id'])
            customer = self.es.find_one('customer', order['customer_id'])
            products = [self.es.find_one('product', product_id) for product_id in order['product_ids']]
            msg = """Dear {}!

    We've just received € {} from you, thank you for your transfer.

    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message('mail-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(f'billing_created error: {e}')

    def customer_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Welcome to Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message('mail-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(f'customer_created error: {e}')

    def customer_deleted(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Good bye, hope to see you soon again at Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message('mail-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(f'customer_deleted error: {e}')

    def subscribe_to_domain_events(self):
        self.es.subscribe('billing', 'created', self.billing_created)
        self.es.subscribe('customer', 'created', self.customer_created)
        self.es.subscribe('customer', 'deleted', self.customer_deleted)
        self.es.subscribe('order', 'created', self.order_created)

    def unsubscribe_from_domain_events(self):
        self.es.unsubscribe('billing', 'created', self.billing_created)
        self.es.unsubscribe('customer', 'created', self.customer_created)
        self.es.unsubscribe('customer', 'deleted', self.customer_deleted)
        self.es.unsubscribe('order', 'created', self.order_created)


logging.basicConfig(level=logging.INFO)

c = CrmService()
c.start()
