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
        self.es = EventStoreClient()

    def start(self):
        logging.info('starting ...')
        self.subscribe_to_domain_events()

    def stop(self):
        self.unsubscribe_from_domain_events()
        logging.info('stopped.')

    def order_created(self, _item):
        if _item.event_action == 'entity_created':
            try:

                msg_data = json.loads(_item.event_data)
                customer = self.es.find_one('customer', msg_data['customer_id'])
                products = [self.es.find_one('product', product_id) for product_id in msg_data['product_ids']]
                msg = """Dear {}!
    
        Please transfer € {} with your favourite payment method.
    
        Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

                send_message('mail-service', 'send', {
                    "to": customer['email'],
                    "msg": msg
                })
            except Exception as e:
                logging.error(f'order_created error: {e}')

    def billing_created(self, _item):
        if _item.event_action == 'entity_created':
            try:
                msg_data = json.loads(_item.event_data)
                order = self.es.find_one('order', msg_data['order_id'])
                customer = self.es.find_one('customer', order['customer_id'])
                products = [self.es.find_one('product', product_id) for product_id in order['product_ids']]
                msg = """Dear {}!
    
        We've just received € {} from you, thank you for your transfer.
    
        Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

                send_message('mail-service', 'send', {
                    "to": customer['email'],
                    "msg": msg
                })
            except Exception as e:
                logging.error(f'billing_created error: {e}')

    def customer_created(self, _item):
        if _item.event_action == 'entity_created':
            try:
                msg_data = json.loads(_item.event_data)
                msg = """Dear {}!
        
        Welcome to Ordershop.
        
        Cheers""".format(msg_data['name'])

                send_message('mail-service', 'send', {
                    "to": msg_data['email'],
                    "msg": msg
                })
            except Exception as e:
                logging.error(f'customer_created error: {e}')

    def customer_deleted(self, _item):
        if _item.event_action == 'entity_deleted':
            try:
                msg_data = json.loads(_item.event_data)
                msg = """Dear {}!
        
        Good bye, hope to see you soon again at Ordershop.
        
        Cheers""".format(msg_data['name'])

                send_message('mail-service', 'send', {
                    "to": msg_data['email'],
                    "msg": msg
                })
            except Exception as e:
                logging.error(f'customer_deleted error: {e}')

    def subscribe_to_domain_events(self):
        self.es.subscribe('billing', self.billing_created)
        self.es.subscribe('customer', self.customer_created)
        self.es.subscribe('customer', self.customer_deleted)
        self.es.subscribe('order', self.order_created)

    def unsubscribe_from_domain_events(self):
        self.es.unsubscribe('billing', self.billing_created)
        self.es.unsubscribe('customer', self.customer_created)
        self.es.unsubscribe('customer', self.customer_deleted)
        self.es.unsubscribe('order', self.order_created)


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

c = CrmService()

signal.signal(signal.SIGINT, lambda n, h: c.stop())
signal.signal(signal.SIGTERM, lambda n, h: c.stop())

c.start()
