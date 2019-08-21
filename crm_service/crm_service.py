import atexit
import json
import logging

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import send_message


class CrmService(object):

    def __init__(self):
        self.es = EventStore()

    def start(self):
        self.subscribe_to_domain_events()
        atexit.register(self.unsubscribe_from_domain_events)

    def customer_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Welcome to Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message('messaging-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(e)

    def customer_deleted(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Good bye, hope to see you soon again at Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message('messaging-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(e)

    def order_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            customer = self.es.find_one('customer', msg_data['customer_id'])
            products = [self.es.find_one('product', product_id) for product_id in msg_data['product_ids']]
            msg = """Dear {}!
    
    Thank you for buying following {} products from Ordershop:
    {}
    
    Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

            send_message('messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(e)

    def subscribe_to_domain_events(self):
        self.es.subscribe('customer', 'created', self.customer_created)
        self.es.subscribe('customer', 'deleted', self.customer_deleted)
        self.es.subscribe('order', 'created', self.order_created)
        logging.info('subscribed to domain events')

    def unsubscribe_from_domain_events(self):
        self.es.unsubscribe('customer', 'created', self.customer_created)
        self.es.unsubscribe('customer', 'deleted', self.customer_deleted)
        self.es.unsubscribe('order', 'created', self.order_created)
        logging.info('unsubscribed from domain events')


logging.basicConfig(level=logging.INFO)

c = CrmService()
c.start()
