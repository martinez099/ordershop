import atexit
import json

from common.utils import log_info, log_error, send_message
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


class CrmService(object):

    def __init__(self):
        self.store = EventStore()
        self.mq = MessageQueue()

    def start(self):
        self.subscribe_to_domain_events()
        atexit.register(self.unsubscribe_from_domain_events)

    def customer_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Welcome to Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message(self.mq, 'messaging-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            log_error(e)

    def customer_deleted(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            msg = """Dear {}!
    
    Good bye, hope to see you soon again at Ordershop.
    
    Cheers""".format(msg_data['name'])

            send_message(self.mq, 'messaging-service', 'send_email', {
                "to": msg_data['email'],
                "msg": msg
            })
        except Exception as e:
            log_error(e)

    def order_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            customer = self.store.find_one('customer', msg_data['customer_id'])
            products = [self.store.find_one('product', product_id) for product_id in msg_data['product_ids']]
            msg = """Dear {}!
    
    Thank you for buying following {} products from Ordershop:
    {}
    
    Cheers""".format(customer['name'], len(products), ", ".join([product['name'] for product in products]))

            send_message(self.mq, 'messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            log_error(e)

    def subscribe_to_domain_events(self):
        self.store.subscribe('customer', 'created', self.customer_created)
        self.store.subscribe('customer', 'deleted', self.customer_deleted)
        self.store.subscribe('order', 'created', self.order_created)
        log_info('subscribed to domain events')

    def unsubscribe_from_domain_events(self):
        self.store.unsubscribe('customer', 'created', self.customer_created)
        self.store.unsubscribe('customer', 'deleted', self.customer_deleted)
        self.store.unsubscribe('order', 'created', self.order_created)
        log_info('unsubscribed from domain events')


c = CrmService()
c.start()
