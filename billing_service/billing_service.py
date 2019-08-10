import atexit
import json
import time
import uuid

from common.utils import log_error, log_info, send_message
from common.receivers import Receivers
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


class BillingService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.store = EventStore()
        self.mq = MessageQueue()
        self.rs = Receivers(self.mq, 'billing-service', [self.get_billings,
                                                         self.post_billings,
                                                         self.put_billing,
                                                         self.delete_billing])

    @staticmethod
    def create_billing(_order_id):
        """
        Create a billing entity.

        :param _order_id: The order ID the billing belongs to.
        :return: A dict with the entity properties.
        """
        return {
            'id': str(uuid.uuid4()),
            'order_id': _order_id,
            'done': time.time()
        }

    def start(self):
        self.store.activate_entity_cache('billing')
        atexit.register(self.store.deactivate_entity_cache, 'billing')
        self.subscribe_to_domain_events()
        atexit.register(self.unsubscribe_from_domain_events)
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def order_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            customer = self.store.find_one('customer', msg_data['customer_id'])
            products = [self.store.find_one('product', product_id) for product_id in msg_data['product_ids']]
            msg = """Dear {}!
    
    Please transfer € {} with your favourite payment method.
    
    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message(self.mq, 'messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            log_error(e)

    def billing_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            order = self.store.find_one('order', msg_data['order_id'])
            customer = self.store.find_one('customer', order['customer_id'])
            products = [self.store.find_one('product', product_id) for product_id in order['product_ids']]
            msg = """Dear {}!
    
    We've just received € {} from you, thank you for your transfer.
    
    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message(self.mq, 'messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            log_error(e)

    def subscribe_to_domain_events(self):
        self.store.subscribe('order', 'created', self.order_created)
        self.store.subscribe('billing', 'created', self.billing_created)
        log_info('subscribed to domain events')

    def unsubscribe_from_domain_events(self):
        self.store.unsubscribe('order', 'created', self.order_created)
        self.store.unsubscribe('billing', 'created', self.billing_created)
        log_info('unsubscribed from domain events')

    def get_billings(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            billings = json.dumps([item for item in self.store.find_all('billing')])
            return json.dumps(billings)

        billing = self.store.find_one('billing', billing_id)
        if not billing:
            raise ValueError("could not find billing")

        return json.dumps(billing) if billing else json.dumps(False)

    def post_billings(self, _req):

        billings = json.loads(_req)
        if not isinstance(billings, list):
            billings = [billings]

        billing_ids = []
        for billing in billings:
            try:
                new_billing = BillingService.create_billing(billing['order_id'])
            except KeyError:
                raise ValueError("missing mandatory parameter 'order_id'")

            # trigger event
            self.store.publish('billing', 'created', **new_billing)

            billing_ids.append(new_billing['id'])

        return json.dumps(billing_ids)

    def put_billing(self, _req):

        billing = json.loads(_req)
        try:
            billing = BillingService.create_billing(billing['order_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'order_id'")

        try:
            billing_id = billing['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        billing['id'] = billing_id

        # trigger event
        self.store.publish('billing', 'updated', **billing)

        return json.dumps(True)

    def delete_billing(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        billing = self.store.find_one('billing', billing_id)
        if not billing:
            raise ValueError("could not find billing")

        # trigger event
        self.store.publish('billing', 'deleted', **billing)

        return json.dumps(True)


b = BillingService()
b.start()
