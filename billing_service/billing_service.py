import atexit
import json
import logging
import time
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import Receivers, send_message


class BillingService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.es = EventStore()
        self.rs = Receivers('billing-service', [self.get_billings,
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
        self.es.activate_entity_cache('billing')
        atexit.register(self.es.deactivate_entity_cache, 'billing')
        self.subscribe_to_domain_events()
        atexit.register(self.unsubscribe_from_domain_events)
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def order_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            customer = self.es.find_one('customer', msg_data['customer_id'])
            products = [self.es.find_one('product', product_id) for product_id in msg_data['product_ids']]
            msg = """Dear {}!
    
    Please transfer € {} with your favourite payment method.
    
    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message('messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(e)

    def billing_created(self, _item):
        try:
            msg_data = json.loads(_item.event_entity)
            order = self.es.find_one('order', msg_data['order_id'])
            customer = self.es.find_one('customer', order['customer_id'])
            products = [self.es.find_one('product', product_id) for product_id in order['product_ids']]
            msg = """Dear {}!
    
    We've just received € {} from you, thank you for your transfer.
    
    Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

            send_message('messaging-service', 'send_email', {
                "to": customer['email'],
                "msg": msg
            })
        except Exception as e:
            logging.error(e)

    def subscribe_to_domain_events(self):
        self.es.subscribe('order', 'created', self.order_created)
        self.es.subscribe('billing', 'created', self.billing_created)
        logging.info('subscribed to domain events')

    def unsubscribe_from_domain_events(self):
        self.es.unsubscribe('order', 'created', self.order_created)
        self.es.unsubscribe('billing', 'created', self.billing_created)
        logging.info('unsubscribed from domain events')

    def get_billings(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            billings = json.dumps([item for item in self.es.find_all('billing')])
            return json.dumps(billings)

        billing = self.es.find_one('billing', billing_id)
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
            self.es.publish('billing', 'created', **new_billing)

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
        self.es.publish('billing', 'updated', **billing)

        return json.dumps(True)

    def delete_billing(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        billing = self.es.find_one('billing', billing_id)
        if not billing:
            raise ValueError("could not find billing")

        # trigger event
        self.es.publish('billing', 'deleted', **billing)

        return json.dumps(True)


logging.basicConfig(level=logging.INFO)

b = BillingService()
b.start()
