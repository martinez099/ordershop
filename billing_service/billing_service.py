import atexit
import logging
import time
import uuid

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import Receivers, send_message


class BillingService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.es = EventStoreClient()
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
        logging.info('starting ...')
        self.es.activate_entity_cache('billing')
        atexit.register(self.es.deactivate_entity_cache, 'billing')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_billings(self, _req):

        try:
            billing_id = _req['id']
        except KeyError:
            return {
                "result": list(self.es.find_all('billing').values())
            }

        billing = self.es.find_one('billing', billing_id)
        if not billing:
            return {
                "error": "could not find billing"
            }

        return {
            "result": billing
        }

    def post_billings(self, _req):

        billings = _req if isinstance(_req, list) else [_req]
        billing_ids = []

        for billing in billings:
            try:
                new_billing = BillingService.create_billing(billing['order_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'order_id'"
                }

            # trigger event
            self.es.publish('billing', 'created', **new_billing)

            billing_ids.append(new_billing['id'])

        return {
            "result": billing_ids
        }

    def put_billing(self, _req):

        try:
            billing = BillingService.create_billing(_req['order_id'])
        except KeyError:
            return {
                "error": "missing mandatory parameter 'order_id'"
            }

        try:
            billing_id = billing['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        billing['id'] = billing_id

        # trigger event
        self.es.publish('billing', 'updated', **billing)

        return {
            "result": True
        }

    def delete_billing(self, _req):

        try:
            billing_id = _req['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        billing = self.es.find_one('billing', billing_id)
        if not billing:
            return {
                "error": "could not find billing"
            }

        # trigger event
        self.es.publish('billing', 'deleted', **billing)

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO)

b = BillingService()
b.start()
