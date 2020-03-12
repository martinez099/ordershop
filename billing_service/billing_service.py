import logging
import signal
import time
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Consumers, send_message


class BillingService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Consumers('billing-service', [self.post_billings,
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
            'entity_id': str(uuid.uuid4()),
            'order_id': _order_id,
            'done': time.time()
        }

    def start(self):
        logging.info('starting ...')
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.receivers.stop()
        logging.info('stopped.')

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
            self.event_store.publish('billing', create_event('entity_created', new_billing))

            billing_ids.append(new_billing['entity_id'])

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
            billing['entity_id'] = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        # trigger event
        self.event_store.publish('billing', create_event('entity_updated', billing))

        return {
            "result": True
        }

    def delete_billing(self, _req):

        try:
            billing_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'billing', 'id': billing_id})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_one_entity',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        billing = rsp['result']
        if not billing:
            return {
                "error": "could not find billing"
            }

        # trigger event
        self.event_store.publish('billing', create_event('entity_deleted', billing))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

b = BillingService()

signal.signal(signal.SIGINT, lambda n, h: b.stop())
signal.signal(signal.SIGTERM, lambda n, h: b.stop())

b.start()
