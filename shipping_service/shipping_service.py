import json
import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class ShippingService(object):
    """
    Shipping Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('shipping-service', [self.create_shippings,
                                                        self.update_shipping,
                                                        self.delete_shipping])

    @staticmethod
    def _create_entity(_order_id, _done=False):
        """
        Create a shipping entity.

        :param _order_id: The order which is shipped.
        :param _done: Boolean indicating fullfillment, defaults to False.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'order_id': _order_id,
            'done': _done
        }

    def start(self):
        logging.info('starting ...')
        self.event_store.subscribe('billing', self.billing_created)
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.event_store.unsubscribe('billing', self.billing_created)
        self.consumers.stop()
        logging.info('stopped.')

    def create_shippings(self, _req):

        shippings = _req if isinstance(_req, list) else [_req]
        shipping_ids = []

        for shipping in shippings:
            try:
                new_shipping = ShippingService._create_entity(shipping['order_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'order_id' and/or 'method'"
                }

            # trigger event
            self.event_store.publish('shipping', create_event('entity_created', new_shipping))

            shipping_ids.append(new_shipping['entity_id'])

        return {
            "result": shipping_ids
        }

    def update_shipping(self, _req):

        try:
            shipping_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'shipping', 'id': shipping_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        shipping = rsp['result']
        if not shipping:
            return {
                "error": "could not find shipping"
            }

        # set new props
        shipping['entity_id'] = shipping_id
        try:
            shipping['order_id'] = _req['order_id']
            shipping['done'] = _req['done']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'order_id' and/or 'done"
            }

        # trigger event
        self.event_store.publish('shipping', create_event('entity_updated', shipping))

        return {
            "result": True
        }

    def delete_shipping(self, _req):

        try:
            shipping_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'shipping', 'id': shipping_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        shipping = rsp['result']
        if not shipping:
            return {
                "error": "could not find shipping"
            }

        # trigger event
        self.event_store.publish('shipping', create_event('entity_deleted', shipping))

        return {
            "result": True
        }

    def billing_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        try:
            billing = json.loads(_item.event_data)
            result = self.create_shippings({'order_id': billing['order_id']})
            # TODO handle error
        except Exception as e:
            logging.error(f'billing_created error: {e}')


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

p = ShippingService()

signal.signal(signal.SIGINT, lambda n, h: p.stop())
signal.signal(signal.SIGTERM, lambda n, h: p.stop())

p.start()
