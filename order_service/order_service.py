import logging
import json
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class OrderService(object):
    """
    Order Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('order-service', [self.create_orders,
                                                     self.update_order,
                                                     self.delete_order])

    @staticmethod
    def _create_entity(_cart_id, _status='CREATED'):
        """
        Create an order entity.

        :param _cart_id: The cart ID the order is for.
        :param _status: The current status of the order, defaults to CREATED.
                        Other options are OUT_OF_STOCK, IN_STOCK, CLEARED, UNCLEARED, SHIPPED and DELIVERED.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'cart_id': _cart_id,
            'status': _status,
        }

    def start(self):
        logging.info('starting ...')
        self.event_store.subscribe('billing', self.billing_created)
        self.event_store.subscribe('billing', self.billing_deleted)
        self.event_store.subscribe('shipping', self.shipping_created)
        self.event_store.subscribe('shipping', self.shipping_updated)
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.event_store.unsubscribe('billing', self.billing_created)
        self.event_store.unsubscribe('billing', self.billing_deleted)
        self.event_store.unsubscribe('shipping', self.shipping_created)
        self.event_store.unsubscribe('shipping', self.shipping_updated)
        self.consumers.stop()
        logging.info('stopped.')

    def create_orders(self, _req):

        orders = _req if isinstance(_req, list) else [_req]
        order_ids = []

        for order in orders:
            try:
                new_order = OrderService._create_entity(order['cart_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'cart_id'"
                }

            # trigger event
            self.event_store.publish('order', create_event('entity_created', new_order))

            order_ids.append(new_order['entity_id'])

        return {
            "result": order_ids
        }

    def update_order(self, _req):

        try:
            order_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': order_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        order = rsp['result']
        if not order:
            return {
                "error": "could not find order"
            }

        # set new props
        order['entity_id'] = order_id
        try:
            order['cart_id'] = _req['cart_id']
            order['status'] = _req['status']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'cart_id' and/or 'status"
            }

        # trigger event
        self.event_store.publish('order', create_event('entity_updated', order))

        return {
            "result": True
        }

    def delete_order(self, _req):

        try:
            order_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': order_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        order = rsp['result']
        if not order:
            return {
                "error": "could not find order"
            }

        # trigger event
        self.event_store.publish('order', create_event('entity_deleted', order))

        return {
            "result": True
        }

    def billing_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        billing = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': billing['order_id']})
        order = rsp['result']
        if not order['status'] == 'IN_STOCK':
            return

        order['status'] = 'CLEARED'
        self.event_store.publish('order', create_event('entity_updated', order))

    def billing_deleted(self, _item):
        if _item.event_action != 'entity_delted':
            return

        billing = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': billing['order_id']})
        order = rsp['result']
        if not order['status'] == 'CLEARED':
            return

        order['status'] = 'UNCLEARED'
        self.event_store.publish('order', create_event('entity_updated', order))

    def shipping_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        shipping = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': shipping['order_id']})
        order = rsp['result']
        if not order['status'] == 'CLEARED':
            return

        order['status'] = 'SHIPPED'
        self.event_store.publish('order', create_event('entity_updated', order))

    def shipping_updated(self, _item):
        if _item.event_action != 'entity_updated':
            return

        shipping = json.loads(_item.event_data)
        if not shipping['delivered']:
            return

        rsp = send_message('read-model', 'get_entities', {'name': 'order', 'id': shipping['order_id']})
        order = rsp['result']
        order['status'] = 'DELIVERED'
        self.event_store.publish('order', create_event('entity_updated', order))


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

o = OrderService()

signal.signal(signal.SIGINT, lambda n, h: o.stop())
signal.signal(signal.SIGTERM, lambda n, h: o.stop())

o.start()
