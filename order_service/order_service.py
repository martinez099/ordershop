import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Consumers, send_message


class OrderService(object):
    """
    Order Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Consumers('order-service', [self.post_orders,
                                                     self.put_order,
                                                     self.delete_order])

    @staticmethod
    def create_order(_product_ids, _customer_id):
        """
        Create an order entity.

        :param _product_ids: The product IDs the order is for.
        :param _customer_id: The customer ID the order is made by.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'product_ids': _product_ids,
            'customer_id': _customer_id
        }

    def start(self):
        logging.info('starting ...')
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.receivers.stop()
        logging.info('stopped.')

    def post_orders(self, _req):

        orders = _req if isinstance(_req, list) else [_req]
        order_ids = []

        # decrement inventory
        try:
            rsp = send_message('inventory-service', 'decr_from_orders', orders)
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                        'decr_from_orders',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from inventory-service)'
            return rsp

        for order in orders:
            try:
                new_order = OrderService.create_order(order['product_ids'], order['customer_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'product_ids' and/or 'customer_id'"
                }

            # trigger event
            self.event_store.publish('order', create_event('entity_created', new_order))

            order_ids.append(new_order['entity_id'])

        return {
            "result": order_ids
        }

    def put_order(self, _req):

        try:
            order_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'order', 'id': order_id})
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

        current_order = rsp['result']

        # increment inventory
        for product_id in current_order['product_ids']:
            try:
                rsp = send_message('inventory-service', 'incr_amount', {'product_id': product_id})
            except Exception as e:
                return {
                    "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                            'incr_amount',
                                                                            e.__class__.__name__,
                                                                            str(e))
                }

            if 'error' in rsp:
                rsp['error'] += ' (from inventory-service)'
                return rsp

        try:
            order = OrderService.create_order(_req['product_ids'], _req['customer_id'])
        except KeyError:
            return {
                "result": "missing mandatory parameter 'product_ids' and/or 'customer_id'"
            }

        # decrement inventory
        try:
            rsp = send_message('inventory-service', 'decr_from_orders', order)
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                        'decr_from_orders',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from inventory-service)'
            return rsp

        order['entity_id'] = order_id

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

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'order', 'id': order_id})
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

        order = rsp['result']
        if not order:
            return {
                "error": "could not find order"
            }

        # increment inventory
        for product_id in order['product_ids']:
            try:
                rsp = send_message('inventory-service', 'incr_amount', {'product_id': product_id})
            except Exception as e:
                return {
                    "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                            'incr_amount',
                                                                            e.__class__.__name__,
                                                                            str(e))
                }

            if 'error' in rsp:
                rsp['error'] += ' (from inventory-service)'
                return rsp

        # trigger event
        self.event_store.publish('order', create_event('entity_deleted', order))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

o = OrderService()

signal.signal(signal.SIGINT, lambda n, h: o.stop())
signal.signal(signal.SIGTERM, lambda n, h: o.stop())

o.start()
