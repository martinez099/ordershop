import atexit
import functools
import logging
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Receivers, send_message


class OrderService(object):
    """
    Order Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Receivers('order-service', [self.get_orders,
                                                     self.get_unbilled,
                                                     self.post_orders,
                                                     self.put_order,
                                                     self.delete_order])
        self.orders = deduce_entities(self.event_store.get('order'))
        self.tracking_handler = functools.partial(track_entities, self.orders)

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
        self.event_store.subscribe('order', self.tracking_handler)
        atexit.register(self.stop)
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.event_store.unsubscribe('order', self.tracking_handler)
        self.receivers.stop()
        logging.info('stopped.')

    def get_orders(self, _req):

        try:
            order_id = _req['entity_id']
        except KeyError:
            return {
                "result": list(self.orders.values())
            }

        order = self.orders.get(order_id)
        if not order:
            return {
                "error": "could not find order"
            }

        return {
            "result": order
        }

    def get_unbilled(self, _req):

        orders = list(self.orders.values())

        # get billings
        try:
            rsp = send_message('billing-service', 'get_billings')
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('billing-service',
                                                                        'get_billings',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from inventory-service)'
            return rsp

        for billing in rsp['result']:
            to_remove = list(filter(lambda x: x['entity_id'] == billing['order_id'], orders))
            orders.remove(to_remove[0])

        return {
            "result": orders
        }

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

        # increment inventory
        current_order = self.orders.get(order_id)

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

        order = self.orders.get(order_id)
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


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)-6s] %(message)s')

o = OrderService()
o.start()
