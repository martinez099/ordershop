import atexit
import logging
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import Receivers, send_message


class OrderService(object):

    def __init__(self):
        self.es = EventStore()
        self.rs = Receivers('order-service', [self.get_orders,
                                              self.get_unbilled,
                                              self.post_orders,
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
            'id': str(uuid.uuid4()),
            'product_ids': _product_ids,
            'customer_id': _customer_id
        }

    def start(self):
        logging.info('starting ...')
        self.es.activate_entity_cache('order')
        atexit.register(self.es.deactivate_entity_cache, 'order')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_orders(self, _req):

        try:
            order_id = _req['id']
        except KeyError:
            return {
                "result": [item for item in self.es.find_all('order')]
            }

        order = self.es.find_one('order', order_id)
        if not order:
            return {
                "error": "could not find order"
            }

        return {
            "result": order
        }

    def get_unbilled(self, _req):

        billings = self.es.find_all('billing')
        orders = self.es.find_all('order')

        for billing in billings:
            to_remove = list(filter(lambda x: x['id'] == billing['order_id'], orders))
            orders.remove(to_remove[0])

        return {
            "result": [order for order in orders]
        }

    def post_orders(self, _req):

        orders = _req if isinstance(_req, list) else [_req]
        order_ids = []

        # decrement inventory
        try:
            send_message('inventory-service', 'decr_from_orders', orders)
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                        'decr_from_orders',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        for order in orders:
            try:
                new_order = OrderService.create_order(order['product_ids'], order['customer_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'product_ids' and/or 'customer_id'"
                }

            # trigger event
            self.es.publish('order', 'created', **new_order)

            order_ids.append(new_order['id'])

        return {
            "result": order_ids
        }

    def put_order(self, _req):

        try:
            order_id = _req['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        # increment inventory
        current_order = self.es.find_one('order', order_id)
        for product_id in current_order['product_ids']:
            try:
                send_message('inventory-service', 'incr_amount', {'product_id': product_id})
            except Exception as e:
                return {
                    "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                            'incr_amount',
                                                                            e.__class__.__name__,
                                                                            str(e))
                }

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
            return rsp

        order['id'] = order_id

        # trigger event
        self.es.publish('order', 'updated', **order)

        return {
            "result": True
        }

    def delete_order(self, _req):

        try:
            order_id = _req['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        order = self.es.find_one('order', order_id)
        if not order:
            return {
                "error": "could not find order"
            }

        for product_id in order['product_ids']:
            try:
                send_message('inventory-service', 'incr_amount', {'product_id': product_id})
            except Exception as e:
                return {
                    "error": "cannot send message to {}.{} ({}): {}".format('inventory-service',
                                                                            'incr_amount',
                                                                            e.__class__.__name__,
                                                                            str(e))
                }

        # trigger event
        self.es.publish('order', 'deleted', **order)

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO)

o = OrderService()
o.start()
