import atexit
import json
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue, Receivers, send_message


class OrderService(object):

    def __init__(self):
        self.store = EventStore()
        self.mq = MessageQueue()
        self.rs = Receivers(self.mq, 'order-service', [self.get_orders,
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
        self.store.activate_entity_cache('order')
        atexit.register(self.store.deactivate_entity_cache, 'order')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_orders(self, _req):

        try:
            order_id = json.loads(_req)['id']
        except KeyError:
            orders = json.dumps([item for item in self.store.find_all('order')])
            return json.dumps(orders)

        order = self.store.find_one('order', order_id)
        if not order:
            raise ValueError("could not find order")

        return json.dumps(order) if order else json.dumps(False)

    def get_unbilled(self, _req):

        billings = self.store.find_all('billing')
        orders = self.store.find_all('order')

        for billing in billings:
            to_remove = list(filter(lambda x: x['id'] == billing['order_id'], orders))
            orders.remove(to_remove[0])

        return json.dumps([item for item in orders])

    def post_orders(self, _req):

        orders = json.loads(_req)
        if not isinstance(orders, list):
            orders = [orders]

        # decrement inventory
        send_message(self.mq, 'inventory-service', 'decr_from_order', orders)

        order_ids = []
        for order in orders:
            try:
                new_order = OrderService.create_order(order['product_ids'], order['customer_id'])
            except KeyError:
                raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

            # trigger event
            self.store.publish('order', 'created', **new_order)

            order_ids.append(new_order['id'])

        return json.dumps(order_ids)

    def put_order(self, _req):

        order = json.loads(_req)
        try:
            order_id = order['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        # increment inventory
        current_order = self.store.find_one('order', order_id)
        for product_id in current_order['product_ids']:
            send_message(self.mq, 'inventory-service', 'incr_amount', {'product_id': product_id})

        try:
            order = OrderService.create_order(order['product_ids'], order['customer_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

        # decrement inventory
        rsp = send_message(self.mq, 'inventory-service', 'decr_from_order', order)
        if json.loads(rsp) is False:
            raise ValueError("out of stock")

        order['id'] = order_id

        # trigger event
        self.store.publish('order', 'updated', **order)

        return json.dumps(True)

    def delete_order(self, _req):

        try:
            order_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        order = self.store.find_one('order', order_id)
        if not order:
            raise ValueError("could not find order")

        for product_id in order['product_ids']:
            send_message(self.mq, 'inventory-service', 'incr_amount', {'product_id': product_id})

        # trigger event
        self.store.publish('order', 'deleted', **order)

        return json.dumps(True)


o = OrderService()
o.start()
