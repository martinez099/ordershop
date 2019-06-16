import atexit
import json
import uuid

import gevent
from gevent import monkey
monkey.patch_all()

from common.utils import run_receiver, do_send
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


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


def get_orders(req):

    try:
        order_id = json.loads(req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('order')])
        mq.send_rsp('order-service', 'get-orders', rsp)
        return

    order = store.find_one('order', order_id)
    if not order:
        raise ValueError("could not find order")

    return json.dumps(order) if order else json.dumps(False)


def get_unbilled(req):

    billings = store.find_all('billing')
    orders = store.find_all('order')

    for billing in billings:
        to_remove = list(filter(lambda x: x['id'] == billing['order_id'], orders))
        orders.remove(to_remove[0])

    return json.dumps([item for item in orders])


def post_orders(req, mq):

    orders = json.loads(req)
    if not isinstance(orders, list):
        orders = [orders]
        
    # decrement inventory
    do_send(mq, 'inventory-service', 'decr-from-order', orders)

    order_ids = []
    for order in orders:
        try:
            new_order = create_order(order['product_ids'], order['customer_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

        # trigger event
        store.publish('order', 'created', **new_order)

        order_ids.append(new_order['id'])

    return json.dumps(order_ids)


def put_order(req, mq):
    
    order = json.loads(req)
    order_id = order['id']

    # increment inventory
    current_order = store.find_one('order', order_id)
    for product_id in current_order['product_ids']:
        do_send(mq, 'inventory-service', 'incr', json.dumps({'id': product_id}))

    try:
        order = create_order(order['product_ids'], order['customer_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

    rsp = do_send(mq, 'inventory-service', 'decr_from_order', order)
    
    if json.loads(rsp) is False:
        raise ValueError("out of stock")

    try:
        order_id = order['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    order['id'] = order_id

    # decrement inventory
    for product_id in order['product_ids']:
        do_send(mq, 'inventory-service', 'decr', {'id': product_id})

    # trigger event
    store.publish('order', 'updated', **order)

    return json.dumps(True)


def delete_order(req):

    try:
        order_id = json.loads(req)['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    order = store.find_one('order', order_id)
    if not order:
        raise ValueError("could not find order")

    for product_id in order['product_ids']:
        do_send(mq, 'inventory-service', 'incr', {'id': product_id})
      
    # trigger event
    store.publish('order', 'deleted', **order)

    return json.dumps(True)


store = EventStore()
mq = MessageQueue()

store.activate_entity_cache('order')
atexit.register(store.deactivate_entity_cache, 'order')

gevent.joinall([
    gevent.spawn(run_receiver, mq, 'order-service', 'get_orders', get_orders),
    gevent.spawn(run_receiver, mq, 'order-service', 'post_orders', post_orders),
    gevent.spawn(run_receiver, mq, 'order-service', 'put_order', put_order),
    gevent.spawn(run_receiver, mq, 'order-service', 'delete_order', delete_order)
])
