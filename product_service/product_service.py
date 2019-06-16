import atexit
import json
import uuid

import gevent
from gevent import monkey
monkey.patch_all()

from common.utils import log_error, log_info, run_receiver, do_send
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


def create_product(_name, _price):
    """
    Create a product entity.

    :param _name: The name of the product.
    :param _price: The price of the product.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'name': _name,
        'price': _price
    }


def get_products(req):

    try:
        product_id = json.loads(req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('product')])
        mq.send_rsp('product-service', 'get-products', rsp)
        return

    product = store.find_one('product', product_id)
    if not product:
        raise ValueError("could not find product")

    return json.dumps(product) if product else json.dumps(False)


def post_products(req):

    products = json.loads(req)
    if not isinstance(products, list):
        products = [products]

    product_ids = []
    for product in products:
        try:
            new_product = create_product(value['name'], value['price'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'price'")

        # trigger event
        store.publish('product', 'created', **new_product)

        product_ids.append(new_product['id'])

    return json.dumps(product_ids)


def put_product(req):
    product = json.loads(req)

    try:
        product = create_product(product['name'], product['price'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    try:
        product_id = product['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    product['id'] = product_id

    # trigger event
    store.publish('product', 'updated', **product)

    return json.dumps(True)


def delete_product(req):

    try:
        product_id = json.loads(req)['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    product = store.find_one('product', product_id)
    if not product:
        raise ValueError("could not find product")

    # trigger event
    store.publish('product', 'deleted', **product)

    return json.dumps(True)


store = EventStore()
mq = MessageQueue()

store.activate_entity_cache('product')
atexit.register(store.deactivate_entity_cache, 'product')

gevent.joinall([
    gevent.spawn(run_receiver, mq, 'product-service', 'get_products', get_products),
    gevent.spawn(run_receiver, mq, 'product-service', 'post_products', post_products),
    gevent.spawn(run_receiver, mq, 'product-service', 'put_product', put_product),
    gevent.spawn(run_receiver, mq, 'product-service', 'delete_product', delete_product)
])
