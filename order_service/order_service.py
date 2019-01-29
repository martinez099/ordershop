import atexit
import json
import os
import requests

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.common import check_rsp
from lib.entity_cache import EntityCache
from lib.event_store import Event
from lib.repository import Repository, Entity


class Order(Entity):
    """
    Order Entity class.
    """

    def __init__(self, _product_ids, _customer_id):
        super(Order, self).__init__()
        self.product_ids = _product_ids
        self.customer_id = _customer_id

    def get_product_ids(self):
        return self.product_ids

    def get_customer_id(self):
        return self.customer_id


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EntityCache(redis)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and hasattr(store, 'subscribe_to_order_events'):
    store.subscribe_to_order_events()
    atexit.register(store.unsubscribe_from_order_events)


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get(order_id=None):

    if order_id:
        order = repo.get_item(order_id)
        return json.dumps(order.__dict__) if order else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled():

    rsp = requests.get('http://billing-service:5000/billing')
    check_rsp(rsp)

    billings = rsp.json()
    orders = repo.get_items()

    for billing in billings:
        to_remove = list(filter(lambda x: x.id == billing['order_id'], orders))
        orders.remove(to_remove[0])

    return json.dumps([item.__dict__ for item in orders])


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
def post():

    values = request.get_json()

    if not isinstance(values, list):
        values = [values]

    rsp = requests.post('http://inventory-service:5000/check', json=values)
    check_rsp(rsp)

    if not rsp.json():
        raise ValueError("out of stock")

    order_ids = []
    for value in values:

        try:
            new_order = Order(value['product_ids'], value['customer_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

        if repo.set_item(new_order):

            # trigger event
            store.publish(Event('order', 'created', **new_order.__dict__))

            order_ids.append(str(new_order.id))
        else:
            raise ValueError("could not create order")

        for product_id in value['product_ids']:
            rsp = requests.post('http://inventory-service:5000/decr/{}'.format(product_id))
            check_rsp(rsp)

    return json.dumps({'status': 'ok',
                       'ids': order_ids})


@app.route('/order/<order_id>', methods=['PUT'])
def put(order_id=None):

    order = repo.get_item(order_id)
    for product_id in order.product_ids:
        rsp = requests.post('http://inventory-service:5000/incr/{}'.format(product_id))
        check_rsp(rsp)

    value = request.get_json()
    try:
        order = Order(value['product_ids'], value['customer_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

    rsp = requests.post('http://inventory-service:5000/check', json=value)
    check_rsp(rsp)

    if not rsp.json():
        raise ValueError("out of stock")

    order.id = order_id
    if repo.set_item(order):

        # trigger event
        store.publish(Event('order', 'updated', **order.__dict__))

        for product_id in value['product_ids']:
            rsp = requests.post('http://inventory-service:5000/decr/{}'.format(product_id))
            check_rsp(rsp)

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update order")


@app.route('/order/<order_id>', methods=['DELETE'])
def delete(order_id=None):

    order = repo.del_item(order_id)
    if order:

        for product_id in order.product_ids:
            rsp = requests.post('http://inventory-service:5000/incr/{}'.format(product_id))
            check_rsp(rsp)

        # trigger event
        store.publish(Event('order', 'deleted', **order.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not delete order")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
