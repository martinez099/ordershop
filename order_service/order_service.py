import atexit
import json
import os
import requests
import uuid

from flask import request
from flask import Flask

from common.utils import check_rsp_code
from event_store.event_store_client import EventStore


app = Flask(__name__)
store = EventStore()


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


if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.activate_entity_cache('order')
    atexit.register(store.deactivate_entity_cache, 'order')


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get(order_id=None):

    if order_id:
        order = store.find_one('order', order_id)
        if not order:
            raise ValueError("could not find order")

        return json.dumps(order) if order else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('order')])


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled():

    billings = store.find_all('billing')
    orders = store.find_all('order')

    for billing in billings:
        to_remove = list(filter(lambda x: x['id'] == billing['order_id'], orders))
        orders.remove(to_remove[0])

    return json.dumps([item for item in orders])


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    rsp = requests.post('http://inventory-service:5000/decr_from_order', json=values)
    check_rsp_code(rsp)

    if not rsp.json():
        raise ValueError("out of stock")

    order_ids = []
    for value in values:
        try:
            new_order = create_order(value['product_ids'], value['customer_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

        # trigger event
        store.publish('order', 'created', **new_order)

        order_ids.append(new_order['id'])

    return json.dumps(order_ids)


@app.route('/order/<order_id>', methods=['PUT'])
def put(order_id):

    order = store.find_one('order', order_id)
    for product_id in order['product_ids']:
        rsp = requests.post('http://inventory-service:5000/incr/{}'.format(product_id))
        check_rsp_code(rsp)

    value = request.get_json()
    try:
        order = create_order(value['product_ids'], value['customer_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

    rsp = requests.post('http://inventory-service:5000/decr_from_order', json=value)
    check_rsp_code(rsp)

    if not rsp.json():
        raise ValueError("out of stock")

    order['id'] = order_id

    # trigger event
    store.publish('order', 'updated', **order)

    for product_id in value['product_ids']:
        rsp = requests.post('http://inventory-service:5000/decr/{}'.format(product_id))
        check_rsp_code(rsp)

    return json.dumps(True)


@app.route('/order/<order_id>', methods=['DELETE'])
def delete(order_id):

    order = store.find_one('order', order_id)
    if order:
        for product_id in order['product_ids']:
            rsp = requests.post('http://inventory-service:5000/incr/{}'.format(product_id))
            check_rsp_code(rsp)

        # trigger event
        store.publish('order', 'deleted', **order)

        return json.dumps(True)
    else:
        raise ValueError("could not find order")
