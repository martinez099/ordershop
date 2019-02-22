import atexit
import json
import os
import requests

from flask import request
from flask import Flask

from common.factory import create_order
from common.utils import check_rsp_code
from lib.event_store import Event, EventStore


app = Flask(__name__)
store = EventStore()


if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.subscribe_to_entity_events('order')
    atexit.register(store.unsubscribe_from_entity_events, 'order')


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get(order_id=None):

    if order_id:
        order = store.find_one('order', order_id)
        if not order:
            raise ValueError("could not find order")

        return json.dumps(order) if order else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('order').values()])


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled():

    billings = store.find_all('billing').values()
    orders = list(store.find_all('order').values())

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
        store.publish(Event('order', 'created', **new_order))

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
    store.publish(Event('order', 'updated', **order))

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
        store.publish(Event('order', 'deleted', **order))

        return json.dumps(True)
    else:
        raise ValueError("could not find order")
