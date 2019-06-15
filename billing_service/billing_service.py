import atexit
import json
import os
import time
import uuid

from flask import request
from flask import Flask

import requests

from common.utils import log_error, log_info
from event_store.event_store_client import EventStore


app = Flask(__name__)
store = EventStore()


def create_billing(_order_id):
    """
    Create a billing entity.

    :param _order_id: The order ID the billing belongs to.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'order_id': _order_id,
        'done': time.time()
    }


def order_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Please transfer € {} with your favourite payment method.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        requests.post('http://msg-service:5000/email', json={
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def billing_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        order = store.find_one('order', msg_data['order_id'])
        customer = store.find_one('customer', order['customer_id'])
        products = [store.find_one('product', product_id) for product_id in order['product_ids']]
        msg = """Dear {}!

We've just received € {} from you, thank you for your transfer.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        requests.post('http://msg-service:5000/email', json={
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def subscribe_to_domain_events():
    store.subscribe('order', 'created', order_created)
    store.subscribe('billing', 'created', billing_created)
    log_info('subscribed to domain events')


def unsubscribe_from_domain_events():
    store.unsubscribe('order', 'created', order_created)
    store.unsubscribe('billing', 'created', billing_created)
    log_info('unsubscribed from domain events')


if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.activate_entity_cache('billing')
    atexit.register(store.deactivate_entity_cache, 'billing')
    subscribe_to_domain_events()
    atexit.register(unsubscribe_from_domain_events)


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get(billing_id=None):

    if billing_id:
        billing = store.find_one('billing', billing_id)
        if not billing:
            raise ValueError("could not find billing")

        return json.dumps(billing) if billing else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('billing')])


@app.route('/billing', methods=['POST'])
@app.route('/billings', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    billing_ids = []
    for value in values:
        try:
            new_billing = create_billing(value['order_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'order_id'")

        # trigger event
        store.publish('billing', 'created', **new_billing)

        billing_ids.append(new_billing['id'])

    return json.dumps(billing_ids)


@app.route('/billing/<billing_id>', methods=['PUT'])
def put(billing_id):

    value = request.get_json()
    try:
        billing = create_billing(value['order_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'order_id'")

    billing['id'] = billing_id

    # trigger event
    store.publish('billing', 'updated', **billing)

    return json.dumps(True)


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete(billing_id):

    billing = store.find_one('billing', billing_id)
    if billing:

        # trigger event
        store.publish('billing', 'deleted', **billing)

        return json.dumps(True)
    else:
        raise ValueError("could not find billing")
