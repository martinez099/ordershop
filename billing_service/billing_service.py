import atexit
import time
import json
import os

from redis import StrictRedis
from flask import request
from flask import Flask

import requests

import lib.common
from lib.entity_cache import EntityCache
from lib.event_store import Event
from lib.repository import Repository, Entity


class Billing(Entity):
    """
    Billing Entity class.
    """

    def __init__(self, _order_id):
        super(Billing, self).__init__()
        self.order_id = _order_id
        self.done = time.time()

    def get_order_id(self):
        return self.order_id

    def get_done(self):
        return self.done


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EntityCache(redis)


def order_created(item):
    try:
        msg_data = json.loads(item[1][0][1]['entity'])
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
        lib.common.log_error(e)


def subscribe():
    store.subscribe('order', 'created', order_created)
    lib.common.log_info('subscribed to channels')


def unsubscribe():
    store.unsubscribe('order', 'created', order_created)
    lib.common.log_info('unsubscribed from channels')


if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and hasattr(store, 'subscribe_to_billing_events'):
    store.subscribe_to_billing_events()
    atexit.register(store.unsubscribe_from_billing_events)
    subscribe()
    atexit.register(unsubscribe)


@app.route('/billing', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get(billing_id=None):

    if billing_id:
        item = repo.get_item(billing_id)
        return json.dumps(item.__dict__) if item else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/billing', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    billing_ids = []
    for value in values:
        try:
            new_billing = Billing(value['order_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'order_id'")

        if repo.set_item(new_billing):

            # trigger event
            store.publish(Event('billing', 'created', **new_billing.__dict__))

            billing_ids.append(str(new_billing.id))
        else:
            raise ValueError("could not create billing")

    return json.dumps({'status': 'ok',
                       'ids': billing_ids})


@app.route('/billing/<billing_id>', methods=['PUT'])
def put(billing_id=None):

    value = request.get_json()
    try:
        billing = Billing(value['order_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'order_id'")

    billing.id = billing_id
    if repo.set_item(billing):

        # trigger event
        store.publish(Event('billing', 'updated', **billing.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update billing")


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete(billing_id=None):

    billing = repo.del_item(billing_id)
    if billing:

        # trigger event
        store.publish(Event('billing', 'deleted', **billing.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not delete billing")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
