import atexit
import json
import os

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.event_store import EventStore, Event
from lib.repository import Repository, Entity


class Order(Entity):
    """
    Order Entity
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
store = EventStore(redis)

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


@app.route('/orders', methods=['POST'])
@app.route('/order', methods=['POST'])
def post():

    values = request.get_json()

    if not isinstance(values, list):
        values = [values]

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

    return json.dumps({'status': 'ok',
                       'ids': order_ids})


@app.route('/order/<order_id>', methods=['PUT'])
def put(order_id=None):

    value = request.get_json()
    try:
        order = Order(value['product_ids'], value['customer_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

    order.id = order_id
    if repo.set_item(order):

        # trigger event
        store.publish(Event('order', 'updated', **order.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update order")


@app.route('/order/<order_id>', methods=['DELETE'])
def delete(order_id=None):

    order = repo.del_item(order_id)
    if order:

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
