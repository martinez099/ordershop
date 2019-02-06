import atexit
import json
import os
import uuid

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.event_store import Event, EventStore


class Product(object):
    """
    Product Entity class.
    """

    def __init__(self, _name, _price):
        self.id = str(uuid.uuid4())
        self.name = _name
        self.price = _price


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
store = EventStore(redis)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.subscribe_to_entity_events('product')
    atexit.register(store.unsubscribe_from_entity_events, 'product')


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get(product_id=None):

    if product_id:
        product = store.find_one('product', product_id)
        if not product:
            raise ValueError("could not find product")

        return json.dumps(product) if product else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('product').values()])


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    product_ids = []
    for value in values:
        try:
            new_product = Product(value['name'], value['price'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'price'")

        # trigger event
        store.publish(Event('product', 'created', **new_product.__dict__))

        product_ids.append(new_product.id)

    return json.dumps(product_ids)


@app.route('/product/<product_id>', methods=['PUT'])
def put(product_id):

    value = request.get_json()
    try:
        product = Product(value['name'], value['price'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    product.id = product_id

    # trigger event
    store.publish(Event('product', 'updated', **product.__dict__))

    return json.dumps(True)


@app.route('/product/<product_id>', methods=['DELETE'])
def delete(product_id):

    product = store.find_one('product', product_id)
    if product:

        # trigger event
        store.publish(Event('product', 'deleted', **product))

        return json.dumps(True)
    else:
        raise ValueError("could not find product")
