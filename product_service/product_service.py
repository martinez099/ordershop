import atexit
import json
import os

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.event_store import EventStore, Event
from lib.repository import Repository, Entity


class Product(Entity):
    """
    Product Entity class.
    """

    def __init__(self, _name, _price):
        super(Product, self).__init__()
        self.name = _name
        self.price = _price

    def get_name(self):
        return self.name

    def get_price(self):
        return self.price


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EventStore(redis)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and hasattr(store, 'subscribe_to_product_events'):
    store.subscribe_to_product_events()
    atexit.register(store.unsubscribe_from_product_events)


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get(product_id=None):

    if product_id:
        product = repo.get_item(product_id)
        return json.dumps(product.__dict__) if product else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/products', methods=['POST'])
@app.route('/product', methods=['POST'])
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

        if repo.set_item(new_product):

            # trigger event
            store.publish(Event('product', 'created', **new_product.__dict__))

            product_ids.append(str(new_product.id))
        else:
            raise ValueError("could not create product")

    return json.dumps({'status': 'ok',
                       'ids': product_ids})


@app.route('/product/<product_id>', methods=['PUT'])
def put(product_id=None):

    value = request.get_json()
    try:
        product = Product(value['name'], value['price'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    product.id = product_id
    if repo.set_item(product):

        # trigger event
        store.publish(Event('product', 'updated', **product.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update product")


@app.route('/product/<product_id>', methods=['DELETE'])
def delete(product_id=None):

    product = repo.del_item(product_id)
    if product:

        # trigger event
        store.publish(Event('product', 'deleted', **product.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not delete product")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
