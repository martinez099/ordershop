import atexit
import json
import os
import uuid

from flask import request
from flask import Flask

from event_store.event_store_client import EventStore


app = Flask(__name__)
store = EventStore()


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


if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.activate_entity_cache('product')
    atexit.register(store.deactivate_entity_cache, 'product')


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get(product_id=None):

    if product_id:
        product = store.find_one('product', product_id)
        if not product:
            raise ValueError("could not find product")

        return json.dumps(product) if product else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('product')])


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    product_ids = []
    for value in values:
        try:
            new_product = create_product(value['name'], value['price'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'price'")

        # trigger event
        store.publish('product', 'created', **new_product)

        product_ids.append(new_product['id'])

    return json.dumps(product_ids)


@app.route('/product/<product_id>', methods=['PUT'])
def put(product_id):

    value = request.get_json()
    try:
        product = create_product(value['name'], value['price'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    product['id'] = product_id

    # trigger event
    store.publish('product', 'updated', **product)

    return json.dumps(True)


@app.route('/product/<product_id>', methods=['DELETE'])
def delete(product_id):

    product = store.find_one('product', product_id)
    if product:

        # trigger event
        store.publish('product', 'deleted', **product)

        return json.dumps(True)
    else:
        raise ValueError("could not find product")
