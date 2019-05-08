import atexit
import json
import os
import uuid

from flask import request
from flask import Flask

from common.event_store import EventStore


app = Flask(__name__)
store = EventStore()


def create_inventory(_product_id, _amount):
    """
    Create an inventory entity.

    :param _product_id: The product ID the inventory is for.
    :param _amount: The amount of products in the inventory.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'product_id': _product_id,
        'amount': _amount
    }


if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    store.activate_entity_cache('inventory')
    atexit.register(store.deactivate_entity_cache, 'inventory')


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get(inventory_id=None):

    if inventory_id:
        inventory = store.find_one('inventory', inventory_id)
        if not inventory:
            raise ValueError("could not find inventory")

        return json.dumps(inventory) if inventory else json.dumps(False)
    else:
        return json.dumps([item for item in store.find_all('inventory')])


@app.route('/inventory', methods=['POST'])
def post():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    inventory_ids = []
    for value in values:
        try:
            new_inventory = create_inventory(value['product_id'], value['amount'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

        # trigger event
        store.publish('inventory', 'created', **new_inventory)

        inventory_ids.append(new_inventory['id'])

    return json.dumps(inventory_ids)


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def put(inventory_id):

    value = request.get_json()
    try:
        inventory = create_inventory(value['product_id'], value['amount'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    inventory['id'] = inventory_id

    # trigger event
    store.publish('inventory', 'updated', **inventory)

    return json.dumps(True)


@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def delete(inventory_id):

    inventory = store.find_one('inventory', inventory_id)
    if inventory:

        # trigger event
        store.publish('inventory', 'deleted', **inventory)

        return json.dumps(True)
    else:
        raise ValueError("could not find inventory")


@app.route('/incr/<product_id>', methods=['POST'])
@app.route('/incr/<product_id>/<value>', methods=['POST'])
def incr(product_id, value=None):

    inventory = list(filter(lambda x: x['product_id'] == product_id, store.find_all('inventory')))
    if not inventory:
        raise ValueError("could not find inventory")

    inventory = inventory[0]
    inventory['amount'] = int(inventory['amount']) - (value if value else 1)

    # trigger event
    store.publish('inventory', 'updated', **inventory)

    return json.dumps(True)


@app.route('/decr/<product_id>', methods=['POST'])
@app.route('/decr/<product_id>/<value>', methods=['POST'])
def decr(product_id, value=None):

    inventory = list(filter(lambda x: x['product_id'] == product_id, store.find_all('inventory')))
    if not inventory:
        raise ValueError("could not find inventory")

    inventory = inventory[0]
    if int(inventory['amount']) - (value if value else 1) >= 0:

        inventory['amount'] = int(inventory['amount']) - (value if value else 1)

        # trigger event
        store.publish('inventory', 'updated', **inventory)

        return json.dumps(True)
    else:
        return json.dumps(False)


@app.route('/decr_from_order', methods=['POST'])
def decr_from_order():

    values = request.get_json()
    if not isinstance(values, list):
        values = [values]

    occurs = {}
    for value in values:
        try:
            product_ids = value['product_ids']
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids'")

        for inventory in store.find_all('inventory'):

            if not inventory['product_id'] in occurs:
                occurs[inventory['product_id']] = 0

            occurs[inventory['product_id']] += product_ids.count(inventory['product_id'])
            if occurs[inventory['product_id']] <= int(inventory['amount']):
                continue
            else:
                return json.dumps(False)

    for k, v in occurs.items():
        inventory = list(filter(lambda x: x['product_id'] == k, store.find_all('inventory')))
        if not inventory:
            raise ValueError("could not find inventory")

        inventory = inventory[0]
        if int(inventory['amount']) - v >= 0:

            inventory['amount'] = int(inventory['amount']) - v

            # trigger event
            store.publish('inventory', 'updated', **inventory)

        else:
            return json.dumps(False)

    return json.dumps(True)
