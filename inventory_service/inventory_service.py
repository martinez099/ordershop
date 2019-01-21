import atexit
import json
import os

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.event_store import EventStore, Event
from lib.repository import Repository, Entity


class Inventory(Entity):
    """
    Inventory Entity class.
    """

    def __init__(self, _product_id, _amount):
        super(Inventory, self).__init__()
        self.product_id = _product_id
        self.amount = _amount

    def get_product_id(self):
        return self.product_id

    def get_amount(self):
        return self.amount

    def decr_amount(self):
        self.amount -= 1

    def incr_amount(self):
        self.amount += 1


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EventStore(redis)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and hasattr(store, 'subscribe_to_product_events'):
    store.subscribe_to_inventory_events()
    atexit.register(store.unsubscribe_from_inventory_events)


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get(inventory_id=None):

    if inventory_id:
        item = repo.get_item(inventory_id)
        return json.dumps(item.__dict__) if item else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/inventory', methods=['POST'])
def post():

    values = request.get_json()

    if not isinstance(values, list):
        values = [values]

    inventory_ids = []
    for value in values:
        try:
            new_inventory = Inventory(value['product_id'], value['amount'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

        if repo.set_item(new_inventory):

            # trigger event
            store.publish(Event('inventory', 'created', **new_inventory.__dict__))

            inventory_ids.append(str(new_inventory.id))
        else:
            raise ValueError("could not create inventory")

    return json.dumps({'status': 'ok',
                       'ids': inventory_ids})


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def put(inventory_id=None):

    value = request.get_json()
    try:
        inventory = Inventory(value['product_id'], value['amount'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'price'")

    inventory.id = inventory_id
    if repo.set_item(inventory):

        # trigger event
        store.publish(Event('inventory', 'updated', **inventory.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update inventory")


@app.route('/product/<product_id>', methods=['DELETE'])
def delete(inventory_id=None):

    inventory = repo.del_item(inventory_id)
    if inventory:

        # trigger event
        store.publish(Event('inventory', 'deleted', **inventory.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not delete inventory")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
