import atexit
import json
import os

from redis import StrictRedis
from flask import request
from flask import Flask

from lib.entity_cache import EntityCache
from lib.event_store import Event
from lib.repository import Repository, Entity


class Customer(Entity):
    """
    Customer Entity
    """

    def __init__(self, _name, _email):
        super(Customer, self).__init__()
        self.name = _name
        self.email = _email

    def get_name(self):
        return self.name

    def get_email(self):
        return self.email


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EntityCache(redis)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" and hasattr(store, 'subscribe_to_customer_events'):
    store.subscribe_to_customer_events()
    atexit.register(store.unsubscribe_from_customer_events)


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def get(customer_id=None):

    if customer_id:
        customer = repo.get_item(customer_id)
        return json.dumps(customer.__dict__) if customer else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/customer', methods=['POST'])
@app.route('/customers', methods=['POST'])
def post():

    values = request.get_json()

    if not isinstance(values, list):
        values = [values]

    customer_ids = []
    for value in values:
        try:
            new_customer = Customer(value['name'], value['email'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'email'")

        if repo.set_item(new_customer):

            # trigger event
            store.publish(Event('customer', 'created', **new_customer.__dict__))

            customer_ids.append(str(new_customer.id))
        else:
            raise ValueError("could not create customer")

    return json.dumps({'status': 'ok',
                       'ids': customer_ids})


@app.route('/customer/<customer_id>', methods=['PUT'])
def put(customer_id=None):

    value = request.get_json()
    try:
        customer = Customer(value['name'], value['email'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'email'")

    customer.id = customer_id
    if repo.set_item(customer):

        # trigger event
        store.publish(Event('customer', 'updated', **customer.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not update customer")


@app.route('/customer/<customer_id>', methods=['DELETE'])
def delete(customer_id=None):

    customer = repo.del_item(customer_id)
    if customer:

        # trigger event
        store.publish(Event('customer', 'deleted', **customer.__dict__))

        return json.dumps({'status': 'ok'})
    else:
        raise ValueError("could not delete customer")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
