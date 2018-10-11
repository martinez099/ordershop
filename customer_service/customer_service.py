import json

from redis import StrictRedis
from flask import request
from flask import Flask

from ordershop.lib.event_store import EventStore, Event
from ordershop.lib.repository import Repository, Entity


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
store = EventStore(redis)


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def query(customer_id=None):

    if customer_id:
        customer = repo.get_item(customer_id)
        return json.dumps(customer.__dict__) if customer else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/customers', methods=['POST'])
@app.route('/customer', methods=['POST'])
@app.route('/customer/<customer_id>', methods=['PUT'])
@app.route('/customer/<customer_id>', methods=['DELETE'])
def command(customer_id=None):

    # handle POST
    if request.method == 'POST':
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
                store.publish(Event('CUSTOMER', 'CREATED', **new_customer.__dict__))

                customer_ids.append(str(new_customer.id))
            else:
                raise ValueError("could not create customer")

        return json.dumps({'status': 'ok',
                           'ids': customer_ids})

    # handle PUT
    if request.method == 'PUT':
        value = request.get_json()
        try:
            customer = Customer(value['name'], value['email'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'email'")

        customer.id = customer_id
        if repo.set_item(customer):

            # trigger event
            store.publish(Event('CUSTOMER', 'UPDATED', **customer.__dict__))

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not update customer")

    # handle DELETE
    if request.method == 'DELETE':
        customer = repo.del_item(customer_id)
        if customer:

            # trigger event
            store.publish(Event('CUSTOMER', 'DELETED', **customer.__dict__))

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not delete customer")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
