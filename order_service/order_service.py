import json

from redis import StrictRedis
from flask import request
from flask import Flask

from ordershop.lib.event_store import EventStore, Event
from ordershop.lib.repository import Repository, Entity


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


@app.route('/orders', methods=['GET'])
@app.route('/orders', methods=['POST'])
@app.route('/order', methods=['POST'])
def no_params():

    # handle GET
    if request.method == 'GET':
        return json.dumps([item.__dict__ for item in repo.get_items()])

    # handle POST
    if request.method == 'POST':
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
                event = Event('ORDER', 'CREATED', **new_order.__dict__)
                store.save(event)
                store.publish(event)

                order_ids.append(str(new_order.id))
            else:
                raise ValueError("could not create order")

        return json.dumps({'status': 'ok',
                           'ids': order_ids})


@app.route('/order/<order_id>', methods=['GET'])
@app.route('/order/<order_id>', methods=['PUT'])
@app.route('/order/<order_id>', methods=['DELETE'])
def one_param(order_id):

    # handle GET
    if request.method == 'GET':
        order = repo.get_item(order_id)
        return json.dumps(order.__dict__) if order else json.dumps(False)

    # handle PUT
    if request.method == 'PUT':
        value = request.get_json()
        try:
            order = Order(value['product_ids'], value['customer_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids' and/or 'customer_id'")

        order.id = order_id
        if repo.set_item(order):

            # trigger event
            event = Event('ORDER', 'UPDATED', **order.__dict__)
            store.save(event)
            store.publish(event)

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not update order")

    # handle DELETE
    if request.method == 'DELETE':
        order = repo.del_item(order_id)
        if order:

            # trigger event
            event = Event('ORDER', 'DELETED', **order.__dict__)
            store.save(event)
            store.publish(event)

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not delete order")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
