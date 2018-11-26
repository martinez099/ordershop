import json

from redis import StrictRedis
from flask import request
from flask import Flask

from ordershop.lib.event_store import EventStore, Event
from ordershop.lib.repository import Repository, Entity


class Product(Entity):
    """
    Product Entity
    """

    def __init__(self, _name, _price, _amount=0):
        super(Product, self).__init__()
        self.name = _name
        self.price = _price
        self.amount = _amount

    def get_name(self):
        return self.name

    def get_price(self):
        return self.price

    def incr_amount(self, _amount):
        self.amount += _amount or 1

    def decr_amount(self, _amount):
        self.amount -= _amount or 1

    def get_amount(self):
        return self.amount


app = Flask(__name__)
redis = StrictRedis(decode_responses=True, host='redis')
repo = Repository()
store = EventStore(redis)


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def query(product_id=None):

    if product_id:
        product = repo.get_item(product_id)
        return json.dumps(product.__dict__) if product else json.dumps(False)
    else:
        return json.dumps([item.__dict__ for item in repo.get_items()])


@app.route('/products', methods=['POST'])
@app.route('/product', methods=['POST'])
@app.route('/product/<product_id>', methods=['PUT'])
@app.route('/product/<product_id>', methods=['DELETE'])
def command(product_id=None):

    # handle POST
    if request.method == 'POST':
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
                store.publish(Event('PRODUCT', 'CREATED', **new_product.__dict__))

                product_ids.append(str(new_product.id))
            else:
                raise ValueError("could not create product")

        return json.dumps({'status': 'ok',
                           'ids': product_ids})

    # handle PUT
    if request.methdo == 'PUT':
        value = request.get_json()
        try:
            product = Product(value['name'], value['price'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'price'")

        product.id = product_id
        if repo.set_item(product):

            # trigger event
            store.publish(Event('PRODUCT', 'UPDATED', **product.__dict__))

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not update product")

    # handle DELETE
    if request.method == 'DELETE':
        product = repo.del_item(product_id)
        if product:

            # trigger event
            store.publish(Event('PRODUCT', 'DELETED', **product.__dict__))

            return json.dumps({'status': 'ok'})
        else:
            raise ValueError("could not delete product")


@app.route('/clear', methods=['POST'])
def clear():

    # clear repo
    repo.reset()

    return json.dumps({'status': 'ok'})
