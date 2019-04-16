import json

import requests

from flask import request
from flask import Flask

from common.utils import check_rsp_code
from lib.event_store import EventStore


app = Flask(__name__)
store = EventStore()


def proxy_command_request(_base_url):
    """
    Helper function to proxy POST, PUT and DELETE requests to the according service.

    :param _base_url: The URL of the service.
    """

    # handle POST
    if request.method == 'POST':

        try:
            values = json.loads(request.data)
        except Exception:
            raise ValueError("cannot parse json body {}".format(request.data))

        rsp = requests.post(_base_url.format(request.full_path), json=values)
        return check_rsp_code(rsp)

    # handle PUT
    if request.method == 'PUT':

        try:
            values = json.loads(request.data)
        except Exception:
            raise ValueError("cannot parse json body {}".format(request.data))

        rsp = requests.put(_base_url.format(request.full_path), json=values)
        return check_rsp_code(rsp)

    # handle DELETE
    if request.method == 'DELETE':
        rsp = requests.delete(_base_url.format(request.full_path))
        return check_rsp_code(rsp)


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def billing_query(billing_id=None):

    if billing_id:
        result = store.find_one('billing', billing_id)
    else:
        result = store.find_all('billing')
    return json.dumps(result)


@app.route('/billing', methods=['POST'])
@app.route('/billings', methods=['POST'])
@app.route('/billing/<billing_id>', methods=['PUT'])
@app.route('/billing/<billing_id>', methods=['DELETE'])
def billing_command(billing_id=None):

    return proxy_command_request('http://billing-service:5000{}')


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def customer_query(customer_id=None):

    if customer_id:
        result = store.find_one('customer', customer_id)
    else:
        result = store.find_all('customer')
    return json.dumps(result)


@app.route('/customer', methods=['POST'])
@app.route('/customers', methods=['POST'])
@app.route('/customer/<customer_id>', methods=['PUT'])
@app.route('/customer/<customer_id>', methods=['DELETE'])
def customer_command(customer_id=None):

    return proxy_command_request('http://customer-service:5000{}')


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def product_query(product_id=None):

    if product_id:
        result = store.find_one('product', product_id)
    else:
        result = store.find_all('product')
    return json.dumps(result)


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
@app.route('/product/<product_id>', methods=['PUT'])
@app.route('/product/<product_id>', methods=['DELETE'])
def product_command(product_id=None):

    return proxy_command_request('http://product-service:5000{}')


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def inventory_query(inventory_id=None):

    if inventory_id:
        result = store.find_one('inventory', inventory_id)
    else:
        result = store.find_all('inventory')
    return json.dumps(result)


@app.route('/inventory', methods=['POST'])
@app.route('/inventory/<inventory_id>', methods=['PUT'])
@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def inventory_command(inventory_id=None):

    return proxy_command_request('http://inventory-service:5000{}')


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
@app.route('/orders/unbilled', methods=['GET'])
def order_query(order_id=None):

    # handle additional query 'unbilled orders'
    if request.path.endswith('/orders/unbilled'):
        rsp = requests.get('http://order-service:5000/orders/unbilled')
        check_rsp_code(rsp)
        return rsp.text
    elif order_id:
        result = store.find_one('order', order_id)
    else:
        result = store.find_all('order')
    return json.dumps(result)


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
@app.route('/order/<order_id>', methods=['PUT'])
@app.route('/order/<order_id>', methods=['DELETE'])
def order_command(order_id=None):

    return proxy_command_request('http://order-service:5000{}')


@app.route('/report', methods=['GET'])
def report():

    result = {
        "products": store.find_all('product'),
        "inventory": store.find_all('inventory'),
        "customers": store.find_all('customer'),
        "orders": store.find_all('order'),
        "billings": store.find_all('billing')
    }

    return json.dumps(result)
