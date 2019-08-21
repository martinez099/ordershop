import json

from flask import request
from flask import Flask

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue, send_message


app = Flask(__name__)
store = EventStore()
mq = MessageQueue()


def proxy_command_request(service_name, func_name, add_params=None):
    """
    Helper function to proxy POST, PUT and DELETE requests to the according service.

    :param service_name: The name of the service to call.
    :param func_name: The name of the function to call.
    :param add_params: A dict with optional additional parameters.
    """

    params = {}
    if request.data:
        params = json.loads(request.data)

    if add_params:
        params.update(add_params)

    return send_message(mq, service_name, func_name, params)


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get_billings(billing_id=None):

    if billing_id:
        result = store.find_one('billing', billing_id)
    else:
        result = store.find_all('billing')
    return json.dumps(result)


@app.route('/billing', methods=['POST'])
@app.route('/billings', methods=['POST'])
def post_billings():

    return proxy_command_request('billing-service', 'post_billings')


@app.route('/billing/<billing_id>', methods=['PUT'])
def put_billing(billing_id):

    return proxy_command_request('billing-service', 'put_billing', {'id': billing_id})


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete_billing(billing_id):

    return proxy_command_request('billing-service', 'delete_billing', {'id': billing_id})


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def get_customers(customer_id=None):

    if customer_id:
        result = store.find_one('customer', customer_id)
    else:
        result = store.find_all('customer')
    return json.dumps(result)


@app.route('/customer', methods=['POST'])
@app.route('/customers', methods=['POST'])
def post_customers():

    return proxy_command_request('customer-service', 'post_customers')


@app.route('/customer/<customer_id>', methods=['PUT'])
def put_customer(customer_id):

    return proxy_command_request('customer-service', 'put_customer', {'id': customer_id})


@app.route('/customer/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):

    return proxy_command_request('customer-service', 'delete_customer', {'id': customer_id})


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get_products(product_id=None):

    if product_id:
        result = store.find_one('product', product_id)
    else:
        result = store.find_all('product')
    return json.dumps(result)


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
def post_products():

    return proxy_command_request('product-service', 'post_products')


@app.route('/product/<product_id>', methods=['PUT'])
def put_prodcut(product_id):

    return proxy_command_request('product-service', 'put_product', {'id': product_id})


@app.route('/product/<product_id>', methods=['DELETE'])
def del_prodcut(product_id):

    return proxy_command_request('product-service', 'delete_product', {'id': product_id})


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get_inventory(inventory_id=None):

    if inventory_id:
        result = store.find_one('inventory', inventory_id)
    else:
        result = store.find_all('inventory')
    return json.dumps(result)


@app.route('/inventory', methods=['POST'])
def post_inventory():

    return proxy_command_request('inventory-service', 'post_inventory')


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def put_inventory(inventory_id):

    return proxy_command_request('inventory-service', 'put_inventory', {'id': inventory_id})


@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def delete_inventory(inventory_id):

    return proxy_command_request('inventory-service', 'delete_inventory', {'id': inventory_id})


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
@app.route('/orders/unbilled', methods=['GET'])
def get_orders(order_id=None):

    # handle additional query 'unbilled orders'
    if request.path.endswith('/orders/unbilled'):
        return send_message(mq, 'order-service', 'get_unbilled')
    elif order_id:
        result = store.find_one('order', order_id)
    else:
        result = store.find_all('order')
    return json.dumps(result)


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
def post_orders():

    return proxy_command_request('order-service', 'post_orders')


@app.route('/order/<order_id>', methods=['PUT'])
def put_order(order_id):

    return proxy_command_request('order-service', 'put_order', {'id': order_id})


@app.route('/order/<order_id>', methods=['DELETE'])
def delete_order(order_id):

    return proxy_command_request('order-service', 'delete_order', {'id': order_id})


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
