import json

from flask import request
from flask import Flask

from message_queue.message_queue_client import send_message


app = Flask(__name__)


def proxy_request(service_name, func_name, add_params=None):
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

    return send_message(service_name, func_name, params)


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get_billings(billing_id=None):

    return proxy_request('billing-service', 'get_billings', {'entity_id': billing_id} if billing_id else None)


@app.route('/billing', methods=['POST'])
@app.route('/billings', methods=['POST'])
def post_billings():

    return proxy_request('billing-service', 'post_billings')


@app.route('/billing/<billing_id>', methods=['PUT'])
def put_billing(billing_id):

    return proxy_request('billing-service', 'put_billing', {'entity_id': billing_id})


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete_billing(billing_id):

    return proxy_request('billing-service', 'delete_billing', {'entity_id': billing_id})


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def get_customers(customer_id=None):

    return proxy_request('customer-service', 'get_customers', {'entity_id': customer_id} if customer_id else None)


@app.route('/customer', methods=['POST'])
@app.route('/customers', methods=['POST'])
def post_customers():

    return proxy_request('customer-service', 'post_customers')


@app.route('/customer/<customer_id>', methods=['PUT'])
def put_customer(customer_id):

    return proxy_request('customer-service', 'put_customer', {'entity_id': customer_id})


@app.route('/customer/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):

    return proxy_request('customer-service', 'delete_customer', {'entity_id': customer_id})


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get_products(product_id=None):

    return proxy_request('product-service', 'get_products', {'entity_id': product_id} if product_id else None)


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
def post_products():

    return proxy_request('product-service', 'post_products')


@app.route('/product/<product_id>', methods=['PUT'])
def put_prodcut(product_id):

    return proxy_request('product-service', 'put_product', {'entity_id': product_id})


@app.route('/product/<product_id>', methods=['DELETE'])
def del_prodcut(product_id):

    return proxy_request('product-service', 'delete_product', {'entity_id': product_id})


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get_inventory(inventory_id=None):

    return proxy_request('inventory-service', 'get_inventory', {'entity_id': inventory_id} if inventory_id else None)


@app.route('/inventory', methods=['POST'])
def post_inventory():

    return proxy_request('inventory-service', 'post_inventory')


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def put_inventory(inventory_id):

    return proxy_request('inventory-service', 'put_inventory', {'entity_id': inventory_id})


@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def delete_inventory(inventory_id):

    return proxy_request('inventory-service', 'delete_inventory', {'entity_id': inventory_id})


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get_orders(order_id=None):

    return proxy_request('order-service', 'get_orders', {'entity_id': order_id} if order_id else None)


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled_orders():

    return proxy_request('read-model', 'get_unbilled_orders')


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
def post_orders():

    return proxy_request('order-service', 'post_orders')


@app.route('/order/<order_id>', methods=['PUT'])
def put_order(order_id):

    return proxy_request('order-service', 'put_order', {'entity_id': order_id})


@app.route('/order/<order_id>', methods=['DELETE'])
def delete_order(order_id):

    return proxy_request('order-service', 'delete_order', {'entity_id': order_id})


@app.route('/report', methods=['GET'])
def report():

    return {
        "result": {
            "products": proxy_request('product-service', 'get_products'),
            "inventory": proxy_request('inventory-service', 'get_inventory'),
            "customers": proxy_request('customer-service', 'get_customers'),
            "orders": proxy_request('order-service', 'get_orders'),
            "billings": proxy_request('billing-service', 'get_billings')
        }
    }
