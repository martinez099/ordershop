import json

from flask import request
from flask import Flask

from message_queue.message_queue_client import send_message


app = Flask(__name__)


def proxy_write_request(service_name, func_name, add_params=None):
    """
    Helper function to proxy POST, PUT and DELETE requests to the according service.

    :param service_name: The name of the service to call.
    :param func_name: The name of the function to call.
    :param add_params: A dict with optional additional parameters.
    :return: A dict with the result response.
    """

    params = {}
    if request.data:
        params = json.loads(request.data)

    if add_params:
        params.update(add_params)

    return send_message(service_name, func_name, params)


def proxy_read_request(entitiy_name, entity_id=None):
    """
    Helper function to proxy GET requests to the read model.

    :param entitiy_name: The entity name, i.e. event topic.
    :param entity_id: An optional entitiy_id.
    :return: A dict with the result response.
    """

    if entity_id:
        return proxy_write_request('read-model', 'get_one_entity', {'name': entitiy_name, 'id': entity_id})
    else:
        entities = proxy_write_request('read-model', 'get_all_entities', {'name': entitiy_name})
        return {
            'result': list(entities['result'].values())
        }


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get_billings(billing_id=None):

    return proxy_read_request('billing', billing_id)


@app.route('/billing', methods=['POST'])
@app.route('/billings', methods=['POST'])
def create_billings():

    return proxy_write_request('billing-service', 'create_billings')


@app.route('/billing/<billing_id>', methods=['PUT'])
def update_billing(billing_id):

    return proxy_write_request('billing-service', 'update_billing', {'entity_id': billing_id})


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete_billing(billing_id):

    return proxy_write_request('billing-service', 'delete_billing', {'entity_id': billing_id})


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def get_customers(customer_id=None):

    return proxy_read_request('customer', customer_id)


@app.route('/customer', methods=['POST'])
@app.route('/customers', methods=['POST'])
def create_customers():

    return proxy_write_request('customer-service', 'create_customers')


@app.route('/customer/<customer_id>', methods=['PUT'])
def update_customer(customer_id):

    return proxy_write_request('customer-service', 'update_customer', {'entity_id': customer_id})


@app.route('/customer/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):

    return proxy_write_request('customer-service', 'delete_customer', {'entity_id': customer_id})


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get_products(product_id=None):

    return proxy_read_request('product', product_id)


@app.route('/product', methods=['POST'])
@app.route('/products', methods=['POST'])
def create_products():

    return proxy_write_request('product-service', 'create_products')


@app.route('/product/<product_id>', methods=['PUT'])
def put_prodcut(product_id):

    return proxy_write_request('product-service', 'update_product', {'entity_id': product_id})


@app.route('/product/<product_id>', methods=['DELETE'])
def del_prodcut(product_id):

    return proxy_write_request('product-service', 'delete_product', {'entity_id': product_id})


@app.route('/inventory', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get_inventory(inventory_id=None):

    return proxy_read_request('inventory', inventory_id)


@app.route('/inventory', methods=['POST'])
def create_inventory():

    return proxy_write_request('inventory-service', 'create_inventory')


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def update_inventory(inventory_id):

    return proxy_write_request('inventory-service', 'update_inventory', {'entity_id': inventory_id})


@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def delete_inventory(inventory_id):

    return proxy_write_request('inventory-service', 'delete_inventory', {'entity_id': inventory_id})


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get_orders(order_id=None):

    return proxy_read_request('order', order_id)


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled_orders():

    return proxy_write_request('read-model', 'get_unbilled_orders')


@app.route('/order', methods=['POST'])
@app.route('/orders', methods=['POST'])
def create_orders():

    return proxy_write_request('order-service', 'create_orders')


@app.route('/order/<order_id>', methods=['PUT'])
def update_order(order_id):

    return proxy_write_request('order-service', 'update_order', {'entity_id': order_id})


@app.route('/order/<order_id>', methods=['DELETE'])
def delete_order(order_id):

    return proxy_write_request('order-service', 'delete_order', {'entity_id': order_id})


@app.route('/report', methods=['GET'])
def report():

    return {
        "result": {
            "products": proxy_read_request('product'),
            "inventory": proxy_read_request('inventory'),
            "customers": proxy_read_request('customer'),
            "orders": proxy_read_request('order'),
            "billings": proxy_read_request('billing')
        }
    }
