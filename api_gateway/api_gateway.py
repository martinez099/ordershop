import functools
import json
import logging

from flask import Flask, request, render_template
from flask_socketio import SocketIO, send, emit

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import send_message, send_message_async


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'
socketio = SocketIO(app)

logging.basicConfig(level=logging.ERROR)

event_store = EventStoreClient()


def _send_message(_service_name, _func_name, _add_params=None, _async=False):
    """
    Helper function to send a message to a service.

    :param _service_name: The name of the service to call.
    :param _func_name: The name of the function to call.
    :param _add_params: A dict with optional additional parameters.
    :param _async: Boolean indicating asynchronous communication.
    :return: A dict with the result response, or a message ID if :param _async: is True.
    """
    params = {}
    if request.data:
        params = json.loads(request.data)

    if _add_params:
        params.update(_add_params)

    if _async:
        return {
            "result": send_message_async(_service_name, _func_name, params)
        }

    return send_message(_service_name, _func_name, params)


def _read_model(_entitiy_name, _entity_id=None):
    """
    Helper function to perform a request to the read model.

    :param _entitiy_name: The entity name, i.e. event topic.
    :param _entity_id: An optional entitiy_id.
    :return: A dict with the result response.
    """
    params = {'name': _entitiy_name}

    if not _entity_id:
        return _send_message('read-model', 'get_entities', params)

    params['id'] = _entity_id

    return _send_message('read-model', 'get_entity', params)


def _emit_event(_name, _event):
    """
    Send domain event to WebSocket clients.

    :param _name: The event name.
    :param _event: The event.
    """
    event = {
        'action': _event.event_action.replace('entity', _name),
        'data': _event.event_data,
        'ts': _event.event_ts
    }
    socketio.emit('entity_event', json.dumps(event))


@app.route('/', methods=['GET'])
def get():

    return render_template('index.html')


@app.route('/billings', methods=['GET'])
@app.route('/billing/<billing_id>', methods=['GET'])
def get_billings(billing_id=None):

    return _read_model('billing', billing_id)


@app.route('/billing', methods=['POST'])
def create_billing():

    return _send_message('billing-service', 'create_billings')


@app.route('/billings', methods=['POST'])
def create_billings():

    return _send_message('billing-service', 'create_billings', _async=True)


@app.route('/billing/<billing_id>', methods=['PUT'])
def update_billing(billing_id):

    return _send_message('billing-service', 'update_billing', {'entity_id': billing_id})


@app.route('/billing/<billing_id>', methods=['DELETE'])
def delete_billing(billing_id):

    return _send_message('billing-service', 'delete_billing', {'entity_id': billing_id})


@app.route('/carts', methods=['GET'])
@app.route('/cart/<cart_id>', methods=['GET'])
def get_carts(cart_id=None):

    return _read_model('cart', cart_id)


@app.route('/cart', methods=['POST'])
def create_cart():

    return _send_message('cart-service', 'create_carts')


@app.route('/carts', methods=['POST'])
def create_carts():

    return _send_message('cart-service', 'create_carts', _async=True)


@app.route('/cart/<cart_id>', methods=['PUT'])
def update_cart(cart_id):

    return _send_message('cart-service', 'update_cart', {'entity_id': cart_id})


@app.route('/cart/<cart_id>', methods=['DELETE'])
def delete_cart(cart_id):

    return _send_message('cart-service', 'delete_cart', {'entity_id': cart_id})


@app.route('/customers', methods=['GET'])
@app.route('/customer/<customer_id>', methods=['GET'])
def get_customers(customer_id=None):

    return _read_model('customer', customer_id)


@app.route('/customer', methods=['POST'])
def create_customer():

    return _send_message('customer-service', 'create_customers')


@app.route('/customers', methods=['POST'])
def create_customers():

    return _send_message('customer-service', 'create_customers', _async=True)


@app.route('/customer/<customer_id>', methods=['PUT'])
def update_customer(customer_id):

    return _send_message('customer-service', 'update_customer', {'entity_id': customer_id})


@app.route('/customer/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):

    return _send_message('customer-service', 'delete_customer', {'entity_id': customer_id})


@app.route('/inventories', methods=['GET'])
@app.route('/inventory/<inventory_id>', methods=['GET'])
def get_inventory(inventory_id=None):

    return _read_model('inventory', inventory_id)


@app.route('/inventory', methods=['POST'])
def create_inventory():

    return _send_message('inventory-service', 'create_inventories')


@app.route('/inventories', methods=['POST'])
def create_inventories():

    return _send_message('inventory-service', 'create_inventories', _async=True)


@app.route('/inventory/<inventory_id>', methods=['PUT'])
def update_inventory(inventory_id):

    return _send_message('inventory-service', 'update_inventory', {'entity_id': inventory_id})


@app.route('/inventory/<inventory_id>', methods=['DELETE'])
def delete_inventory(inventory_id):

    return _send_message('inventory-service', 'delete_inventory', {'entity_id': inventory_id})


@app.route('/orders', methods=['GET'])
@app.route('/order/<order_id>', methods=['GET'])
def get_orders(order_id=None):

    return _read_model('order', order_id)


@app.route('/orders/unbilled', methods=['GET'])
def get_unbilled_orders():

    return _send_message('read-model', 'get_unbilled_orders')


@app.route('/orders/unshipped', methods=['GET'])
def get_unshipped_orders():

    return _send_message('read-model', 'get_unshipped_orders')


@app.route('/orders/delivered', methods=['GET'])
def get_delivered_orders():

    return _send_message('read-model', 'get_delivered_orders')


@app.route('/order', methods=['POST'])
def create_order():

    return _send_message('order-service', 'create_orders')


@app.route('/orders', methods=['POST'])
def create_orders():

    return _send_message('order-service', 'create_orders', _async=True)


@app.route('/order/<order_id>', methods=['PUT'])
def update_order(order_id):

    return _send_message('order-service', 'update_order', {'entity_id': order_id})


@app.route('/order/<order_id>', methods=['DELETE'])
def delete_order(order_id):

    return _send_message('order-service', 'delete_order', {'entity_id': order_id})


@app.route('/products', methods=['GET'])
@app.route('/product/<product_id>', methods=['GET'])
def get_products(product_id=None):

    return _read_model('product', product_id)


@app.route('/product', methods=['POST'])
def create_product():

    return _send_message('product-service', 'create_products')


@app.route('/products', methods=['POST'])
def create_products():

    return _send_message('product-service', 'create_products', _async=True)


@app.route('/product/<product_id>', methods=['PUT'])
def put_prodcut(product_id):

    return _send_message('product-service', 'update_product', {'entity_id': product_id})


@app.route('/product/<product_id>', methods=['DELETE'])
def del_prodcut(product_id):

    return _send_message('product-service', 'delete_product', {'entity_id': product_id})


@app.route('/shippings', methods=['GET'])
@app.route('/shipping/<shipping_id>', methods=['GET'])
def get_shippings(shipping_id=None):

    return _read_model('shipping', shipping_id)


@app.route('/shipping', methods=['POST'])
def create_shipping():

    return _send_message('shipping-service', 'create_shippings')


@app.route('/shippings', methods=['POST'])
def create_shippings():

    return _send_message('shipping-service', 'create_shippings', _async=True)


@app.route('/shipping/<shipping_id>', methods=['PUT'])
def update_shipping(shipping_id):

    return _send_message('shipping-service', 'update_shipping', {'entity_id': shipping_id})


@app.route('/shipping/<shipping_id>', methods=['DELETE'])
def delete_shipping(shipping_id):

    return _send_message('shipping-service', 'delete_shipping', {'entity_id': shipping_id})


@app.route('/mails/sent', methods=['GET'])
def get_sent_mails():

    return _send_message('read-model', 'get_mails')


@app.route('/report', methods=['GET'])
def get_report():
    return {
        "result": {
            "billings": _read_model('billing')['result'],
            "carts": _read_model('cart')['result'],
            "customers": _read_model('customer')['result'],
            "inventory": _read_model('inventory')['result'],
            "orders": _read_model('order')['result'],
            "products": _read_model('product')['result'],
            "shippings": _read_model('shipping')['result'],
            "mails": _send_message('read-model', 'get_mails')['result'],
        }
    }


@app.route('/report/orders', methods=['GET'])
def get_order_report():
    rsp = send_message('read-model', 'get_entities', {'name': 'order'})
    orders = rsp['result']
    for order in orders:
        rsp = send_message('read-model', 'get_entity', {'name': 'cart', 'id': order['cart_id']})
        order['cart'] = rsp['result']

        rsp = send_message('read-model', 'get_entity', {'name': 'customer', 'id': order['cart']['customer_id']})
        order['cart']['customer'] = rsp['result']

        rsp = send_message('read-model', 'get_entities', {'name': 'product', 'ids': order['cart']['product_ids']})
        order['cart']['products'] = rsp['result']

    return {
        "result": orders
    }


@socketio.on('connect')
def on_connect():
    app.logger.info('WS client connected')


@socketio.on('disconnect')
def on_disconnect():
    app.logger.info('WS client disconnected')


@socketio.on('stop')
def on_stop():
    socketio.stop()
    app.logger.info('FlaskIO server stopped')


# subscribe to domain events and forward each event to websocket clients
[event_store.subscribe(topic, functools.partial(_emit_event, topic)) for topic in ['billing',
                                                                                   'cart',
                                                                                   'customer',
                                                                                   'inventory',
                                                                                   'order',
                                                                                   'product',
                                                                                   'shipping',
                                                                                   'mail']]


DEBUG = True
HOST = '0.0.0.0'

if __name__ == "__main__":
    socketio.run(app, host=HOST, debug=DEBUG)
