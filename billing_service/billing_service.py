import atexit
import json
import time
import uuid

import gevent
from gevent import monkey
monkey.patch_all()

from common.utils import log_error, log_info, run_receiver, do_send
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


def create_billing(_order_id):
    """
    Create a billing entity.

    :param _order_id: The order ID the billing belongs to.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'order_id': _order_id,
        'done': time.time()
    }


def order_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        customer = store.find_one('customer', msg_data['customer_id'])
        products = [store.find_one('product', product_id) for product_id in msg_data['product_ids']]
        msg = """Dear {}!

Please transfer € {} with your favourite payment method.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        do_send('messaging-service', 'send-email', {
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def billing_created(item):
    try:
        msg_data = json.loads(item.event_entity)
        order = store.find_one('order', msg_data['order_id'])
        customer = store.find_one('customer', order['customer_id'])
        products = [store.find_one('product', product_id) for product_id in order['product_ids']]
        msg = """Dear {}!

We've just received € {} from you, thank you for your transfer.

Cheers""".format(customer['name'], sum([int(product['price']) for product in products]))

        do_send('messaging-service', 'send-email', {
            "to": customer['email'],
            "msg": msg
        })
    except Exception as e:
        log_error(e)


def subscribe_to_domain_events():
    store.subscribe('order', 'created', order_created)
    store.subscribe('billing', 'created', billing_created)
    log_info('subscribed to domain events')


def unsubscribe_from_domain_events():
    store.unsubscribe('order', 'created', order_created)
    store.unsubscribe('billing', 'created', billing_created)
    log_info('unsubscribed from domain events')


def get_billings(req):

    try:
        billing_id = json.loads(req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('billing')])
        mq.send_rsp('billing-service', 'get-billings', rsp)
        return

    billing = store.find_one('billing', billing_id)
    if not billing:
        raise ValueError("could not find billing")

    return json.dumps(billing) if billing else json.dumps(False)


def post_billings(req):

    billings = json.loads(req)
    if not isinstance(billings, list):
        billings = [billings]

    billing_ids = []
    for billing in billings:
        try:
            new_billing = create_billing(billing['order_id'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'order_id'")

        # trigger event
        store.publish('billing', 'created', **new_billing)

        billing_ids.append(new_billing['id'])

    return json.dumps(billing_ids)


def put_billing(req):

    billing = json.loads(req)

    try:
        billing = create_billing(billing['order_id'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'order_id'")

    try:
        billing_id = billing['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    billing['id'] = billing_id

    # trigger event
    store.publish('billing', 'updated', **billing)

    return json.dumps(True)


def delete_billing(req):

    try:
        billing_id = json.loads(req)['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    billing = store.find_one('billing', billing_id)
    if not billing:
        raise ValueError("could not find billing")

    # trigger event
    store.publish('billing', 'deleted', **billing)

    return json.dumps(True)


store = EventStore()
mq = MessageQueue()

store.activate_entity_cache('billing')
atexit.register(store.deactivate_entity_cache, 'billing')
subscribe_to_domain_events()
atexit.register(unsubscribe_from_domain_events)

gevent.joinall([
    gevent.spawn(run_receiver, mq, 'billing-service', 'get_billings', get_billings),
    gevent.spawn(run_receiver, mq, 'billing-service', 'post_billings', post_billings),
    gevent.spawn(run_receiver, mq, 'billing-service', 'put_billing', put_billing),
    gevent.spawn(run_receiver, mq, 'billing-service', 'delete_billing', delete_billing)
])
