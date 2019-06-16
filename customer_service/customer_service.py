import atexit
import json
import uuid

import gevent
from gevent import monkey
monkey.patch_all()

from common.utils import log_error, log_info, run_receiver, do_send
from event_store.event_store_client import EventStore
from message_queue.messasge_queue_client import MessageQueue


def create_customer(_name, _email):
    """
    Create a customer entity.

    :param _name: The name of the customer.
    :param _email: The em2ail address of the customer.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'name': _name,
        'email': _email
    }


def get_customers(req):

    try:
        billing_id = json.loads(req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('customer')])
        mq.send_rsp('customer-service', 'get-customers', rsp)
        return

    customer = store.find_one('customer', billing_id)
    if not customer:
        raise ValueError("could not find customer")

    return json.dumps(customer) if customer else json.dumps(False)


def post_customers(req):

    customers = json.loads(req)
    if not isinstance(customers, list):
        customers = [customers]

    customer_ids = []
    for customer in customers:
        try:
            new_customer = create_customer(customer['name'], customer['email'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'email'")

        # trigger event
        store.publish('customer', 'created', **new_customer)

        customer_ids.append(new_customer['id'])

    return json.dumps(customer_ids)


def put_customer(req):

    customer = json.loads(req)

    try:
        customer = create_customer(customer['name'], customer['email'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'name' and/or 'email'")
    
    try:
        customer_id = customer['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    customer['id'] = customer_id

    # trigger event
    store.publish('customer', 'updated', **customer)

    return json.dumps(True)


def delete_customer(req):

    try:
        customer_id = json.loads(req)['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    customer = store.find_one('customer', customer_id)
    if not customer:
        raise ValueError("could not find customer")

    # trigger event
    store.publish('customer', 'deleted', **customer)

    return json.dumps(True)


store = EventStore()
mq = MessageQueue()

store.activate_entity_cache('customer')
atexit.register(store.deactivate_entity_cache, 'customer')

gevent.joinall([
    gevent.spawn(run_receiver, mq, 'customer-service', 'get_customers', get_customers),
    gevent.spawn(run_receiver, mq, 'customer-service', 'post_customers', post_customers),
    gevent.spawn(run_receiver, mq, 'customer-service', 'put_customer', put_customer),
    gevent.spawn(run_receiver, mq, 'customer-service', 'delete_customer', delete_customer)
])