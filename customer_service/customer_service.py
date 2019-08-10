import atexit
import json
import uuid

from common.utils import log_info
from common.receivers import Receivers
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


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


def get_customers(_req, _mq):

    try:
        billing_id = json.loads(_req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('customer')])
        mq.send_rsp('customer-service', 'get-customers', rsp)
        return

    customer = store.find_one('customer', billing_id)
    if not customer:
        raise ValueError("could not find customer")

    return json.dumps(customer) if customer else json.dumps(False)


def post_customers(_req, _mq):

    customers = json.loads(_req)
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


def put_customer(_req, _mq):

    customer = json.loads(_req)

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


def delete_customer(_req, _mq):

    try:
        customer_id = json.loads(_req)['id']
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

rs = Receivers(mq, 'customer-service', [post_customers, get_customers, put_customer, delete_customer])

rs.start()
rs.wait()
