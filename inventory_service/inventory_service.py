import atexit
import json
import uuid


from common.utils import log_info
from common.receivers import Receivers
from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue


def create_inventory(_product_id, _amount):
    """
    Create an inventory entity.

    :param _product_id: The product ID the inventory is for.
    :param _amount: The amount of products in the inventory.
    :return: A dict with the entity properties.
    """
    return {
        'id': str(uuid.uuid4()),
        'product_id': _product_id,
        'amount': _amount
    }


def get_inventory(_req, _mq):

    try:
        billing_id = json.loads(_req)['id']
    except KeyError:
        rsp = json.dumps([item for item in store.find_all('inventory')])
        mq.send_rsp('inventory-service', 'get-inventory', rsp)
        return

    inventory = store.find_one('inventory', billing_id)
    if not inventory:
        raise ValueError("could not find inventory")

    return json.dumps(inventory) if inventory else json.dumps(False)


def post_inventory(_req, _mq):

    inventory = json.loads(_req)
    if not isinstance(inventory, list):
        inventory = [inventory]

    inventory_ids = []
    for inventory in inventory:
        try:
            new_inventory = create_inventory(inventory['product_id'], inventory['amount'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

        # trigger event
        store.publish('inventory', 'created', **new_inventory)

        inventory_ids.append(new_inventory['id'])

    return json.dumps(inventory_ids)


def put_inventory(_req, _mq):
    inventory = json.loads(_req)

    try:
        inventory = create_inventory(inventory['product_id'], inventory['amount'])
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

    try:
        inventory_id = inventory['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    inventory['id'] = inventory_id

    # trigger event
    store.publish('inventory', 'updated', **inventory)

    return json.dumps(True)


def delete_inventory(_req, _mq):

    try:
        inventory_id = json.loads(_req)['id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'id'")

    inventory = store.find_one('inventory', inventory_id)
    if not inventory:
        raise ValueError("could not find inventory")

    # trigger event
    store.publish('inventory', 'deleted', **inventory)

    return json.dumps(True)


def incr_amount(_req, _mq):
    
    params = json.loads(_req)
    try:
        product_id = params['product_id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_id'")

    inventory = list(filter(lambda x: x['product_id'] == product_id, store.find_all('inventory')))
    if not inventory:
        raise ValueError("could not find inventory")

    value = params.get('value')
    inventory = inventory[0]
    inventory['amount'] = int(inventory['amount']) - (value if value else 1)

    # trigger event
    store.publish('inventory', 'updated', **inventory)

    return json.dumps(True)


def decr_amount(_req, _mq):
    
    params = json.loads(_req)
    try:
        product_id = params['product_id']
    except KeyError:
        raise ValueError("missing mandatory parameter 'product_id'")

    inventory = list(filter(lambda x: x['product_id'] == product_id, store.find_all('inventory')))
    if not inventory:
        raise ValueError("could not find inventory")

    value = params.get('value')
    inventory = inventory[0]
    if int(inventory['amount']) - (value if value else 1) >= 0:

        inventory['amount'] = int(inventory['amount']) - (value if value else 1)

        # trigger event
        store.publish('inventory', 'updated', **inventory)

        return json.dumps(True)
    else:
        return json.dumps(False)


def decr_from_order(_req, _mq):

    values = json.loads(_req)
    if not isinstance(values, list):
        values = [values]

    occurs = {}
    for value in values:
        try:
            product_ids = value['product_ids']
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_ids'")

        for inventory in store.find_all('inventory'):

            if not inventory['product_id'] in occurs:
                occurs[inventory['product_id']] = 0

            # check amount
            occurs[inventory['product_id']] += product_ids.count(inventory['product_id'])
            if occurs[inventory['product_id']] <= int(inventory['amount']):
                continue
            else:
                return json.dumps(False)

    for k, v in occurs.items():
        inventory = list(filter(lambda x: x['product_id'] == k, store.find_all('inventory')))
        if not inventory:
            raise ValueError("could not find inventory")

        inventory = inventory[0]
        if int(inventory['amount']) - v >= 0:

            inventory['amount'] = int(inventory['amount']) - v

            # trigger event
            store.publish('inventory', 'updated', **inventory)

        else:
            return json.dumps(False)

    return json.dumps(True)


store = EventStore()
mq = MessageQueue()

store.activate_entity_cache('inventory')
atexit.register(store.deactivate_entity_cache, 'inventory')

rs = Receivers(mq, 'inventory-service', [get_inventory,
                                         post_inventory,
                                         put_inventory,
                                         delete_inventory,
                                         incr_amount,
                                         decr_amount,
                                         decr_from_order])

rs.start()
rs.wait()
