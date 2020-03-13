import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Consumers, send_message


class InventoryService(object):
    """
    Inventory Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Consumers('inventory-service', [self.create_inventory,
                                                         self.update_inventory,
                                                         self.delete_inventory,
                                                         self.incr_inventory,
                                                         self.decr_inventory,
                                                         self.decr_from_orders])

    @staticmethod
    def _create_entity(_product_id, _amount):
        """
        Create an inventory entity.

        :param _product_id: The product ID the inventory is for.
        :param _amount: The amount of products in the inventory.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'product_id': _product_id,
            'amount': _amount
        }

    def start(self):
        logging.info('starting ...')
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.receivers.stop()
        logging.info('stopped.')

    def create_inventory(self, _req):

        inventory = _req if isinstance(_req, list) else [_req]
        inventory_ids = []

        for inventory in inventory:
            try:
                new_inventory = InventoryService._create_entity(inventory['product_id'], inventory['amount'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'product_id' and/or 'amount'"
                }

            # trigger event
            self.event_store.publish('inventory', create_event('entity_created', new_inventory))

            inventory_ids.append(new_inventory['entity_id'])

        return {
            "result": inventory_ids
        }

    def update_inventory(self, _req):

        try:
            inventory = InventoryService._create_entity(_req['product_id'], _req['amount'])
        except KeyError:
            return {
                "error": "missing mandatory parameter 'product_id' and/or 'amount'"
            }

        try:
            inventory['entity_id'] = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return {
            "result": True
        }

    def delete_inventory(self, _req):

        try:
            inventory_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'inventory', 'id': inventory_id})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_one_entity',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        inventory = rsp['result']
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        # trigger event
        self.event_store.publish('inventory', create_event('entity_deleted', inventory))

        return {
            "result": True
        }

    def incr_inventory(self, _req):

        try:
            product_id = _req['product_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'product_id'"
            }

        try:
            rsp = send_message('read-model', 'get_all_entities', {'name': 'inventory'})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_all_entities',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        inventory = rsp['result']

        inventory = list(filter(lambda x: x['product_id'] == product_id, inventory.values()))
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        inventory = inventory[0]
        value = _req.get('value')
        inventory['amount'] = int(inventory['amount']) - (value if value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return {
            "result": True
        }

    def decr_inventory(self, _req):

        try:
            product_id = _req['product_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'product_id'"
            }

        try:
            rsp = send_message('read-model', 'get_all_entities', {'name': 'inventory'})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_all_entities',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        inventory = list(filter(lambda x: x['product_id'] == product_id, rsp['result'].values()))
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        value = _req.get('value')
        inventory = inventory[0]
        if int(inventory['amount']) - (value if value else 1) < 0:
            return {
                "error": "out of stock"
            }

        inventory['amount'] = int(inventory['amount']) - (value if value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return {
            "result": True
        }

    def decr_from_orders(self, _req):

        orders = _req if isinstance(_req, list) else [_req]

        occurs = {}

        try:
            rsp = send_message('read-model', 'get_all_entities', {'name': 'inventory'})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_all_entities',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        inventories = rsp['result'].values()

        for order in orders:
            try:
                product_ids = order['product_ids']
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'product_ids'"
                }

            for inventory in inventories:

                if not inventory['product_id'] in occurs:
                    occurs[inventory['product_id']] = 0

                # check amount
                occurs[inventory['product_id']] += product_ids.count(inventory['product_id'])
                if occurs[inventory['product_id']] > int(inventory['amount']):
                    return {
                        "error": "out of stock"
                    }

        for k, v in occurs.items():
            inventory = list(filter(lambda x: x['product_id'] == k, inventories))
            if not inventory:
                return {
                    "error": "could not find inventory"
                }

            inventory = inventory[0]
            if int(inventory['amount']) - v < 0:
                return {
                    "error": "out of stock"
                }

            inventory['amount'] = int(inventory['amount']) - v

            # trigger event
            self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

i = InventoryService()

signal.signal(signal.SIGINT, lambda n, h: i.stop())
signal.signal(signal.SIGTERM, lambda n, h: i.stop())

i.start()
