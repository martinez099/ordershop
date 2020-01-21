import atexit
import functools
import logging
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Receivers


class InventoryService(object):
    """
    Inventory Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Receivers('inventory-service', [self.get_inventory,
                                                         self.post_inventory,
                                                         self.put_inventory,
                                                         self.delete_inventory,
                                                         self.incr_amount,
                                                         self.decr_amount,
                                                         self.decr_from_orders])
        self.inventory = deduce_entities(self.event_store.get('inventory'))
        self.tracking_handler = functools.partial(track_entities, self.inventory)

    @staticmethod
    def create_inventory(_product_id, _amount):
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
        self.event_store.subscribe('inventory', self.tracking_handler)
        atexit.register(self.stop)
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.event_store.unsubscribe('inventory', self.tracking_handler)
        self.receivers.stop()
        logging.info('stopped.')

    def get_inventory(self, _req):

        try:
            inventory_id = _req['entity_id']
        except KeyError:
            return {
                "result": list(self.inventory.values())
            }

        inventory = self.inventory.get(inventory_id)
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        return {
            "result": inventory
        }

    def post_inventory(self, _req):

        inventory = _req if isinstance(_req, list) else [_req]
        inventory_ids = []

        for inventory in inventory:
            try:
                new_inventory = InventoryService.create_inventory(inventory['product_id'], inventory['amount'])
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

    def put_inventory(self, _req):

        try:
            inventory = InventoryService.create_inventory(_req['product_id'], _req['amount'])
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

        inventory = self.inventory.get(inventory_id)
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        # trigger event
        self.event_store.publish('inventory', create_event('entity_deleted', inventory))

        return {
            "result": True
        }

    def incr_amount(self, _req):

        try:
            product_id = _req['product_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'product_id'"
            }

        inventory = list(filter(lambda x: x['product_id'] == product_id, self.inventory.values()))
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

    def decr_amount(self, _req):

        try:
            product_id = _req['product_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'product_id'"
            }

        inventory = list(filter(lambda x: x['product_id'] == product_id, self.inventory.values()))
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        value = _req.get('value')
        inventory = inventory[0]
        if int(inventory['amount']) - (value if value else 1) >= 0:

            inventory['amount'] = int(inventory['amount']) - (value if value else 1)

            # trigger event
            self.event_store.publish('inventory', create_event('entity_updated', inventory))

            return {
                "result": True
            }
        else:
            return {
                "error": "out of stock"
            }

    def decr_from_orders(self, _req):

        orders = _req if isinstance(_req, list) else [_req]

        occurs = {}
        inventories = self.inventory.values()

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
                if occurs[inventory['product_id']] <= int(inventory['amount']):
                    continue
                else:
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
            if int(inventory['amount']) - v >= 0:

                inventory['amount'] = int(inventory['amount']) - v

                # trigger event
                self.event_store.publish('inventory', create_event('entity_updated', inventory))

            else:
                return {
                    "error": "out of stock"
                }

        return {
            "result": True
        }


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)-6s] %(message)s')

i = InventoryService()
i.start()
