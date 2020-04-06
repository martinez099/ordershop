import json
import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class InventoryService(object):
    """
    Inventory Service class.
    """
    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('inventory-service', [self.create_inventory,
                                                         self.update_inventory,
                                                         self.delete_inventory])

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
        self.event_store.subscribe('order', self.order_created)
        self.event_store.subscribe('order', self.order_deleted)
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.event_store.unsubscribe('order', self.order_created)
        self.event_store.unsubscribe('order', self.order_deleted)
        self.consumers.stop()
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
            inventory_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entities', {'name': 'inventory', 'id': inventory_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        inventory = rsp['result']
        if not inventory:
            return {
                "error": "could not find inventory"
            }

        # set new props
        inventory['entity_id'] = inventory_id
        try:
            inventory['product_id'] = _req['product_id']
            inventory['amount'] = _req['amount']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'product_id' and/or 'amount"
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

        rsp = send_message('read-model', 'get_entities', {'name': 'inventory', 'id': inventory_id})
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

    def incr_inventory(self, _product_id, _value=1):
        rsp = send_message('read-model', 'get_entities', {'name': 'inventory', 'props': {'product_id': _product_id}})
        if 'error' in rsp:
            raise Exception(rsp['error'] + ' (from read-model)')

        inventory = rsp['result']
        if not inventory:
            logging.error("could not find inventory for product {}".format(_product_id))
            return False

        inventory = inventory[0]
        inventory['amount'] = int(inventory['amount']) - (_value if _value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def decr_inventory(self, _product_id, _value=1):
        rsp = send_message('read-model', 'get_entities', {'name': 'inventory', 'props': {'product_id': _product_id}})
        if 'error' in rsp:
            raise Exception(rsp['error'] + ' (from read-model)')

        inventory = rsp['result']
        if not inventory:
            logging.warning("could not find inventory for product {}".format(_product_id))
            return False

        inventory = inventory[0]
        if int(inventory['amount']) - (_value if _value else 1) < 0:
            logging.info("product {} is out of stock".format(_product_id))
            return False

        inventory['amount'] = int(inventory['amount']) - (_value if _value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def decr_from_cart(self, _cart):
        rsp = send_message('read-model', 'get_entities', {'name': 'inventory', 'props': {'ids': _cart['product_ids']}})
        if 'error' in rsp:
            raise Exception(rsp['error'] + ' (from read-model)')

        inventories = rsp['result']

        try:
            product_ids = _cart['product_ids']
        except KeyError:
            raise Exception("missing mandatory parameter 'product_ids'")

        # count products
        counts = []
        for inventory in inventories:
            found = product_ids.count(inventory['product_id'])

            # check amount
            if found > int(inventory['amount']):
                logging.info("product {} is out of stock".format(inventory['product_id']))
                return False

            counts.append((inventory, found))

        # decrement inventory
        for inventory, count in counts:
            inventory['amount'] = int(inventory['amount']) - count

            # trigger event
            self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def order_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        order = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entities', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        result = self.decr_from_cart(cart)
        order['status'] = 'IN_STOCK' if result else 'OUT_OF_STOCK'
        self.event_store.publish('order', create_event('entity_updated', order))

    def order_deleted(self, _item):
        if _item.event_action != 'entity_deleted':
            return

        order = json.loads(_item.event_data)
        if order['status'] != 'IN_STOCK':
            return

        rsp = send_message('read-model', 'get_entities', {'name': 'cart', 'id': order['cart_id']})
        cart = rsp['result']
        [self.incr_inventory(product_id) for product_id in cart['product_ids']]


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

i = InventoryService()

signal.signal(signal.SIGINT, lambda n, h: i.stop())
signal.signal(signal.SIGTERM, lambda n, h: i.stop())

i.start()
