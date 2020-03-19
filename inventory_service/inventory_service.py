import json
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
        self.subscribe_to_domain_events()
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.unsubscribe_from_domain_events()
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

        rsp = send_message('read-model', 'get_one_entity', {'name': 'inventory', 'id': inventory_id})
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

        rsp = send_message('read-model', 'get_one_entity', {'name': 'inventory', 'id': inventory_id})
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

        rsp = send_message('read-model',
                           'get_spec_entities',
                           {'name': 'inventory', 'props': {'product_id': _product_id}})
        if 'error' in rsp:
            logging.error(rsp['error'] + ' (from read-model)')
            return False

        inventory = list(rsp['result'].values())
        if not inventory:
            logging.error("could not find inventory")
            return False

        inventory = inventory[0]
        inventory['amount'] = int(inventory['amount']) - (_value if _value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def decr_inventory(self, _product_id, _value=1):

        rsp = send_message('read-model',
                           'get_spec_entities',
                           {'name': 'inventory', 'props': {'product_id': _product_id}})
        if 'error' in rsp:
            logging.error(rsp['error'] + ' (from read-model)')
            return False

        inventory = list(rsp['result'].values())
        if not inventory:
            logging.error("could not find inventory")
            return False

        inventory = inventory[0]
        if int(inventory['amount']) - (_value if _value else 1) < 0:
            logging.info("out of stock")
            return False

        inventory['amount'] = int(inventory['amount']) - (_value if _value else 1)

        # trigger event
        self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def decr_from_carts(self, _orders):

        orders = _orders if isinstance(_orders, list) else [_orders]

        rsp = send_message('read-model', 'get_all_entities', {'name': 'inventory'})
        if 'error' in rsp:
            logging.error(rsp['error'] + ' (from read-model)')
            return False

        inventories = rsp['result'].values()

        occurs = {}

        # calc amount
        for order in orders:
            try:
                product_ids = order['product_ids']
            except KeyError:
                logging.error("missing mandatory parameter 'product_ids'")
                return False

            for inventory in inventories:

                if not inventory['product_id'] in occurs:
                    occurs[inventory['product_id']] = 0

                occurs[inventory['product_id']] += product_ids.count(inventory['product_id'])
                if occurs[inventory['product_id']] > int(inventory['amount']):
                    logging.info("out of stock")
                    return False

        # check amount
        for product_id, amount in occurs.items():
            inventory = list(filter(lambda x: x['product_id'] == product_id, inventories))
            if not inventory:
                logging.error("could not find inventory")
                return False

            inventory = inventory[0]
            if int(inventory['amount']) - amount < 0:
                logging.info("out of stock")
                return False

            inventory['amount'] = int(inventory['amount']) - amount

            # trigger event
            self.event_store.publish('inventory', create_event('entity_updated', inventory))

        return True

    def cart_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        try:
            cart = json.loads(_item.event_data)
            result = self.decr_from_carts(cart)
            # TODO handle error
        except Exception as e:
            logging.error(f'cart_created error: {e}')

    def cart_updated(self, _item):
        if _item.event_action != 'entity_updated':
            return

        try:
            new_cart = json.loads(_item.event_data)
            rsp = send_message('read-model',
                               'get_one_entity',
                               {'name': 'cart', 'id': new_cart['entity_id']})
            old_cart = rsp['result']
            results = [self.incr_inventory(product_id) for product_id in old_cart['product_ids']]
            # TODO handle errors
            result = self.decr_from_carts(new_cart)
            # TODO handle error
        except Exception as e:
            logging.error(f'cart_updated error: {e}')

    def cart_deleted(self, _item):
        if _item.event_action != 'entity_deleted':
            return

        try:
            cart = json.loads(_item.event_data)
            product_ids = cart['product_ids']
            results = [self.incr_inventory(product_id) for product_id in product_ids]
            # TODO handle errors
        except Exception as e:
            logging.error(f'cart_deleted error: {e}')

    def subscribe_to_domain_events(self):
        self.event_store.subscribe('cart', self.cart_created)
        self.event_store.subscribe('cart', self.cart_updated)
        self.event_store.subscribe('cart', self.cart_deleted)

    def unsubscribe_from_domain_events(self):
        self.event_store.unsubscribe('cart', self.cart_created)
        self.event_store.unsubscribe('cart', self.cart_updated)
        self.event_store.unsubscribe('cart', self.cart_deleted)


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

i = InventoryService()

signal.signal(signal.SIGINT, lambda n, h: i.stop())
signal.signal(signal.SIGTERM, lambda n, h: i.stop())

i.start()
