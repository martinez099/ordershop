import atexit
import json
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue, Receivers


class InventoryService(object):

    def __init__(self):
        self.store = EventStore()
        self.mq = MessageQueue()
        self.rs = Receivers(self.mq, 'inventory-service', [self.get_inventory,
                                                           self.post_inventory,
                                                           self.put_inventory,
                                                           self.delete_inventory,
                                                           self.incr_amount,
                                                           self.decr_amount,
                                                           self.decr_from_order])

    @staticmethod
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

    def start(self):
        self.store.activate_entity_cache('inventory')
        atexit.register(self.store.deactivate_entity_cache, 'inventory')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_inventory(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            inventory = json.dumps([item for item in self.store.find_all('inventory')])
            return json.dumps(inventory)

        inventory = self.store.find_one('inventory', billing_id)
        if not inventory:
            raise ValueError("could not find inventory")

        return json.dumps(inventory) if inventory else json.dumps(False)

    def post_inventory(self, _req):

        inventory = json.loads(_req)
        if not isinstance(inventory, list):
            inventory = [inventory]

        inventory_ids = []
        for inventory in inventory:
            try:
                new_inventory = InventoryService.create_inventory(inventory['product_id'], inventory['amount'])
            except KeyError:
                raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

            # trigger event
            self.store.publish('inventory', 'created', **new_inventory)

            inventory_ids.append(new_inventory['id'])

        return json.dumps(inventory_ids)

    def put_inventory(self, _req):

        inventory = json.loads(_req)
        try:
            inventory = InventoryService.create_inventory(inventory['product_id'], inventory['amount'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id' and/or 'amount'")

        try:
            inventory_id = inventory['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        inventory['id'] = inventory_id

        # trigger event
        self.store.publish('inventory', 'updated', **inventory)

        return json.dumps(True)

    def delete_inventory(self, _req):

        try:
            inventory_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        inventory = self.store.find_one('inventory', inventory_id)
        if not inventory:
            raise ValueError("could not find inventory")

        # trigger event
        self.store.publish('inventory', 'deleted', **inventory)

        return json.dumps(True)

    def incr_amount(self, _req):

        params = json.loads(_req)
        try:
            product_id = params['product_id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id'")

        inventory = list(filter(lambda x: x['product_id'] == product_id, self.store.find_all('inventory')))
        if not inventory:
            raise ValueError("could not find inventory")

        value = params.get('value')
        inventory = inventory[0]
        inventory['amount'] = int(inventory['amount']) - (value if value else 1)

        # trigger event
        self.store.publish('inventory', 'updated', **inventory)

        return json.dumps(True)

    def decr_amount(self, _req):

        params = json.loads(_req)
        try:
            product_id = params['product_id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'product_id'")

        inventory = list(filter(lambda x: x['product_id'] == product_id, self.store.find_all('inventory')))
        if not inventory:
            raise ValueError("could not find inventory")

        value = params.get('value')
        inventory = inventory[0]
        if int(inventory['amount']) - (value if value else 1) >= 0:

            inventory['amount'] = int(inventory['amount']) - (value if value else 1)

            # trigger event
            self.store.publish('inventory', 'updated', **inventory)

            return json.dumps(True)
        else:
            return json.dumps(False)

    def decr_from_order(self, _req):

        values = json.loads(_req)
        if not isinstance(values, list):
            values = [values]

        occurs = {}
        for value in values:
            try:
                product_ids = value['product_ids']
            except KeyError:
                raise ValueError("missing mandatory parameter 'product_ids'")

            for inventory in self.store.find_all('inventory'):

                if not inventory['product_id'] in occurs:
                    occurs[inventory['product_id']] = 0

                # check amount
                occurs[inventory['product_id']] += product_ids.count(inventory['product_id'])
                if occurs[inventory['product_id']] <= int(inventory['amount']):
                    continue
                else:
                    return json.dumps(False)

        for k, v in occurs.items():
            inventory = list(filter(lambda x: x['product_id'] == k, self.store.find_all('inventory')))
            if not inventory:
                raise ValueError("could not find inventory")

            inventory = inventory[0]
            if int(inventory['amount']) - v >= 0:

                inventory['amount'] = int(inventory['amount']) - v

                # trigger event
                self.store.publish('inventory', 'updated', **inventory)

            else:
                return json.dumps(False)

        return json.dumps(True)


i = InventoryService()
i.start()
