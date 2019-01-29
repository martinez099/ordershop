import json

from lib.event_store import EventStore


class EntityCache(EventStore):
    """
    Entity Cache class.
    """

    def __init__(self, _redis):
        """
        Initialize an entity cache.

        :param _redis: A Redis instance.
        """
        super(EntityCache, self).__init__(_redis)

    def subscribe_to_product_events(self):
        self.subscribe('product', 'created', self.product_created)
        self.subscribe('product', 'deleted', self.product_deleted)
        self.subscribe('product', 'updated', self.product_updated)

    def subscribe_to_inventory_events(self):
        self.subscribe('inventory', 'created', self.inventory_created)
        self.subscribe('inventory', 'deleted', self.inventory_deleted)
        self.subscribe('inventory', 'updated', self.inventory_updated)

    def subscribe_to_customer_events(self):
        self.subscribe('customer', 'created', self.customer_created)
        self.subscribe('customer', 'deleted', self.customer_deleted)
        self.subscribe('customer', 'updated', self.customer_updated)

    def subscribe_to_order_events(self):
        self.subscribe('order', 'created', self.order_created)
        self.subscribe('order', 'deleted', self.order_deleted)
        self.subscribe('order', 'updated', self.order_updated)

    def subscribe_to_billing_events(self):
        self.subscribe('billing', 'created', self.order_created)
        self.subscribe('billing', 'deleted', self.order_deleted)
        self.subscribe('billing', 'updated', self.order_updated)

    def unsubscribe_from_product_events(self):
        self.unsubscribe('product', 'created', self.product_created)
        self.unsubscribe('product', 'deleted', self.product_deleted)
        self.unsubscribe('product', 'updated', self.product_updated)

    def unsubscribe_from_inventory_events(self):
        self.unsubscribe('inventory', 'created', self.inventory_created)
        self.unsubscribe('inventory', 'deleted', self.inventory_deleted)
        self.unsubscribe('inventory', 'updated', self.inventory_updated)

    def unsubscribe_from_customer_events(self):
        self.unsubscribe('customer', 'created', self.customer_created)
        self.unsubscribe('customer', 'deleted', self.customer_deleted)
        self.unsubscribe('customer', 'updated', self.customer_updated)

    def unsubscribe_from_order_events(self):
        self.unsubscribe('order', 'created', self.order_created)
        self.unsubscribe('order', 'deleted', self.order_deleted)
        self.unsubscribe('order', 'updated', self.order_updated)

    def unsubscribe_from_billing_events(self):
        self.unsubscribe('billing', 'created', self.order_created)
        self.unsubscribe('billing', 'deleted', self.order_deleted)
        self.unsubscribe('billing', 'updated', self.order_updated)

    def product_created(self, item):
        if self.redis.exists('{}_IDs'.format('product')):
            product = json.loads(item[1][0][1]['entity'])
            self.redis.rpush('{}_IDs'.format('product'), product['id'])
            self.redis.hmset('{}_entity:{}'.format('product', product['id']), product)

    def product_deleted(self, item):
        if self.redis.exists('{}_IDs'.format('product')):
            product = json.loads(item[1][0][1]['entity'])
            self.redis.lrem('{}_IDs'.format('product'), 1, product['id'])
            self.redis.delete('{}_entity:{}'.format('product', product['id']))

    def product_updated(self, item):
        if self.redis.exists('{}_IDs'.format('product')):
            product = json.loads(item[1][0][1]['entity'])
            self.redis.hmset('{}_entity:{}'.format('product', product['id']), product)

    def inventory_created(self, item):
        if self.redis.exists('{}_IDs'.format('inventory')):
            inventory = json.loads(item[1][0][1]['entity'])
            self.redis.rpush('{}_IDs'.format('inventory'), inventory['id'])
            for k, v in inventory.items():
                if isinstance(v, list):
                    lid = '{}_{}:{}'.format('inventory', k, inventory['id'])
                    self.redis.hset('{}_entity:{}'.format('inventory', inventory['id']), k, lid)
                    self.redis.rpush(lid, *v)
                else:
                    self.redis.hset('{}_entity:{}'.format('inventory', inventory['id']), k, v)

    def inventory_deleted(self, item):
        if self.redis.exists('{}_IDs'.format('inventory')):
            inventory = json.loads(item[1][0][1]['entity'])
            self.redis.lrem('{}_IDs'.format('inventory'), 1, inventory['id'])
            self.redis.delete('{}_entity:{}'.format('inventory', inventory['id']))
            for k, v in inventory.items():
                if isinstance(v, list):
                    self.redis.delete('{}_{}:{}'.format('inventory', k, inventory['id']))

    def inventory_updated(self, item):
        if self.redis.exists('{}_IDs'.format('inventory')):
            inventory = json.loads(item[1][0][1]['entity'])
            for k, v in inventory.items():
                if isinstance(v, list):
                    lid = '{}_{}:{}'.format('inventory', k, inventory['id'])
                    self.redis.delete(lid)
                    self.redis.rpush(lid, *v)
                else:
                    self.redis.hset('{}_entity:{}'.format('inventory', inventory['id']), k, v)

    def customer_created(self, item):
        if self.redis.exists('{}_IDs'.format('customer')):
            customer = json.loads(item[1][0][1]['entity'])
            self.redis.rpush('{}_IDs'.format('customer'), customer['id'])
            self.redis.hmset('{}_entity:{}'.format('customer', customer['id']), customer)

    def customer_deleted(self, item):
        if self.redis.exists('{}_IDs'.format('customer')):
            customer = json.loads(item[1][0][1]['entity'])
            self.redis.lrem('{}_IDs'.format('customer'), 1, customer['id'])
            self.redis.delete('{}_entity:{}'.format('customer', customer['id']))

    def customer_updated(self, item):
        if self.redis.exists('{}_IDs'.format('customer')):
            customer = json.loads(item[1][0][1]['entity'])
            self.redis.hmset('{}_entity:{}'.format('customer', customer['id']), customer)

    def order_created(self, item):
        if self.redis.exists('{}_IDs'.format('order')):
            order = json.loads(item[1][0][1]['entity'])
            self.redis.rpush('{}_IDs'.format('order'), order['id'])
            for k, v in order.items():
                if isinstance(v, list):
                    lid = '{}_{}:{}'.format('order', k, order['id'])
                    self.redis.hset('{}_entity:{}'.format('order', order['id']), k, lid)
                    self.redis.rpush(lid, *v)
                else:
                    self.redis.hset('{}_entity:{}'.format('order', order['id']), k, v)

    def order_deleted(self, item):
        if self.redis.exists('{}_IDs'.format('order')):
            order = json.loads(item[1][0][1]['entity'])
            self.redis.lrem('{}_IDs'.format('order'), 1, order['id'])
            self.redis.delete('{}_entity:{}'.format('order', order['id']))
            for k, v in order.items():
                if isinstance(v, list):
                    self.redis.delete('{}_{}:{}'.format('order', k, order['id']))

    def order_updated(self, item):
        if self.redis.exists('{}_IDs'.format('order')):
            order = json.loads(item[1][0][1]['entity'])
            for k, v in order.items():
                if isinstance(v, list):
                    lid = '{}_{}:{}'.format('order', k, order['id'])
                    self.redis.delete(lid)
                    self.redis.rpush(lid, *v)
                else:
                    self.redis.hset('{}_entity:{}'.format('order', order['id']), k, v)
