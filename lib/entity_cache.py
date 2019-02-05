import functools
import json

from lib.domain_model import DomainModel
from lib.event_store import EventStore


class EntityCache(EventStore):
    """
    Entity Cache class.
    """
    domain_model = None

    def __init__(self, _redis):
        """
        Initialize an entity cache.

        :param _redis: A Redis instance.
        """
        super(EntityCache, self).__init__(_redis)
        self.domain_model = DomainModel(_redis)

    def read_from_cache(self, _topic):
        if self.domain_model.exists(_topic):
            return self.domain_model.retrieve(_topic)

    def write_into_cache(self, _topic, _result):
        for k, v in _result.items():
            self.domain_model.create(_topic, v)

    def subscribe_to_product_events(self):
        self.subscribe('product', 'created', functools.partial(self.entity_created, 'product'))
        self.subscribe('product', 'deleted', functools.partial(self.entity_deleted, 'product'))
        self.subscribe('product', 'updated', functools.partial(self.entity_updated, 'product'))

    def subscribe_to_inventory_events(self):
        self.subscribe('inventory', 'created', functools.partial(self.entity_created, 'inventory'))
        self.subscribe('inventory', 'deleted', functools.partial(self.entity_deleted, 'inventory'))
        self.subscribe('inventory', 'updated', functools.partial(self.entity_updated, 'inventory'))

    def subscribe_to_customer_events(self):
        self.subscribe('customer', 'created', functools.partial(self.entity_created, 'customer'))
        self.subscribe('customer', 'deleted', functools.partial(self.entity_deleted, 'customer'))
        self.subscribe('customer', 'updated', functools.partial(self.entity_updated, 'customer'))

    def subscribe_to_order_events(self):
        self.subscribe('order', 'created', functools.partial(self.entity_created, 'order'))
        self.subscribe('order', 'deleted', functools.partial(self.entity_deleted, 'order'))
        self.subscribe('order', 'updated', functools.partial(self.entity_updated, 'order'))

    def subscribe_to_billing_events(self):
        self.subscribe('billing', 'created', functools.partial(self.entity_created, 'billing'))
        self.subscribe('billing', 'deleted', functools.partial(self.entity_deleted, 'billing'))
        self.subscribe('billing', 'updated', functools.partial(self.entity_updated, 'billing'))

    def unsubscribe_from_product_events(self):
        self.unsubscribe('product', 'created', functools.partial(self.entity_created, 'product'))
        self.unsubscribe('product', 'deleted', functools.partial(self.entity_deleted, 'product'))
        self.unsubscribe('product', 'updated', functools.partial(self.entity_updated, 'product'))

    def unsubscribe_from_inventory_events(self):
        self.unsubscribe('inventory', 'created', functools.partial(self.entity_created, 'inventory'))
        self.unsubscribe('inventory', 'deleted', functools.partial(self.entity_deleted, 'inventory'))
        self.unsubscribe('inventory', 'updated', functools.partial(self.entity_updated, 'inventory'))

    def unsubscribe_from_customer_events(self):
        self.unsubscribe('customer', 'created', functools.partial(self.entity_created, 'customer'))
        self.unsubscribe('customer', 'deleted', functools.partial(self.entity_deleted, 'customer'))
        self.unsubscribe('customer', 'updated', functools.partial(self.entity_updated, 'customer'))

    def unsubscribe_from_order_events(self):
        self.unsubscribe('order', 'created', functools.partial(self.entity_created, 'order'))
        self.unsubscribe('order', 'deleted', functools.partial(self.entity_deleted, 'order'))
        self.unsubscribe('order', 'updated', functools.partial(self.entity_updated, 'order'))

    def unsubscribe_from_billing_events(self):
        self.unsubscribe('billing', 'created', functools.partial(self.entity_created, 'billing'))
        self.unsubscribe('billing', 'deleted', functools.partial(self.entity_deleted, 'billing'))
        self.unsubscribe('billing', 'updated', functools.partial(self.entity_updated, 'billing'))

    def entity_created(self, _topic, _item):
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.create(_topic, entity)

    def entity_deleted(self, _topic, _item):
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.delete(_topic, entity)

    def entity_updated(self, _topic, _item):
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.update(_topic, entity)
