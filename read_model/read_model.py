import functools
import logging
import signal

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Consumers


class ReadModel(object):
    """
    Read Model class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('read-model', [self.get_all_entities,
                                                  self.get_mult_entities,
                                                  self.get_spec_entities,
                                                  self.get_one_entity,
                                                  self.get_unbilled_orders,
                                                  self.get_unshipped_orders])
        self.cache = {}
        self.subscriptions = {}

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        for name, handler in self.subscriptions.items():
            self.event_store.unsubscribe(name, handler)
        self.consumers.stop()
        logging.info('stopped.')

    def get_all_entities(self, _req):
        return {
            'result': self._query_entities(_req['name'])
        }

    def get_mult_entities(self, _req):
        return {
            'result': [self._query_entities(_req['name']).get(_id) for _id in _req['ids']]
        }

    def get_spec_entities(self, _req):
        return {
            'result': self._query_spec_entities(_req['name'], _req['props'])
        }

    def get_one_entity(self, _req):
        return {
            'result': self._query_entities(_req['name']).get(_req['id'])
        }

    def get_unbilled_orders(self, _req):
        return {
            'result': self._unbilled_orders()
        }

    def get_unshipped_orders(self, _req):
        return {
            'result': self._unshipped_orders()
        }

    def _query_entities(self, _name):
        """
        Query all entities of a given name.

        :param _name: The entity name.
        :return: A dict mapping entity ID -> entity.
        """
        if _name in self.cache:
            return self.cache[_name]

        # deduce entities
        events = self.event_store.get(_name)
        entities = deduce_entities(events)

        # cache entities
        self.cache[_name] = entities

        # track entities
        tracking_handler = functools.partial(track_entities, entities)
        self.event_store.subscribe(_name, tracking_handler)
        self.subscriptions[_name] = tracking_handler

        return entities

    def _query_spec_entities(self, _name, _props):
        """
        Query entities with defined properities.

        :param _name: The entity name.
        :param _props: A dict mapping property name -> property value.
        :return: A dict mapping entity ID -> entity.
        """
        result = {}
        for entity_id, entity in self._query_entities(_name).items():
            for prop_name, prop_value in _props.items():
                if prop_name in entity and entity[prop_name] == prop_value:
                    result[entity_id] = entity

        return result

    def _unbilled_orders(self):
        """
        Query all unbilled orders, i.e. orders w/o corresponding billing.

        :return: a dict mapping entity ID -> entity.
        """
        orders = self._query_entities('order')
        billings = self._query_entities('billing')

        unbilled = orders.copy()
        for billing_id, billing in billings.items():
            order_ids_to_remove = list(filter(lambda x: x == billing['order_id'], orders))
            if not order_ids_to_remove:
                raise Exception(f'could not find order {billing["order_id"]} for billing {billing_id}')

            if order_ids_to_remove[0] not in unbilled:
                raise Exception(f'could not find order {order_ids_to_remove[0]}')

            del unbilled[order_ids_to_remove[0]]

        return unbilled

    def _unshipped_orders(self):
        """
        Query all unshipped orders, i.e. orders w/o corresponding shipping done.

        :return: a dict mapping entity ID -> entity.
        """
        orders = self._query_entities('order')
        shipping = self._query_entities('shipping')

        unshipped = orders.copy()
        for shipping_id, shipping in shipping.items():
            order_ids_to_remove = list(filter(lambda x: x == shipping['order_id'] and shipping['done'], orders))
            if not order_ids_to_remove:
                raise Exception(f'could not find order {shipping["order_id"]} for shipping {shipping_id}')

            if order_ids_to_remove[0] not in unshipped:
                raise Exception(f'could not find order {order_ids_to_remove[0]}')

            del unshipped[order_ids_to_remove[0]]

        return unshipped


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

r = ReadModel()

signal.signal(signal.SIGINT, lambda n, h: r.stop())
signal.signal(signal.SIGTERM, lambda n, h: r.stop())

r.start()
