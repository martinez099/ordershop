import functools
import json
import logging
import signal

from event_store.event_store_client import EventStoreClient, create_event
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

    @staticmethod
    def _deduce_entities(_events):
        """
        Deduce entities from events.

        :param _events: A list with events.
        :return: A dict mapping entity ID -> entity data.
        """
        if not _events:
            return {}

        # get 'created' events
        result = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                  for e in filter(lambda x: x[1]['event_action'] == 'entity_created', _events)}

        # del 'deleted' events
        deleted = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                   for e in filter(lambda x: x[1]['event_action'] == 'entity_deleted', _events)}

        for d_id, d_data in deleted.items():
            del result[d_id]

        # set 'updated' events
        updated = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                   for e in filter(lambda x: x[1]['event_action'] == 'entity_updated', _events)}

        for u_id, u_data in updated.items():
            result[u_id] = u_data

        return result

    @staticmethod
    def _track_entities(_entities, _event):
        """
        Keep track of entity events.

        :param _entities: A dict with entities, mapping entity ID -> entity data.
        :param _event: The event entry.
        """
        if _event.event_action == 'entity_created':
            event_data = json.loads(_event.event_data)
            if event_data['entity_id'] in _entities:
                raise Exception('could not deduce created event')

            _entities[event_data['entity_id']] = event_data

        if _event.event_action == 'entity_deleted':
            event_data = json.loads(_event.event_data)
            if event_data['entity_id'] not in _entities:
                raise Exception('could not deduce deleted event')

            del _entities[event_data['entity_id']]

        if _event.event_action == 'entity_updated':
            event_data = json.loads(_event.event_data)
            if event_data['entity_id'] not in _entities:
                raise Exception('could not deduce updated event')

            _entities[event_data['entity_id']] = event_data

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
        entities = self._deduce_entities(events)

        # cache entities
        self.cache[_name] = entities

        # track entities
        tracking_handler = functools.partial(self._track_entities, entities)
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
