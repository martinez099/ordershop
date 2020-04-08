import functools
import json
import logging
import os
import signal
import threading

import redis

from domain_model import DomainModel
from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers


class ReadModel(object):
    """
    Read Model class.
    """

    def __init__(self, _redis_host='localhost', _redis_port=6379):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('read-model', [self.get_entity,
                                                  self.get_entities,
                                                  self.get_mails,
                                                  self.get_unbilled_orders,
                                                  self.get_unshipped_orders])
        self.domain_model = DomainModel(
            redis.StrictRedis(host=_redis_host, port=_redis_port, decode_responses=True)
        )
        self.subscriptions = {}
        self.locks = {}

    @staticmethod
    def _deduce_entities(_events):
        """
        Deduce entities from events.

        :param _events: A list with events.
        :return: A dict mapping entity ID -> entity data.
        """
        if not _events:
            return {}

        # find 'created' events
        result = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                  for e in filter(lambda x: x[1]['event_action'] == 'entity_created', _events)}

        # remove 'deleted' events
        deleted = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                   for e in filter(lambda x: x[1]['event_action'] == 'entity_deleted', _events)}

        for d_id, d_data in deleted.items():
            del result[d_id]

        # change 'updated' events
        updated = {json.loads(e[1]['event_data'])['entity_id']: json.loads(e[1]['event_data'])
                   for e in filter(lambda x: x[1]['event_action'] == 'entity_updated', _events)}

        for u_id, u_data in updated.items():
            result[u_id] = u_data

        return result

    def _track_entities(self, _name, _event):
        """
        Keep track of entity events.

        :param _name: The entity name.
        :param _event: The event data.
        """
        if not self.domain_model.exists(_name):
            return

        entity = json.loads(_event.event_data)

        if _event.event_action == 'entity_created':
            self.domain_model.create(_name, entity)

        if _event.event_action == 'entity_deleted':
            self.domain_model.delete(_name, entity)

        if _event.event_action == 'entity_updated':
            self.domain_model.update(_name, entity)

    def _query_entities(self, _name):
        """
        Query all entities of a given name.

        :param _name: The entity name.
        :return: A dict mapping entity ID -> entity.
        """
        if self.domain_model.exists(_name):
            return self.domain_model.retrieve(_name)

        if _name not in self.locks:
            self.locks[_name] = threading.Lock()

        with self.locks[_name]:

            # deduce entities
            events = self.event_store.get(_name)
            entities = self._deduce_entities(events)

            # cache entities
            [self.domain_model.create(_name, entity) for entity in entities.values()]

            # track entities
            tracking_handler = functools.partial(self._track_entities, _name)
            self.event_store.subscribe(_name, tracking_handler)
            self.subscriptions[_name] = tracking_handler

            return entities

    def _query_defined_entities(self, _name, _props):
        """
        Query entities with defined properities.

        :param _name: The entity name.
        :param _props: A dict mapping property name -> property value(s).
        :return: A dict mapping entity ID -> entity.
        """
        result = {}
        for entity_id, entity in self._query_entities(_name).items():
            for prop_name, prop_value in _props.items():
                if not isinstance(prop_value, list):
                    prop_value = [prop_value]
                if prop_name in entity and entity[prop_name] in prop_value:
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
            order_ids_to_remove = list(filter(lambda x: x == shipping['order_id'] and shipping['delivered'], orders))
            if not order_ids_to_remove:
                raise Exception(f'could not find order {shipping["order_id"]} for shipping {shipping_id}')

            if order_ids_to_remove[0] not in unshipped:
                raise Exception(f'could not find order {order_ids_to_remove[0]}')

            del unshipped[order_ids_to_remove[0]]

        return unshipped

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        for name, handler in self.subscriptions.items():
            self.event_store.unsubscribe(name, handler)
        self.consumers.stop()
        logging.info('stopped.')

    def get_entity(self, _req):
        if 'name' not in _req:
            return {
                "error": "missing mandatory parameter 'name'"
            }

        if 'id' in _req:
            return {
                'result': self._query_entities(_req['name']).get(_req['id'])
            }

        elif 'props' in _req and isinstance(_req['props'], dict):
            result = list(self._query_defined_entities(_req['name'], _req['props']).values())
            if len(result) <= 1:
                return {
                    'result': result[0] if result else None
                }
            else:
                return {
                    'error': 'more than 1 result found'
                }
        else:
            return {
                'result': 'invalid paramters'
            }

    def get_entities(self, _req):
        if 'name' not in _req:
            return {
                "error": "missing mandatory parameter 'name'"
            }

        elif 'ids' in _req and isinstance(_req['ids'], list):
            return {
                'result': [self._query_entities(_req['name']).get(_id) for _id in _req['ids']]
            }

        elif 'props' in _req and isinstance(_req['props'], dict):
            return {
                'result': list(self._query_defined_entities(_req['name'], _req['props']).values())
            }

        else:
            return {
                'result': list(self._query_entities(_req['name']).values())
            }

    def get_mails(self, _req):
        return {
            'result': self.event_store.get('mail')
        }

    def get_unbilled_orders(self, _req):
        return {
            'result': self._unbilled_orders()
        }

    def get_unshipped_orders(self, _req):
        return {
            'result': self._unshipped_orders()
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

r = ReadModel(_redis_host=os.getenv('READ_MODEL_REDIS_HOST', 'localhost'))

signal.signal(signal.SIGINT, lambda n, h: r.stop())
signal.signal(signal.SIGTERM, lambda n, h: r.stop())

r.start()
