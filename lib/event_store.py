import functools
import json
import threading
import time
import uuid

from redis import StrictRedis

from lib.domain_model import DomainModel


class Event(object):
    """
    Event class.
    """

    def __init__(self, _topic, _action, **_entity):
        """
        :param _topic: The event topic.
        :param _action: The event action.
        :param _entity: The event entity.
        """
        self.id = str(uuid.uuid4())
        self.ts = time.time()
        self.topic = _topic
        self.action = _action
        self.entity = _entity


class EventStore(object):
    """
    Event Store class.
    """

    def __init__(self):
        self.redis = StrictRedis(decode_responses=True, host='redis')
        self.subscribers = {}
        self.domain_model = DomainModel(self.redis)

    def publish(self, _event):
        """
        Publish an event.

        :param _event: The event to publish.
        :return: Success.
        """
        key = 'events:{{{0}}}_{1}'.format(_event.topic, _event.action)
        entity = json.dumps(_event.entity)
        entry_id = '{0:.6f}'.format(_event.ts).replace('.', '-')

        return self.redis.xadd(key, {'event_id': _event.id, 'entity': entity}, id=entry_id)

    def subscribe(self, _topic, _action, _handler):
        """
        Subscribe to an event channel.

        :param _topic: The event topic.
        :param _action: The event action.
        :param _handler: The event handler.
        :return: Success.
        """
        if (_topic, _action) in self.subscribers:
            self.subscribers[(_topic, _action)].add_handler(_handler)
        else:
            subscriber = Subscriber(_topic, _action, _handler, self.redis)
            subscriber.start()
            self.subscribers[(_topic, _action)] = subscriber

        return True

    def unsubscribe(self, _topic, _action, _handler):
        """
        Unsubscribe from an event channel.

        :param _topic: The event topic.
        :param _action: The event action.
        :param _handler: The event handler.
        :return: Success.
        """
        subscriber = self.subscribers.get((_topic, _action))
        if not subscriber:
            return False

        subscriber.rem_handler(_handler)
        if not subscriber:
            subscriber.stop()
            del self.subscribers[(_topic, _action)]

        return True

    def find_one(self, _topic, _id):
        """
        Find an entity for a topic with an specific id.

        :param _topic: The event topic, i.e. name of entity.
        :param _id: The entity id.
        :return: A dict with the entity.
        """
        return self._find_all(_topic).get(_id)

    def find_all(self, _topic):
        """
        Find all entites for a topic.

        :param _topic: The event topic, i.e name of entity.
        :return: A list with all entitys.
        """
        return list(self._find_all(_topic).values())

    def activate_entity_cache(self, _topic):
        """
        Keep entity cache up to date.

        :param _topic: The entity type.
        """
        self.subscribe(_topic, 'created', functools.partial(self._entity_created, _topic))
        self.subscribe(_topic, 'deleted', functools.partial(self._entity_deleted, _topic))
        self.subscribe(_topic, 'updated', functools.partial(self._entity_updated, _topic))

    def deactivate_entity_cache(self, _topic):
        """
        Stop keeping entity cache up to date.

        :param _topic: The entity type.
        """
        self.unsubscribe(_topic, 'created', functools.partial(self._entity_created, _topic))
        self.unsubscribe(_topic, 'deleted', functools.partial(self._entity_deleted, _topic))
        self.unsubscribe(_topic, 'updated', functools.partial(self._entity_updated, _topic))

    def _find_all(self, _topic):
        """
        Find all entites for a topic.

        :param _topic: The event topic, i.e name of entity.
        :return: A dict mapping id -> entity.
        """
        def _remove_deleted(_created, _deleted):
            for d in _deleted:
                del _created[d]
            return _created

        def _set_updated(_created, _updated):
            for k, v in _updated.items():
                _created[k] = v
            return _created

        # read from cache
        if self.domain_model.exists(_topic):
            return self.domain_model.retrieve(_topic)

        # read all events at once
        with self.redis.pipeline() as pipe:
            pipe.multi()
            pipe.xrange('events:{{{0}}}_created'.format(_topic))
            pipe.xrange('events:{{{0}}}_deleted'.format(_topic))
            pipe.xrange('events:{{{0}}}_updated'.format(_topic))
            created_events, deleted_events, updated_events = pipe.execute()

        # result is a dict mapping id -> entity
        result = {}

        # get created entities
        if created_events:
            created_entities = map(lambda x: json.loads(x[1]['entity']), created_events)
            result = dict(map(lambda x: (x['id'], x), created_entities))

        # remove deleted entities
        if deleted_events:
            deleted_entities = map(lambda x: json.loads(x[1]['entity']), deleted_events)
            deleted_entities = map(lambda x: x['id'], deleted_entities)
            result = _remove_deleted(result, deleted_entities)

        # set updated entities
        if updated_events:
            updated_entities = map(lambda x: json.loads(x[1]['entity']), updated_events)
            updated_entities = dict(map(lambda x: (x['id'], x), updated_entities))
            result = _set_updated(result, updated_entities)

        # write into cache
        for value in result.values():
            self.domain_model.create(_topic, value)

        return result

    def _entity_created(self, _topic, _item):
        """
        Event handler for entity created events, i.e. create a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.create(_topic, entity)

    def _entity_deleted(self, _topic, _item):
        """
        Event handler for entity deleted events, i.e. delete a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.delete(_topic, entity)

    def _entity_updated(self, _topic, _item):
        """
        Event handler for entity updated events, i.e. update a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.update(_topic, entity)


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _action, _handler, _redis):
        """
        :param _topic: The topic to subscirbe to.
        :param _action: The action to scubscribe to.
        :param _handler: A handler function.
        :param _redis: A Redis instance.
        """
        super(Subscriber, self).__init__()
        self._running = False
        self.key = 'events:{{{0}}}_{1}'.format(_topic, _action)
        self.subscribed = True
        self.handlers = [_handler]
        self.redis = _redis

    def __len__(self):
        return bool(self.handlers)

    def run(self):
        """
        Poll the event stream and call each handler with each entry returned.
        """
        if self._running:
            return

        last_id = '$'
        self._running = True
        while self.subscribed:
            items = self.redis.xread({self.key: last_id}, block=1000) or []
            for item in items:
                for handler in self.handlers:
                    handler(item)
                last_id = item[1][0][0]
        self._running = False

    def stop(self):
        """
        Stop polling the event stream.
        """
        self.subscribed = False

    def add_handler(self, _handler):
        """
        Add an event handler.

        :param _handler: The event handler function.
        """
        self.handlers.append(_handler)

    def rem_handler(self, _handler):
        """
        Remove an event handler.

        :param _handler: The event handler function.
        """
        self.handlers.remove(_handler)
