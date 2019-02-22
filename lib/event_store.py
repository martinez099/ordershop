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
        key = 'events:{}_{}'.format(_event.topic, _event.action)
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
        Find an event from a topic with an specific id.

        :param _topic: The event topic.
        :param _id: The event id.
        :return: The event dict.
        """
        return self.find_all(_topic).get(_id)

    def find_all(self, _topic):
        """
        Find all aggregated events for a topic.

        :param _topic: The event topic.
        :return: A dict mapping id -> dict of all aggregated events.
        """

        result = {}

        # read from cache
        if self.domain_model.exists(_topic):
            result = self.domain_model.retrieve(_topic)

        if not result:

            # get created entities
            created_events = self.redis.xrange('events:{}_created'.format(_topic))
            if created_events:
                created_entities = map(lambda x: json.loads(x[1]['entity']), created_events)
                result = dict(map(lambda x: (x['id'], x), created_entities))

            # remove deleted entities
            deleted_events = self.redis.xrange('events:{}_deleted'.format(_topic))
            if deleted_events:
                deleted_entities = map(lambda x: json.loads(x[1]['entity']), deleted_events)
                deleted_entities = map(lambda x: x['id'], deleted_entities)
                result = EventStore.remove_deleted(result, deleted_entities)

            # set updated entities
            updated_events = self.redis.xrange('events:{}_updated'.format(_topic))
            if updated_events:
                updated_entities = map(lambda x: json.loads(x[1]['entity']), updated_events)
                updated_entities = dict(map(lambda x: (x['id'], x), updated_entities))
                result = EventStore.set_updated(result, updated_entities)

            # write into cache
            for k, v in result.items():
                self.domain_model.create(_topic, v)

        return result

    def subscribe_to_entity_events(self, _topic):
        """
        Keep entity cache up to date.

        :param _topic: The entity type.
        """
        self.subscribe(_topic, 'created', functools.partial(self.entity_created, _topic))
        self.subscribe(_topic, 'deleted', functools.partial(self.entity_deleted, _topic))
        self.subscribe(_topic, 'updated', functools.partial(self.entity_updated, _topic))

    def unsubscribe_from_entity_events(self, _topic):
        """
        Stop keeping entity cache up to date.

        :param _topic: The entity type.
        """
        self.unsubscribe(_topic, 'created', functools.partial(self.entity_created, _topic))
        self.unsubscribe(_topic, 'deleted', functools.partial(self.entity_deleted, _topic))
        self.unsubscribe(_topic, 'updated', functools.partial(self.entity_updated, _topic))

    def entity_created(self, _topic, _item):
        """
        Event handler for entity created events, i.e. create a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.create(_topic, entity)

    def entity_deleted(self, _topic, _item):
        """
        Event handler for entity deleted events, i.e. delete a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.delete(_topic, entity)

    def entity_updated(self, _topic, _item):
        """
        Event handler for entity updated events, i.e. update a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['entity'])
            self.domain_model.update(_topic, entity)

    @staticmethod
    def remove_deleted(created, deleted):
        """
        Remove deleted events.

        :param created: A dict mapping id -> dict of created events.
        :param deleted: A list of deleted ids.
        :return: A dict without deleted events.
        """
        for d in deleted:
            del created[d]
        return created

    @staticmethod
    def set_updated(created, updated):
        """
        Adapt updated events.

        :param created: A dict mapping id -> dict of created events.
        :param updated: A dict mapping id -> dict of updated events.
        :return: A dict with updated events.
        """
        for k, v in updated.items():
            created[k] = v
        return created


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
        self.key = 'events:{}_{}'.format(_topic, _action)
        self.subscribed = True
        self.handlers = [_handler]
        self.redis = _redis

    def __len__(self):
        return bool(self.handlers)

    def run(self):
        """
        Poll the event stream and call each handler for each entry returned.
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
