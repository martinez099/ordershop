import json
import time
import uuid
import threading

import lib.common


class Event(object):
    """
    Event class.
    """

    def __init__(self, _topic, _action, **_entity):
        """
        Initialize an event.

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

    def __init__(self, _redis):
        """
        Initialize an event store.

        :param _redis: A Redis instance.
        """
        self.redis = _redis
        self.subscribers = {}

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
        if self.redis.exists('{}_entity:{}'.format(_topic, _id)):

            # read from cache
            result = self.redis.hgetall('{}_entity:{}'.format(_topic, _id))
            for k, v in result.items():
                if lib.common.is_key(v):
                    result[k] = self.redis.lrange(v, 0, -1)

        else:

            result = self.find_all(_topic).get(_id)

        return result

    def find_all(self, _topic):
        """
        Find all aggregated events for a topic.

        :param _topic: The event topic.
        :return: A dict mapping id -> dict of all aggregated events.
        """
        result = {}

        if self.redis.exists('{}_IDs'.format(_topic)):

            # read from cache
            for eid in self.redis.lrange('{}_IDs'.format(_topic), 0, -1):
                result[eid] = self.redis.hgetall('{}_entity:{}'.format(_topic, eid))
                for k, v in result[eid].items():
                    if lib.common.is_key(v):
                        result[eid][k] = self.redis.lrange(v, 0, -1)

        else:

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
                result = remove_deleted(result, deleted_entities)

            # set updated entities
            updated_events = self.redis.xrange('events:{}_updated'.format(_topic))
            if updated_events:
                updated_entities = map(lambda x: json.loads(x[1]['entity']), updated_events)
                updated_entities = dict(map(lambda x: (x['id'], x), updated_entities))
                result = set_updated(result, updated_entities)

            # write into cache
            for eid, value in result.items():
                self.redis.rpush('{}_IDs'.format(_topic), eid)
                for k, v in value.items():
                    if isinstance(v, list):
                        lid = '{}_{}:{}'.format(_topic, k, eid)
                        self.redis.hset('{}_entity:{}'.format(_topic, eid), k, lid)
                        self.redis.rpush(lid, *v)
                    else:
                        self.redis.hset('{}_entity:{}'.format(_topic, eid), k, v)

        return result

    def reset(self):
        self.redis.flushdb()


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _action, _handler, _redis):
        """
        Initialize a subscriber thread.

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
        self.subscribed = False

    def add_handler(self, _handler):
        self.handlers.append(_handler)

    def rem_handler(self, _handler):
        self.handlers.remove(_handler)


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
