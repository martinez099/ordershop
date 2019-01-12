import json
import time
import uuid


class Event(object):
    """
    Event class.
    """

    def __init__(self, _topic, _action, **entity):
        """
        Initialize an event.

        :param _topic: The event topic.
        :param _action: The event action.
        :param entity: The event entity.
        """
        self.id = str(uuid.uuid4())
        self.ts = time.time()
        self.topic = _topic
        self.action = _action
        self.entity = entity


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
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.thread = None
        lua = """
if redis.call('SET', KEYS[1], ARGV[1]) then
    if redis.call('ZADD', KEYS[2], ARGV[2], KEYS[1]) then
        if redis.call('PUBLISH', ARGV[3], ARGV[1]) then
            return true
        else
            redis.call('DEL', KEYS[1])
            redis.call('ZREM', KEYS[2], KEYS[1])
            return false
        end
    else
        redis.call('DEL', KEYS[1])
        return false
    end
else
    return false
end
"""
        self.script_publish = self.redis.register_script(lua)

    def __del__(self):
        """
        Finalize an event store.
        """
        if self.thread:
            self.thread.stop()
        self.pubsub.close()

    def publish(self, _event):
        """
        Publish an event.

        :param _event: The event to publish.
        :return: Success.
        """
        event_key = 'EVENT:{}'.format('{' + _event.topic + '}')
        event_key = event_key + '_{}:{}'.format(_event.action, _event.id)

        events_key = 'EVENTS:{}'.format('{' + _event.topic + '}')

        channel = 'EVENT:{}_{}'.format(_event.topic, _event.action)
        payload = json.dumps(_event.entity)
        timestamp = _event.ts

        return self.script_publish(keys=[event_key, events_key], args=[payload, timestamp, channel])

    def subscribe(self, _topic, _action, _handler):
        """
        Subscribe to an event channel.

        :param _topic: The event topic.
        :param _action: The event action.
        :param _handler: The event handler.
        :return: Success.
        """
        key = 'EVENT:{}_{}'.format(_topic, _action)
        result = self.pubsub.subscribe(**{key: _handler})

        # start background thread
        if not self.thread:
            self.thread = self.pubsub.run_in_thread(sleep_time=0.001)

        return result

    def unsubscribe(self, _topic, _action):
        """
        Unsubscribe from an event channel.

        :param _topic: The event topic.
        :param _action: The event action.
        :return: Success.
        """
        key = 'EVENT:{}_{}'.format(_topic, _action)
        result = self.pubsub.unsubscribe(key)

        # stop background thread
        if not self.redis.pubsub_channels():
            self.thread.stop()
            self.thread = None

        return result

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

        # get all event ids for that topic
        event_ids = self.redis.zrange('EVENTS:{}'.format('{' + _topic + '}'), 0, -1)

        # get created entities
        created_eids = list(filter(lambda x: x.startswith('EVENT:{}_CREATED'.format('{' + _topic + '}')), event_ids))
        if created_eids:
            created_events = list(map(lambda x: json.loads(x), self.redis.mget(created_eids)))
            created_ids = list(map(lambda x: x['id'], created_events))
            result = dict(zip(created_ids, created_events))

        # remove deleted entities
        deleted_eids = list(filter(lambda x: x.startswith('EVENT:{}_DELETED'.format('{' + _topic + '}')), event_ids))
        if deleted_eids:
            deleted_events = list(map(lambda x: json.loads(x), self.redis.mget(deleted_eids)))
            deleted_ids = list(map(lambda x: x['id'], deleted_events))
            result = EventStore.filter_deleted(result, deleted_ids)

        # adapt updated entities
        updated_eids = list(filter(lambda x: x.startswith('EVENT:{}_UPDATED'.format('{' + _topic + '}')), event_ids))
        if updated_eids:
            updated_events = list(map(lambda x: json.loads(x), self.redis.mget(updated_eids)))
            updated_map = dict(zip(list(map(lambda x: x['id'], updated_events)), updated_events))
            result = EventStore.set_updated(result, updated_map)

        return result

    def reset(self):
        self.redis.flushdb()

    @staticmethod
    def filter_deleted(created, deleted):
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
