import json
import time
import uuid


class Event(object):
    """
    Event
    """

    def __init__(self, _topic, _action, **entity):
        self.id = str(uuid.uuid4())
        self.ts = time.time()
        self.topic = _topic
        self.action = _action
        self.entity = entity


class EventStore(object):
    """
    Event Store
    """

    def __init__(self, _redis):
        self.redis = _redis
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.thread = None

    def __del__(self):
        if self.thread:
            self.thread.stop()
        self.pubsub.close()

    def publish(self, _event):
        return self.redis.publish('{}_{}'.format(_event.topic, _event.action), json.dumps(_event.entity))

    def subscribe(self, _topic, _action, _handler):
        key = _topic + '_' + _action
        result = self.pubsub.subscribe(**{key: _handler})
        if not self.thread:
            self.thread = self.pubsub.run_in_thread(sleep_time=0.001)
        return result

    def unsubscribe(self, _topic, _action):
        key = _topic + '_' + _action
        result = self.pubsub.unsubscribe(key)
        if not self.redis.pubsub_channels():
            self.thread.stop()
            self.thread = None
        return result

    def save(self, _event):
        key = '{}_{}:{}'.format(_event.topic, _event.action, _event.id)
        if self.redis.zadd(_event.topic + 'S', _event.ts, key):
            return self.redis.set(key, json.dumps(_event.entity))
        return False

    def find_one(self, _topic, _id):
        return self.find_all(_topic).get(_id)

    def find_all(self, _topic):

        result = {}

        # get all event ids for that topic
        event_ids = self.redis.zrange(_topic + 'S', 0, -1)

        # get created entities
        created_eids = list(filter(lambda x: x.startswith(_topic + '_CREATED'), event_ids))
        if created_eids:
            created_events = list(map(lambda x: json.loads(x), self.redis.mget(created_eids)))
            result = dict(zip(list(map(lambda x: x['id'], created_events)), created_events))

        # remove deleted entities
        deleted_eids = list(filter(lambda x: x.startswith(_topic + '_DELETED'), event_ids))
        if deleted_eids:
            deleted_events = list(map(lambda x: json.loads(x), self.redis.mget(deleted_eids)))
            deleted_ids = list(map(lambda x: x['id'], deleted_events))
            result = EventStore.filter_deleted(result, deleted_ids)

        # adapt updated entities
        updated_eids = list(filter(lambda x: x.startswith(_topic + '_UPDATED'), event_ids))
        if updated_eids:
            updated_events = list(map(lambda x: json.loads(x), self.redis.mget(updated_eids)))
            updated_map = dict(zip(list(map(lambda x: x['id'], updated_events)), updated_events))
            result = EventStore.set_updated(result, updated_map)

        return result

    def reset(self):
        self.redis.flushdb()

    @staticmethod
    def filter_deleted(created, deleted):
        for d in deleted:
            del created[d]
        return created

    @staticmethod
    def set_updated(created, updated):
        for k, v in updated.items():
            created[k] = v
        return created
