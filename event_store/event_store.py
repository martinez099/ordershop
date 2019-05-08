import functools
import json
import threading
import time
from concurrent import futures

import grpc
from redis import StrictRedis

from common.domain_model import DomainModel
from common.event_store_pb2 import PublishResponse, Notification, UnsubscribeResponse, FindAllResponse, \
    FindOneResponse, ActivateEntityCacheResponse, DeactivateEntityCacheResponse
from common.event_store_pb2_grpc import EventStoreServicer, add_EventStoreServicer_to_server
from common.utils import log_info, log_error


class EventStore(EventStoreServicer):
    """
    Event Store class.
    """

    def __init__(self):
        self.redis = StrictRedis(decode_responses=True, host='redis')
        self.subscribers = {}
        self.entity_cache_handlers = {}
        self.domain_model = DomainModel(self.redis)

    def publish(self, request, context):
        """
        Publish an event.

        :param request: The client request.
        :param context: The client context.
        :return: The entry ID.
        """
        key = 'events:{{{0}}}_{1}'.format(request.event_topic, request.event_action)
        entry_id = self.redis.xadd(
            key,
            {'event_id': request.event_id, 'event_entity': request.event_entity},
            id='{0:.6f}'.format(time.time()).replace('.', '-')
        )

        return PublishResponse(entry_id=entry_id)

    def subscribe(self, request, context):
        """
        Subscribe to an event.

        :param request: The client request.
        :param context: The client context.
        """
        self.subscribers[(request.event_topic, request.event_action, context.peer())] = True

        key = 'events:{{{0}}}_{1}'.format(request.event_topic, request.event_action)
        last_id = '$'
        while self.subscribers[(request.event_topic, request.event_action, context.peer())]:
            items = self.redis.xread({key: last_id}, block=1000) or []
            for item in items:
                last_id = item[1][0][0]
                yield Notification(
                    event_id=item[1][0][1]['event_id'],
                    event_ts=float(last_id.replace('-', '.')),
                    event_entity=item[1][0][1]['event_entity']
                )

    def unsubscribe(self, request, context):
        """
        Unsubscribe from an event.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        self.subscribers[(request.event_topic, request.event_action, context.peer())] = False

        return UnsubscribeResponse(success=True)

    def find_one(self, request, context):
        """
        Find an entity for a topic with an specific id.

        :param _topic: The event topic, i.e. name of entity.
        :param _id: The entity id.
        :return: A dict with the entity.
        """
        entity = self._find_all(request.event_topic).get(request.event_id)

        return FindOneResponse(entity=json.dumps(entity) if entity else None)

    def find_all(self, request, context):
        """
        Find all entites for a topic.

        :param _topic: The event topic, i.e name of entity.
        :return: A list with all entitys.
        """
        entities = self._find_all(request.event_topic).values()

        return FindAllResponse(entities=json.dumps(list(entities)) if entities else None)

    def activate_entity_cache(self, request, context):
        """
        Keep entity cache up to date.

        :param _topic: The entity type.
        """
        created_handler = functools.partial(self._entity_created, request.event_topic)
        self.entity_cache_handlers[(request.event_topic, 'created')] = created_handler
        self._subscribe(request.event_topic, 'created', created_handler)

        deleted_handler = functools.partial(self._entity_deleted, request.event_topic)
        self.entity_cache_handlers[(request.event_topic, 'deleted')] = deleted_handler
        self._subscribe(request.event_topic, 'deleted', deleted_handler)

        updated_handler = functools.partial(self._entity_updated, request.event_topic)
        self.entity_cache_handlers[(request.event_topic, 'updated')] = updated_handler
        self._subscribe(request.event_topic, 'updated', updated_handler)

        return ActivateEntityCacheResponse()

    def deactivate_entity_cache(self, request, context):
        """
        Stop keeping entity cache up to date.

        :param _topic: The entity type.
        """
        created_handler = self.entity_cache_handlers[(request.event_topic, 'created')]
        self._unsubscribe(request.event_topic, 'created', created_handler)

        deleted_handler = self.entity_cache_handlers[(request.event_topic, 'deleted')]
        self._unsubscribe(request.event_topic, 'deleted', deleted_handler)

        updated_handler = self.entity_cache_handlers[(request.event_topic, 'updated')]
        self._unsubscribe(request.event_topic, 'updated', updated_handler)

        return DeactivateEntityCacheResponse()

    def _find_all(self, _topic):
        """
        Find all entites for a topic.

        :param _topic: The event topic, i.e name of entity.
        :return: A dict mapping id -> entity.
        """
        def _get_entities(_events):
            entities = map(lambda x: json.loads(x[1]['event_entity']), _events)
            return dict(map(lambda x: (x['id'], x), entities))

        def _remove_deleted(_created, _deleted):
            for d in _deleted.values():
                del _created[d]
            return _created

        def _set_updated(_created, _updated):
            for k, v in _updated.items():
                _created[k] = v
            return _created

        # read from cache
        if self.domain_model.exists(_topic):
            return self.domain_model.retrieve(_topic)

        # result is a dict mapping id -> entity
        result = {}

        # read all events at once
        with self.redis.pipeline() as pipe:
            pipe.multi()
            pipe.xrange('events:{{{0}}}_created'.format(_topic))
            pipe.xrange('events:{{{0}}}_deleted'.format(_topic))
            pipe.xrange('events:{{{0}}}_updated'.format(_topic))
            created_events, deleted_events, updated_events = pipe.execute()

        # get created entities
        if created_events:
            result = _get_entities(created_events)

        # remove deleted entities
        if deleted_events:
            result = _remove_deleted(result, _get_entities(deleted_events))

        # set updated entities
        if updated_events:
            result = _set_updated(result, _get_entities(updated_events))

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
            entity = json.loads(_item[1][0][1]['event_entity'])
            self.domain_model.create(_topic, entity)

    def _entity_deleted(self, _topic, _item):
        """
        Event handler for entity deleted events, i.e. delete a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['event_entity'])
            self.domain_model.delete(_topic, entity)

    def _entity_updated(self, _topic, _item):
        """
        Event handler for entity updated events, i.e. update a cached entity.

        :param _topic: The entity type.
        :param _item: A dict with entity properties.
        """
        if self.domain_model.exists(_topic):
            entity = json.loads(_item[1][0][1]['event_entity'])
            self.domain_model.update(_topic, entity)

    def _subscribe(self, _topic, _action, _handler):
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

    def _unsubscribe(self, _topic, _action, _handler):
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
        return len(self.handlers)

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


EVENT_STORE_ADDRESS = '[::]:50051'
EVENT_STORE_THREADS = 10
EVENT_STORE_SLEEP_INTERVAL = 60 * 60 * 24
EVENT_STORE_GRACE_INTERVAL = 0


def serve():
    """
    Run the gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=EVENT_STORE_THREADS))
    try:
        add_EventStoreServicer_to_server(EventStore(), server)
        server.add_insecure_port(EVENT_STORE_ADDRESS)
        server.start()
    except Exception as e:
        log_error(e)

    log_info('serving ...')
    try:
        while True:
            time.sleep(EVENT_STORE_SLEEP_INTERVAL)
    except KeyboardInterrupt:
        server.stop(EVENT_STORE_GRACE_INTERVAL)

    log_info('done')


if __name__ == '__main__':
    serve()
