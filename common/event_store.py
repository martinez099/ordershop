import json
import threading
import uuid

import grpc

from common.event_store_pb2 import PublishRequest, FindOneRequest, FindAllRequest, ActivateEntityCacheRequest, \
    DeactivateEntityCacheRequest, SubscribeRequest
from common.event_store_pb2_grpc import EventStoreStub


EVENT_STORE_ADDRESS = 'event-store:50051'


class EventStore(object):
    """
    Event Store class.
    """

    def __init__(self):
        self.channel = grpc.insecure_channel(EVENT_STORE_ADDRESS)
        self.stub = EventStoreStub(self.channel)
        self.subscribers = {}

    def __del__(self):
        self.channel.close()

    def publish(self, _topic, _action, **_entity):
        """
        Publish an event.

        :param _topic: The event topic.
        :param _action: The event action.
        :param _entity: The event entity.
        :return: The entry ID.
        """
        request = PublishRequest(
            event_id=str(uuid.uuid4()),
            event_topic=_topic,
            event_action=_action,
            event_entity=json.dumps(_entity)
        )
        response = self.stub.publish(request)
        return response.entry_id

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
            subscriber = Subscriber(_topic, _action, _handler, self.stub)
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
        request = FindOneRequest(event_topic=_topic, event_id=_id)
        response = self.stub.find_one(request)

        return json.loads(response.entity) if response.entity else None

    def find_all(self, _topic):
        """
        Find all entites for a topic.

        :param _topic: The event topic, i.e name of entity.
        :return: A list with all entitys.
        """
        request = FindAllRequest(event_topic=_topic)
        response = self.stub.find_all(request)

        return json.loads(response.entities) if response.entities else None

    def activate_entity_cache(self, _topic):
        """
        Keep entity cache up to date.

        :param _topic: The entity type.
        """
        request = ActivateEntityCacheRequest(event_topic=_topic)
        response = self.stub.activate_entity_cache(request)

        return bool(response)

    def deactivate_entity_cache(self, _topic):
        """
        Stop keeping entity cache up to date.

        :param _topic: The entity type.
        """
        request = DeactivateEntityCacheRequest(event_topic=_topic)
        response = self.stub.deactivate_entity_cache(request)

        return bool(response)


class Subscriber(threading.Thread):
    """
    Subscriber Thread class.
    """

    def __init__(self, _topic, _action, _handler, _stub):
        """
        :param _topic: The topic to subscirbe to.
        :param _action: The action to scubscribe to.
        :param _handler: A handler function.
        """
        super(Subscriber, self).__init__()
        self._running = False
        self.subscribed = True
        self.handlers = [_handler]
        self.topic = _topic
        self.action = _action
        self.stub = _stub

    def __len__(self):
        return len(self.handlers)

    def run(self):
        """
        Poll the event stream and call each handler with each entry returned.
        """
        if self._running:
            return

        self._running = True
        while self.subscribed:
            request = SubscribeRequest(event_topic=self.topic, event_action=self.action)
            for item in self.stub.subscribe(request):
                for handler in self.handlers:
                    handler(item)
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
