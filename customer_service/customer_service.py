import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Receivers, send_message


class CustomerService(object):
    """
    Customer Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Receivers('customer-service', [self.post_customers,
                                                        self.get_customers,
                                                        self.put_customer,
                                                        self.delete_customer])

    @staticmethod
    def create_customer(_name, _email):
        """
        Create a customer entity.

        :param _name: The name of the customer.
        :param _email: The em2ail address of the customer.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'name': _name,
            'email': _email
        }

    def start(self):
        logging.info('starting ...')
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.receivers.stop()
        logging.info('stopped.')

    def get_customers(self, _req):

        try:
            rsp = send_message('read-model', 'get_all_entities', {'name': 'customer'})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_all_entities',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        customers = rsp['result']

        try:
            customer_id = _req['entity_id']
        except KeyError:
            return {
                "result": list(customers.values())
            }

        customer = customers.get(customer_id)
        if not customer:
            return {
                "error": "could not find customer"
            }

        return {
            "result": customer
        }

    def post_customers(self, _req):

        customers = _req if isinstance(_req, list) else [_req]
        customer_ids = []

        for customer in customers:
            try:
                new_customer = CustomerService.create_customer(customer['name'], customer['email'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'name' and/or 'email'"
                }

            # trigger event
            self.event_store.publish('customer', create_event('entity_created', new_customer))

            customer_ids.append(new_customer['entity_id'])

        return {
            "result": customer_ids
        }

    def put_customer(self, _req):

        try:
            customer = CustomerService.create_customer(_req['name'], _req['email'])
        except KeyError:
            return {
                "error": "missing mandatory parameter 'name' and/or 'email'"
            }

        try:
            customer['entity_id'] = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        # trigger event
        self.event_store.publish('customer', create_event('entity_updated', customer))

        return {
            "result": True
        }

    def delete_customer(self, _req):

        try:
            customer_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'customer', 'id': customer_id})
        except Exception as e:
            return {
                "error": "cannot send message to {}.{} ({}): {}".format('read-model',
                                                                        'get_one_entity',
                                                                        e.__class__.__name__,
                                                                        str(e))
            }

        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        customer = rsp['result']
        if not customer:
            return {
                "error": "could not find customer"
            }

        # trigger event
        self.event_store.publish('customer', create_event('entity_deleted', customer))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

c = CustomerService()

signal.signal(signal.SIGINT, c.stop)
signal.signal(signal.SIGTERM, c.stop)

c.start()
