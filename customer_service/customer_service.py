import atexit
import logging
import uuid

from event_store.event_store_client import EventStoreClient
from message_queue.message_queue_client import Receivers


class CustomerService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.es = EventStoreClient()
        self.rs = Receivers('customer-service', [self.post_customers,
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
            'id': str(uuid.uuid4()),
            'name': _name,
            'email': _email
        }

    def start(self):
        logging.info('starting ...')
        self.es.activate_entity_cache('customer')
        atexit.register(self.es.deactivate_entity_cache, 'customer')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_customers(self, _req):

        try:
            billing_id = _req['id']
        except KeyError:
            return {
                "result": list(self.es.find_all('customer').values())
            }

        customer = self.es.find_one('customer', billing_id)
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
            self.es.publish('customer', 'created', **new_customer)

            customer_ids.append(new_customer['id'])

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
            customer_id = customer['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        customer['id'] = customer_id

        # trigger event
        self.es.publish('customer', 'updated', **customer)

        return {
            "result": True
        }

    def delete_customer(self, _req):

        try:
            customer_id = _req['id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'id'"
            }

        customer = self.es.find_one('customer', customer_id)
        if not customer:
            return {
                "error": "could not find customer"
            }

        # trigger event
        self.es.publish('customer', 'deleted', **customer)

        return {
            "result": True
        }


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)-6s] %(message)s')

c = CustomerService()
c.start()
