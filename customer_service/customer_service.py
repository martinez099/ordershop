import atexit
import json
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import MessageQueue, Receivers


class CustomerService(object):
    """
    Billing Service class.
    """

    def __init__(self):
        self.store = EventStore()
        self.mq = MessageQueue()
        self.rs = Receivers(self.mq, 'customer-service', [self.post_customers,
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
        self.store.activate_entity_cache('customer')
        atexit.register(self.store.deactivate_entity_cache, 'customer')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_customers(self, _req):

        try:
            billing_id = json.loads(_req)['id']
        except KeyError:
            customers = json.dumps([item for item in self.store.find_all('customer')])
            return json.dumps(customers)

        customer = self.store.find_one('customer', billing_id)
        if not customer:
            raise ValueError("could not find customer")

        return json.dumps(customer) if customer else json.dumps(False)

    def post_customers(self, _req):

        customers = json.loads(_req)
        if not isinstance(customers, list):
            customers = [customers]

        customer_ids = []
        for customer in customers:
            try:
                new_customer = CustomerService.create_customer(customer['name'], customer['email'])
            except KeyError:
                raise ValueError("missing mandatory parameter 'name' and/or 'email'")

            # trigger event
            self.store.publish('customer', 'created', **new_customer)

            customer_ids.append(new_customer['id'])

        return json.dumps(customer_ids)

    def put_customer(self, _req):

        customer = json.loads(_req)
        try:
            customer = CustomerService.create_customer(customer['name'], customer['email'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'email'")

        try:
            customer_id = customer['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        customer['id'] = customer_id

        # trigger event
        self.store.publish('customer', 'updated', **customer)

        return json.dumps(True)

    def delete_customer(self, _req):

        try:
            customer_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        customer = self.store.find_one('customer', customer_id)
        if not customer:
            raise ValueError("could not find customer")

        # trigger event
        self.store.publish('customer', 'deleted', **customer)

        return json.dumps(True)


c = CustomerService()
c.start()
