import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class ProductService(object):
    """
    Product Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('product-service', [self.create_products,
                                                       self.update_product,
                                                       self.delete_product])

    @staticmethod
    def _create_entity(_name, _price):
        """
        Create a product entity.

        :param _name: The name of the product.
        :param _price: The price of the product.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'name': _name,
            'price': _price
        }

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.consumers.stop()
        logging.info('stopped.')

    def create_products(self, _req):

        products = _req if isinstance(_req, list) else [_req]
        product_ids = []

        for product in products:
            try:
                new_product = ProductService._create_entity(product['name'], product['price'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'name' and/or 'price'"
                }

            # trigger event
            self.event_store.publish('product', create_event('entity_created', new_product))

            product_ids.append(new_product['entity_id'])

        return {
            "result": product_ids
        }

    def update_product(self, _req):

        try:
            product_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'product', 'id': product_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        product = rsp['result']
        if not product:
            return {
                "error": "could not find product"
            }

        # set new props
        product['entity_id'] = product_id
        try:
            product['name'] = _req['name']
            product['price'] = _req['price']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'name' and/or 'price"
            }

        # trigger event
        self.event_store.publish('product', create_event('entity_updated', product))

        return {
            "result": True
        }

    def delete_product(self, _req):

        try:
            product_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'product', 'id': product_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        product = rsp['result']
        if not product:
            return {
                "error": "could not find product"
            }

        # trigger event
        self.event_store.publish('product', create_event('entity_deleted', product))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

p = ProductService()

signal.signal(signal.SIGINT, lambda n, h: p.stop())
signal.signal(signal.SIGTERM, lambda n, h: p.stop())

p.start()
