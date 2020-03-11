import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Receivers, send_message


class ProductService(object):
    """
    Product Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.receivers = Receivers('product-service', [self.get_products,
                                                       self.post_products,
                                                       self.put_product,
                                                       self.delete_product])

    @staticmethod
    def create_product(_name, _price):
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
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.receivers.stop()
        logging.info('stopped.')

    def get_products(self, _req):

        try:
            rsp = send_message('read-model', 'get_all_entities', {'name': 'product'})
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

        products = rsp['result']

        try:
            product_id = _req['entity_id']
        except KeyError:
            return {
                "result": list(products.values())
            }

        product = products.get(product_id)
        if not product:
            return {
                "error": "could not find product"
            }

        return {
            "result": product
        }

    def post_products(self, _req):

        products = _req if isinstance(_req, list) else [_req]
        product_ids = []

        for product in products:
            try:
                new_product = ProductService.create_product(product['name'], product['price'])
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

    def put_product(self, _req):

        try:
            product = ProductService.create_product(_req['name'], _req['price'])
        except KeyError:
            return {
                "error": "missing mandatory parameter 'name' and/or 'price'"
            }

        try:
            product['entity_id'] = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
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

        try:
            rsp = send_message('read-model', 'get_one_entity', {'name': 'billing', 'id': product_id})
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

signal.signal(signal.SIGINT, p.stop)
signal.signal(signal.SIGTERM, p.stop)

p.start()
