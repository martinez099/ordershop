import atexit
import functools
import logging
import uuid

from event_store.event_store_client import EventStoreClient, create_event, deduce_entities, track_entities
from message_queue.message_queue_client import Receivers


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
        self.products = deduce_entities(self.event_store.get('product'))
        self.tracking_handler = functools.partial(track_entities, self.products)

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
        self.event_store.subscribe('product', self.tracking_handler)
        atexit.register(self.stop)
        self.receivers.start()
        self.receivers.wait()

    def stop(self):
        self.event_store.unsubscribe('product', self.tracking_handler)
        self.receivers.stop()
        logging.info('stopped.')

    def get_products(self, _req):

        try:
            product_id = _req['entity_id']
        except KeyError:
            return {
                "result": list(self.products.values())
            }

        product = self.products.get(product_id)
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

        product = self.event_store.find_one('product', product_id)
        if not product:
            return {
                "error": "could not find product"
            }

        # trigger event
        self.event_store.publish('product', create_event('entity_deleted', product))

        return {
            "result": True
        }


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)-6s] %(message)s')

p = ProductService()
p.start()
