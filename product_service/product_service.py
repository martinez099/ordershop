import atexit
import json
import logging
import uuid

from event_store.event_store_client import EventStore
from message_queue.message_queue_client import Receivers


class ProductService(object):
    """
    Product Service class.
    """

    def __init__(self):
        self.es = EventStore()
        self.rs = Receivers('product-service', [self.get_products,
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
            'id': str(uuid.uuid4()),
            'name': _name,
            'price': _price
        }

    def start(self):
        self.es.activate_entity_cache('product')
        atexit.register(self.es.deactivate_entity_cache, 'product')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def get_products(self, _req):

        try:
            product_id = json.loads(_req)['id']
        except KeyError:
            products = json.dumps([item for item in self.es.find_all('product')])
            return json.dumps(products)

        product = self.es.find_one('product', product_id)
        if not product:
            raise ValueError("could not find product")

        return json.dumps(product) if product else json.dumps(False)

    def post_products(self,_req):

        products = json.loads(_req)
        if not isinstance(products, list):
            products = [products]

        product_ids = []
        for product in products:
            try:
                new_product = ProductService.create_product(product['name'], product['price'])
            except KeyError:
                raise ValueError("missing mandatory parameter 'name' and/or 'price'")

            # trigger event
            self.es.publish('product', 'created', **new_product)

            product_ids.append(new_product['id'])

        return json.dumps(product_ids)

    def put_product(self, _req):

        product = json.loads(_req)
        try:
            product = ProductService.create_product(product['name'], product['price'])
        except KeyError:
            raise ValueError("missing mandatory parameter 'name' and/or 'price'")

        try:
            product_id = product['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        product['id'] = product_id

        # trigger event
        self.es.publish('product', 'updated', **product)

        return json.dumps(True)

    def delete_product(self, _req):

        try:
            product_id = json.loads(_req)['id']
        except KeyError:
            raise ValueError("missing mandatory parameter 'id'")

        product = self.es.find_one('product', product_id)
        if not product:
            raise ValueError("could not find product")

        # trigger event
        self.es.publish('product', 'deleted', **product)

        return json.dumps(True)


logging.basicConfig(level=logging.INFO)

p = ProductService()
p.start()
