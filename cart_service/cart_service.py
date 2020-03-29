import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class CartService(object):
    """
    Cart Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('cart-service', [self.create_carts,
                                                    self.update_cart,
                                                    self.delete_cart])

    @staticmethod
    def _create_entity(_customer_id, _product_ids):
        """
        Create a cart entity.

        :param _customer_id: The customer ID.
        :param _product_ids: The product IDs.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'customer_id': _customer_id,
            'product_ids': _product_ids
        }

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.consumers.stop()
        logging.info('stopped.')

    def create_carts(self, _req):

        carts = _req if isinstance(_req, list) else [_req]
        cart_ids = []

        for cart in carts:
            try:
                new_cart = CartService._create_entity(cart['customer_id'], cart['product_ids'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'customer_id' and/or 'product_ids'"
                }

            # trigger event
            self.event_store.publish('cart', create_event('entity_created', new_cart))

            cart_ids.append(new_cart['entity_id'])

        return {
            "result": cart_ids
        }

    def update_cart(self, _req):

        try:
            cart_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'cart', 'id': cart_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        cart = rsp['result']
        if not cart:
            return {
                "error": "could not find cart"
            }

        # set new props
        cart['entity_id'] = cart_id
        try:
            cart['customer_id'] = _req['customer_id']
            cart['product_ids'] = _req['product_ids']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'customer_id' and/or 'product_ids"
            }

        # trigger event
        self.event_store.publish('cart', create_event('entity_updated', cart))

        return {
            "result": True
        }

    def delete_cart(self, _req):

        try:
            cart_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_one_entity', {'name': 'cart', 'id': cart_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        cart = rsp['result']
        if not cart:
            return {
                "error": "could not find cart"
            }

        # trigger event
        self.event_store.publish('cart', create_event('entity_deleted', cart))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

p = CartService()

signal.signal(signal.SIGINT, lambda n, h: p.stop())
signal.signal(signal.SIGTERM, lambda n, h: p.stop())

p.start()
