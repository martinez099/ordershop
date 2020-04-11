import logging
import time
from urllib import request

from common import BASE_URL, create_carts, create_customers, create_products, create_inventories, create_orders, \
    http_cmd_req, get_result


def test():

    logging.info("creating customers ...")
    customers = create_customers(10)
    http_cmd_req('{}/customers'.format(BASE_URL), customers)

    time.sleep(1)

    rsp = request.urlopen('{}/customers'.format(BASE_URL))
    customers = get_result(rsp)

    logging.info("creating products ...")
    products = create_products(100)
    http_cmd_req('{}/products'.format(BASE_URL), products)

    time.sleep(1)

    rsp = request.urlopen('{}/products'.format(BASE_URL))
    products = get_result(rsp)

    logging.info("creating inventory ...")
    inventories = create_inventories([product['entity_id'] for product in products], 100)
    http_cmd_req('{}/inventories'.format(BASE_URL), inventories)

    time.sleep(1)

    rsp = request.urlopen('{}/inventories'.format(BASE_URL))
    inventories = get_result(rsp)

    logging.info("creating carts ...")
    carts = create_carts(10, customers, products)
    http_cmd_req('{}/carts'.format(BASE_URL), carts)

    time.sleep(2)

    rsp = request.urlopen('{}/carts'.format(BASE_URL))
    carts = get_result(rsp)

    while True:

        logging.info("updating carts ...")

        for cart in carts:
            http_cmd_req('{}/cart/{}'.format(BASE_URL, cart['entity_id']), cart, _method='PUT')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test()
