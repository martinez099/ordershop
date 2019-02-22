import pprint
import random
import string
import unittest

import redis
import requests

from common.utils import check_rsp_code


BASE_URL = 'http://localhost:5000'


class OrderShopTestCase(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(OrderShopTestCase, self).__init__(method_name)

    @classmethod
    def setUpClass(cls):

        # clear state
        r = redis.StrictRedis(decode_responses=True)
        r.flushdb()

    @staticmethod
    def test_1_create_customers():

        # create customers
        customers = OrderShopTestCase.create_customers(10)
        rsp = requests.post('{}/customers'.format(BASE_URL), json=customers)
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/customers'.format(BASE_URL))
        check_rsp_code(rsp)
        assert len(customers) == len(rsp.json())

    @staticmethod
    def test_2_create_products():

        # create propducts
        products = OrderShopTestCase.create_products(10)
        rsp = requests.post('{}/products'.format(BASE_URL), json=products)
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/products'.format(BASE_URL))
        check_rsp_code(rsp)
        assert len(products) == len(rsp.json())

    @staticmethod
    def test_3_create_inventory():

        # load products
        rsp = requests.get('{}/products'.format(BASE_URL))
        check_rsp_code(rsp)
        products = rsp.json()

        # create inventory
        inventory = OrderShopTestCase.create_inventory([product['id'] for product in products], 100)
        rsp = requests.post('{}/inventory'.format(BASE_URL), json=inventory)
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/inventory'.format(BASE_URL))
        check_rsp_code(rsp)
        assert len(inventory) == len(rsp.json())

    @staticmethod
    def test_4_create_orders():

        # load customers
        rsp = requests.get('{}/customers'.format(BASE_URL))
        check_rsp_code(rsp)
        customers = rsp.json()

        # load products
        rsp = requests.get('{}/products'.format(BASE_URL))
        check_rsp_code(rsp)
        products = rsp.json()

        # create orders
        orders = OrderShopTestCase.create_orders(10, customers, products)
        ordered = 0
        for order in orders:
            rsp = requests.post('{}/orders'.format(BASE_URL), json=order)
            check_rsp_code(rsp)
            ordered += 1

        # check result
        rsp = requests.get('{}/orders'.format(BASE_URL))
        check_rsp_code(rsp)
        assert ordered == len(rsp.json())

    @staticmethod
    def test_5_update_second_order():

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        check_rsp_code(rsp)
        orders = rsp.json()

        # load products
        rsp = requests.get('{}/products'.format(BASE_URL))
        check_rsp_code(rsp)
        products = rsp.json()

        # update second order
        orders[1]['product_ids'][0] = OrderShopTestCase.get_any_product_id(products, orders[1]['product_ids'][0])
        rsp = requests.put('{}/order/{}'.format(BASE_URL, orders[1]['id']), json=orders[1])
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/order/{}'.format(BASE_URL, orders[1]['id']))
        check_rsp_code(rsp)
        order = rsp.json()
        assert order['product_ids'][0]
        assert orders[1]['product_ids'][0] == order['product_ids'][0]

    @staticmethod
    def test_6_delete_third_order():

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        check_rsp_code(rsp)
        orders = rsp.json()

        # delete third order
        rsp = requests.delete('{}/order/{}'.format(BASE_URL, orders[2]['id']))
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/order/{}'.format(BASE_URL, orders[2]['id']))
        check_rsp_code(rsp)
        assert rsp.json() is False

    @staticmethod
    def test_7_delete_third_customer():

        # load customers
        rsp = requests.get('{}/customers'.format(BASE_URL))
        check_rsp_code(rsp)
        customers = rsp.json()

        # delete third customer
        rsp = requests.delete('{}/customer/{}'.format(BASE_URL, customers[2]['id']))
        check_rsp_code(rsp)

        # check result
        rsp = requests.get('{}/customer/{}'.format(BASE_URL, customers[2]['id']))
        check_rsp_code(rsp)
        assert rsp.json() is False

    @staticmethod
    def test_8_perform_billing():

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        check_rsp_code(rsp)
        orders = rsp.json()

        # perform billing
        rsp = requests.post('{}/billing'.format(BASE_URL), json={"order_id": orders[0]['id']})
        check_rsp_code(rsp)

        # check result
        assert len(rsp.json())

    @staticmethod
    def test_9_get_unbilled_orders():

        # load unbilled orders
        rsp = requests.get('{}/orders/unbilled'.format(BASE_URL))
        check_rsp_code(rsp)
        unbilled = rsp.json()

        # check result
        assert len(unbilled) == 8

    @staticmethod
    def test_Z_print_report():

        # load customers
        rsp = requests.get('{}/report'.format(BASE_URL))
        check_rsp_code(rsp)
        report = rsp.json()

        # print result
        pprint.pprint(report)

    @staticmethod
    def create_customers(amount):
        customers = []
        for _ in range(amount):
            name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
            customers.append({
                "name": name.title(),
                "email": "{}@server.com".format(name)
            })
        return customers

    @staticmethod
    def create_products(amount):
        products = []
        for _ in range(amount):
            name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
            products.append({
                "name": name.title(),
                "price": random.randint(10, 1000)
            })
        return products

    @staticmethod
    def create_inventory(product_ids, amount):
        inventory = []
        for product_id in product_ids:
            inventory.append({
                "product_id": product_id,
                "amount": amount
            })
        return inventory

    @staticmethod
    def create_orders(amount, customers, products):
        orders = []
        for _ in range(amount):
            orders.append({
                "product_ids": [OrderShopTestCase.get_any_product_id(products) for _ in range(random.randint(1, 10))],
                "customer_id": OrderShopTestCase.get_any_customer_id(customers)
            })
        return orders

    @staticmethod
    def get_any_customer_id(customers, but=None):
        customer_id = None
        while not customer_id:
            idx = random.randrange(len(customers))
            customer = customers[idx]
            customer_id = customer['id'] if customer['id'] != but else None
        return customer_id

    @staticmethod
    def get_any_product_id(products, but=None):
        product_id = None
        while not product_id:
            idx = random.randrange(len(products))
            product = products[idx]
            product_id = product['id'] if product['id'] != but else None
        return product_id
