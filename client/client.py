import random
import string
import unittest

import requests


BASE_URL = 'http://localhost:5000'


class OrderShopTestCase(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(OrderShopTestCase, self).__init__(method_name)

    @classmethod
    def setUpClass(cls):

        # clear state
        rsp = requests.post('{}/clear'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)

    @staticmethod
    def test_1_create_customers():

        # create customers
        customers = OrderShopTestCase.create_customers(10)
        rsp = requests.post('{}/customers'.format(BASE_URL), json=customers)
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/customers'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        assert len(customers) == len(rsp.json())

    @staticmethod
    def test_2_create_products():

        # create propducts
        products = OrderShopTestCase.create_products(10)
        rsp = requests.post('{}/products'.format(BASE_URL), json=products)
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/products'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        assert len(products) == len(rsp.json())

    @staticmethod
    def test_3_create_orders():

        # load customers
        rsp = requests.get('{}/customers'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        customers = rsp.json()

        # load products
        rsp = requests.get('{}/products'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        products = rsp.json()

        # create orders
        orders = OrderShopTestCase.create_orders(100, customers, products)
        rsp = requests.post('{}/orders'.format(BASE_URL), json=orders)
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/orders'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        assert len(orders) == len(rsp.json())

    @staticmethod
    def test_4_update_second_order():

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        orders = rsp.json()

        # load products
        rsp = requests.get('{}/products'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        products = rsp.json()

        # update second order
        orders[1]['product_ids'][0] = OrderShopTestCase.get_any_product_id(products, orders[1]['product_ids'][0])
        rsp = requests.put('{}/order/{}'.format(BASE_URL, orders[1]['id']), json=orders[1])
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/order/{}'.format(BASE_URL, orders[1]['id']))
        OrderShopTestCase.check_status_code(rsp)
        order = rsp.json()
        assert order['product_ids'][0]
        assert orders[1]['product_ids'][0] == order['product_ids'][0]

    @staticmethod
    def test_5_delete_third_order():

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        orders = rsp.json()

        # delete third order
        rsp = requests.delete('{}/order/{}'.format(BASE_URL, orders[2]['id']))
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/order/{}'.format(BASE_URL, orders[2]['id']))
        OrderShopTestCase.check_status_code(rsp)
        assert rsp.json() is False

    @staticmethod
    def test_6_delete_third_customer():

        # load customers
        rsp = requests.get('{}/customers'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        customers = rsp.json()

        # delete third customer
        rsp = requests.delete('{}/customer/{}'.format(BASE_URL, customers[2]['id']))
        OrderShopTestCase.check_status_code(rsp)

        # check result
        rsp = requests.get('{}/customer/{}'.format(BASE_URL, customers[2]['id']))
        OrderShopTestCase.check_status_code(rsp)
        assert rsp.json() is False

    @staticmethod
    def test_7_get_all_products_for_a_customer():

        # load customers
        rsp = requests.get('{}/customers'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        customers = rsp.json()

        # load orders
        rsp = requests.get('{}/orders'.format(BASE_URL))
        OrderShopTestCase.check_status_code(rsp)
        orders = rsp.json()

        # find all product ids for the first customer
        product_ids = []
        for order in orders:
            if customers[0]['id'] == order['customer_id']:
                product_ids.extend(order['product_ids'])

        # load products for these product ids
        products = []
        for product_id in product_ids:
            rsp = requests.get('{}/product/{}'.format(BASE_URL, product_id))
            OrderShopTestCase.check_status_code(rsp)
            products.append(rsp.json())

        # check result
        assert products
        assert len(products) == len(product_ids)

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
            customer_id = customers[idx]['id'] if customers[idx]['id'] != but else None
        return customer_id

    @staticmethod
    def get_any_product_id(products, but=None):
        product_id = None
        while not product_id:
            idx = random.randrange(len(products))
            product_id = products[idx]['id'] if products[idx]['id'] != but else None
        return product_id

    @staticmethod
    def check_status_code(response):
        if response.status_code != 200:
            raise Exception(str(response))
        return True
