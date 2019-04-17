import json
import pprint
import random
import string
import unittest
import urllib.request

import redis


BASE_URL = 'http://localhost:5000'


def http_cmd_req(_url, _data=None, _method='POST'):
    """
    Do a HTTP request.

    :param _url: The URL of the request.
    :param _data: The JSON payload.
    :param _method: The HTTP method, defaults to POST.
    :return: The response.
    """
    if _data:
        data = json.dumps(_data).encode('utf-8')
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Length': len(data)
        }
        req = urllib.request.Request(_url, data=data, headers=headers, method=_method)
    else:
        req = urllib.request.Request(_url, method=_method)
    return urllib.request.urlopen(req)


def check_rsp(_rsp):
    """
    Check HTTP response code.

    :param _rsp: The HTTP response.
    :return: The response body.
    """
    if _rsp.code == 200:
        return _rsp.read()
    else:
        raise Exception(str(_rsp))


def create_customers(amount):
    """
    Create an amount of random customers.

    :param amount: The amount of customers.
    :return: The generated customers.
    """
    customers = []
    for _ in range(amount):
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
        customers.append({
            "name": name.title(),
            "email": "{}@server.com".format(name)
        })
    return customers


def create_products(amount):
    """
    Create an amount of random products.

    :param amount: The amount of products.
    :return: The generated products.
    """
    products = []
    for _ in range(amount):
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
        products.append({
            "name": name.title(),
            "price": random.randint(10, 1000)
        })
    return products


def create_inventory(product_ids, amount):
    """
    Create an amount of random inventories.

    :param product_ids: The ID of the product the inventory refers to.
    :param amount: The amount of products in the inventory.
    :return: The generated inventory.
    """
    inventory = []
    for product_id in product_ids:
        inventory.append({
            "product_id": product_id,
            "amount": amount
        })
    return inventory


def create_orders(amount, customers, products):
    """
    Create an amount of random orders.
    
    :param amount: The amount of orders.
    :param customers: The customers of the order.
    :param products: The products of the order.
    :return: 
    """
    orders = []
    for _ in range(amount):
        orders.append({
            "product_ids": [get_any_id(products) for _ in range(random.randint(1, 10))],
            "customer_id": get_any_id(customers)
        })
    return orders


def get_any_id(_entities, _but=None):
    """
    Get a random id out of entities.
    
    :param _entities: The entities to chose from.
    :param _but: Exclude this entity.
    :return: The randomly chosen ID.
    """
    _id = None
    while not _id:
        idx = random.randrange(len(_entities))
        entity = _entities[idx]
        _id = entity['id'] if entity['id'] != _but else None
    return _id


class OrderShopTestCase(unittest.TestCase):
    """
    Test Case class.
    """

    def __init__(self, method_name='runTest'):
        super(OrderShopTestCase, self).__init__(method_name)

    @classmethod
    def setUpClass(cls):

        # clear state
        r = redis.StrictRedis(decode_responses=True)
        r.flushdb()

    def test_1_create_customers(self):

        # create customers
        customers = create_customers(10)
        rsp = http_cmd_req('{}/customers'.format(BASE_URL), customers)
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/customers'.format(BASE_URL))
        rsp = check_rsp(rsp)
        self.assertEqual(len(customers), len(json.loads(rsp)))

    def test_2_create_products(self):

        # create propducts
        products = create_products(10)
        rsp = http_cmd_req('{}/products'.format(BASE_URL), products)
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/products'.format(BASE_URL))
        rsp = check_rsp(rsp)
        self.assertEqual(len(products), len(json.loads(rsp)))

    def test_3_create_inventory(self):

        # load products
        rsp = urllib.request.urlopen('{}/products'.format(BASE_URL))
        rsp = check_rsp(rsp)
        products = json.loads(rsp)

        # create inventory
        inventory = create_inventory([product['id'] for product in products], 100)
        rsp = http_cmd_req('{}/inventory'.format(BASE_URL), inventory)
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/inventory'.format(BASE_URL))
        rsp = check_rsp(rsp)
        self.assertEqual(len(inventory), len(json.loads(rsp)))

    def test_4_create_orders(self):

        # load customers
        rsp = urllib.request.urlopen('{}/customers'.format(BASE_URL))
        rsp = check_rsp(rsp)
        customers = json.loads(rsp)

        # load products
        rsp = urllib.request.urlopen('{}/products'.format(BASE_URL))
        rsp = check_rsp(rsp)
        products = json.loads(rsp)

        # create orders
        orders = create_orders(10, customers, products)
        ordered = 0
        for order in orders:
            rsp = http_cmd_req('{}/orders'.format(BASE_URL), order)
            check_rsp(rsp)
            ordered += 1

        # check result
        rsp = urllib.request.urlopen('{}/orders'.format(BASE_URL))
        rsp = check_rsp(rsp)
        self.assertEqual(ordered, len(json.loads(rsp)))

    def test_5_update_second_order(self):

        # load orders
        rsp = urllib.request.urlopen('{}/orders'.format(BASE_URL))
        rsp = check_rsp(rsp)
        orders = json.loads(rsp)

        # load products
        rsp = urllib.request.urlopen('{}/products'.format(BASE_URL))
        rsp = check_rsp(rsp)
        products = json.loads(rsp)

        # update second order
        orders[1]['product_ids'][0] = get_any_id(products, orders[1]['product_ids'][0])
        rsp = http_cmd_req('{}/order/{}'.format(BASE_URL, orders[1]['id']), orders[1], 'PUT')
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/order/{}'.format(BASE_URL, orders[1]['id']))
        rsp = check_rsp(rsp)
        order = json.loads(rsp)
        self.assertIsNotNone(order['product_ids'][0])
        self.assertEqual(orders[1]['product_ids'][0], order['product_ids'][0])

    def test_6_delete_third_order(self):

        # load orders
        rsp = urllib.request.urlopen('{}/orders'.format(BASE_URL))
        rsp = check_rsp(rsp)
        orders = json.loads(rsp)

        # delete third order
        rsp = http_cmd_req('{}/order/{}'.format(BASE_URL, orders[2]['id']), _method='DELETE')
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/order/{}'.format(BASE_URL, orders[2]['id']))
        rsp = check_rsp(rsp)
        self.assertIsNone(json.loads(rsp))

    def test_7_delete_third_customer(self):

        # load customers
        rsp = urllib.request.urlopen('{}/customers'.format(BASE_URL))
        rsp = check_rsp(rsp)
        customers = json.loads(rsp)

        # delete third customer
        rsp = http_cmd_req('{}/customer/{}'.format(BASE_URL, customers[2]['id']), _method='DELETE')
        check_rsp(rsp)

        # check result
        rsp = urllib.request.urlopen('{}/customer/{}'.format(BASE_URL, customers[2]['id']))
        rsp = check_rsp(rsp)
        self.assertIsNone(json.loads(rsp))

    def test_8_perform_billing(self):

        # load orders
        rsp = urllib.request.urlopen('{}/orders'.format(BASE_URL))
        rsp = check_rsp(rsp)
        orders = json.loads(rsp)

        # perform billing
        rsp = http_cmd_req('{}/billing'.format(BASE_URL), {"order_id": orders[0]['id']})
        rsp = check_rsp(rsp)

        # check result
        self.assertIsNotNone(len(json.loads(rsp)))

    def test_9_get_unbilled_orders(self):

        # load unbilled orders
        rsp = urllib.request.urlopen('{}/orders/unbilled'.format(BASE_URL))
        rsp = check_rsp(rsp)
        unbilled = json.loads(rsp)

        # check result
        self.assertEqual(len(unbilled), 8)

    def test_Z_print_report(self):

        # load customers
        rsp = urllib.request.urlopen('{}/report'.format(BASE_URL))
        rsp = check_rsp(rsp)

        # print result
        pprint.pprint(json.loads(rsp))

        # check result
        self.assertIsNotNone(rsp)
