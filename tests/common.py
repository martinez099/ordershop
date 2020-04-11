import json
import random
import string

from urllib import request


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
        req = request.Request(_url, data=data, headers=headers, method=_method)
    else:
        req = request.Request(_url, method=_method)

    return request.urlopen(req)


def get_result(_rsp):
    """
    Check HTTP response code and get the JSON result.

    :param _rsp: The HTTP response.
    :return: The result.
    :raise Exception: In case of an error.
    """
    if _rsp.code != 200:
        raise Exception(str(_rsp))

    rsp = json.loads(_rsp.read())
    if 'error' in rsp:
        raise Exception(rsp['error'])

    return rsp['result']


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


def create_inventories(product_ids, amount):
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


def create_carts(amount, customers, products):
    """
    Create an amount of random carts.

    :param amount: The amount of carts.
    :param customers: The customers of the cart.
    :param products: The products of the cart.
    :return:
    """
    orders = []
    for _ in range(amount):
        orders.append({
            "customer_id": get_any_id(customers),
            "product_ids": [get_any_id(products) for _ in range(random.randint(1, 10))],
        })

    return orders


def create_orders(carts):
    """
    Create an amount of random orders.

    :param carts: Carts for the orders.
    :return:
    """
    orders = []
    for cart in carts:
        orders.append({
            "cart_id": cart['entity_id']
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
        entity = _entities[random.randrange(len(_entities))]
        _id = entity['entity_id'] if entity['entity_id'] != _but else None

    return _id
