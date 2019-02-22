import time
import uuid


def create_customer(_name, _email):
    return {
        'id': str(uuid.uuid4()),
        'name': _name,
        'email': _email
    }


def create_billing(_order_id):
    return {
        'id': str(uuid.uuid4()),
        'order_id': _order_id,
        'done': time.time()
    }


def create_inventory(_product_id, _amount):
    return {
        'id': str(uuid.uuid4()),
        'product_id': _product_id,
        'amount': _amount
    }


def create_order(_product_ids, _customer_id):
    return {
        'id': str(uuid.uuid4()),
        'product_ids': _product_ids,
        'customer_id': _customer_id
    }


def create_product(_name, _price):
    return {
        'id': str(uuid.uuid4()),
        'name': _name,
        'price': _price
    }
