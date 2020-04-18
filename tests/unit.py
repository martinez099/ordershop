import pprint
import time
import unittest
from urllib import request

from common import BASE_URL, create_carts, create_customers, create_inventories, create_orders, create_products, \
    get_result, http_cmd_req, get_any_id


class OrderShopTestCase(unittest.TestCase):
    """
    Test Case class.
    """

    def __init__(self, method_name='runTest'):
        super(OrderShopTestCase, self).__init__(method_name)

    def test_a_create_customers(self):

        # create customers
        customers = create_customers(10)
        http_cmd_req('{}/customers'.format(BASE_URL), customers)

        # digest async
        time.sleep(1)

        # check result
        rsp = request.urlopen('{}/customers'.format(BASE_URL))
        customers = get_result(rsp)
        self.assertEqual(len(customers), 10)

    def test_b_create_products(self):

        # create propducts
        products = create_products(10)
        http_cmd_req('{}/products'.format(BASE_URL), products)

        # digest async
        time.sleep(1)

        # check result
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        customers = get_result(rsp)
        self.assertEqual(len(customers), 10)

    def test_c_create_inventory(self):

        # get products
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        products = get_result(rsp)

        # create inventories
        inventories = create_inventories([product['entity_id'] for product in products], 100)
        http_cmd_req('{}/inventories'.format(BASE_URL), inventories)

        # digest async
        time.sleep(1)

        # check result
        rsp = request.urlopen('{}/inventories'.format(BASE_URL))
        customers = get_result(rsp)
        self.assertEqual(len(customers), 10)

    def test_d_create_carts(self):

        # get customers
        rsp = request.urlopen('{}/customers'.format(BASE_URL))
        customers = get_result(rsp)

        # get products
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        products = get_result(rsp)

        # create carts
        carts = create_carts(10, customers, products)
        http_cmd_req('{}/carts'.format(BASE_URL), carts)

        # digest async
        time.sleep(2)

        # check result
        rsp = request.urlopen('{}/carts'.format(BASE_URL))
        customers = get_result(rsp)
        self.assertEqual(len(customers), 10)

    def test_e_update_a_cart(self):

        # get carts
        rsp = request.urlopen('{}/carts'.format(BASE_URL))
        carts = get_result(rsp)

        # get products
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        products = get_result(rsp)

        # update second cart
        carts[1]['product_ids'][0] = get_any_id(products, carts[1]['product_ids'][0])
        rsp = http_cmd_req('{}/cart/{}'.format(BASE_URL, carts[1]['entity_id']), carts[1], 'PUT')
        updated = get_result(rsp)

        # check result
        self.assertTrue(updated)

        # double check result
        rsp = request.urlopen('{}/cart/{}'.format(BASE_URL, carts[1]['entity_id']))
        cart = get_result(rsp)
        self.assertIsNotNone(cart['product_ids'][0])
        self.assertEqual(carts[1]['product_ids'][0], cart['product_ids'][0])

    def test_f_create_orders(self):

        # get carts
        rsp = request.urlopen('{}/carts'.format(BASE_URL))
        carts = get_result(rsp)

        # create orders
        orders = create_orders(carts)
        http_cmd_req('{}/orders'.format(BASE_URL), orders)

        # digest async
        time.sleep(1)

        # check result
        rsp = request.urlopen('{}/orders'.format(BASE_URL))
        customers = get_result(rsp)
        self.assertEqual(len(customers), 10)

    def test_g_delete_an_order(self):

        # get orders
        rsp = request.urlopen('{}/orders'.format(BASE_URL))
        orders = get_result(rsp)

        # delete third order
        rsp = http_cmd_req('{}/order/{}'.format(BASE_URL, orders[2]['entity_id']), _method='DELETE')
        deleted = get_result(rsp)

        # check result
        self.assertTrue(deleted)

    def test_h_delete_a_customer(self):

        # get customers
        rsp = request.urlopen('{}/customers'.format(BASE_URL))
        customers = get_result(rsp)

        # delete third customer
        rsp = http_cmd_req('{}/customer/{}'.format(BASE_URL, customers[2]['entity_id']), _method='DELETE')
        deleted = get_result(rsp)

        # check result
        self.assertTrue(deleted)

    def test_i_perform_billing(self):

        # get orders
        rsp = request.urlopen('{}/orders'.format(BASE_URL))
        orders = get_result(rsp)

        # get cart of 1st order
        rsp = request.urlopen('{}/cart/{}'.format(BASE_URL, orders[0]['cart_id']))
        cart = get_result(rsp)

        # get products of cart
        rsps = [request.urlopen('{}/product/{}'.format(BASE_URL, product_id)) for product_id in cart['product_ids']]
        products = [get_result(rsp) for rsp in rsps]

        # calculate total amount
        amount = sum([int(product['price']) for product in products])

        # perform billing
        rsp = http_cmd_req('{}/billing'.format(BASE_URL), {'order_id': orders[0]['entity_id'], 'amount': amount})
        billing_id = get_result(rsp)

        # check result
        self.assertIsNotNone(billing_id)

    def test_j_get_unbilled_orders(self):

        # get unbilled orders
        rsp = request.urlopen('{}/orders/unbilled'.format(BASE_URL))
        unbilled_orders = get_result(rsp)

        # check result
        self.assertEqual(len(unbilled_orders), 8)

    def test_k_confirm_shipping(self):

        # get shippings
        rsp = request.urlopen('{}/shippings'.format(BASE_URL))
        shippings = get_result(rsp)

        # update first shipping
        shippings[0]['delivered'] = time.time()
        rsp = http_cmd_req('{}/shipping/{}'.format(BASE_URL, shippings[0]['entity_id']), shippings[0], 'PUT')
        updated = get_result(rsp)

        # check result
        self.assertTrue(updated)

    def test_l_get_unshipped_orders(self):

        # get unshipped orders
        rsp = request.urlopen('{}/orders/unshipped'.format(BASE_URL))
        unshipped_orders = get_result(rsp)

        # check result
        self.assertEqual(len(unshipped_orders), 8)

    def test_m_get_delivered_orders(self):

        # get delivered orders
        rsp = request.urlopen('{}/orders/delivered'.format(BASE_URL))
        delivered_orders = get_result(rsp)

        # check result
        self.assertEqual(len(delivered_orders), 1)

    def test_n_get_sent_mails(self):

        # get sent mails
        rsp = request.urlopen('{}/mails/sent'.format(BASE_URL))
        sent_mails = get_result(rsp)

        # check result
        self.assertEqual(len(sent_mails), 23)

    def test_z_print_report(self):

        # get customers
        rsp = request.urlopen('{}/report'.format(BASE_URL))
        report = get_result(rsp)

        # check result
        self.assertIsNotNone(report)

        # print result
        pprint.pprint(report)
