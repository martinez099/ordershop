import pprint
import unittest

from urllib import request

from common import BASE_URL, create_carts, create_customers, create_inventory, create_orders, create_products, \
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
        rsp = http_cmd_req('{}/customers'.format(BASE_URL), customers)

        # check result
        customer_ids = get_result(rsp)
        self.assertEqual(len(customer_ids), len(customers))

    def test_b_create_products(self):

        # create propducts
        products = create_products(10)
        rsp = http_cmd_req('{}/products'.format(BASE_URL), products)

        # check result
        product_ids = get_result(rsp)
        self.assertEqual(len(product_ids), len(products))

    def test_c_create_inventory(self):

        # load products
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        products = get_result(rsp)

        # create inventory
        inventory = create_inventory([product['entity_id'] for product in products], 100)
        rsp = http_cmd_req('{}/inventory'.format(BASE_URL), inventory)

        # check result
        inventory_ids = get_result(rsp)
        self.assertEqual(len(inventory_ids), len(inventory))

    def test_d_create_carts(self):

        # load customers
        rsp = request.urlopen('{}/customers'.format(BASE_URL))
        customers = get_result(rsp)

        # load products
        rsp = request.urlopen('{}/products'.format(BASE_URL))
        products = get_result(rsp)

        # create carts
        carts = create_carts(10, customers, products)
        created = 0
        for cart in carts:
            rsp = http_cmd_req('{}/carts'.format(BASE_URL), cart)
            order_ids = get_result(rsp)
            created += len(order_ids)

        # check result
        self.assertEqual(created, len(carts))

    def test_e_create_orders(self):

        # load carts
        rsp = request.urlopen('{}/carts'.format(BASE_URL))
        carts = get_result(rsp)

        # create orders
        orders = create_orders(carts)
        ordered = 0
        for order in orders:
            rsp = http_cmd_req('{}/orders'.format(BASE_URL), order)
            order_ids = get_result(rsp)
            ordered += len(order_ids)

        # check result
        self.assertEqual(ordered, len(orders))

    def test_f_update_second_cart(self):

        # load carts
        rsp = request.urlopen('{}/carts'.format(BASE_URL))
        carts = get_result(rsp)

        # load products
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

    def test_g_delete_third_order(self):

        # load orders
        rsp = request.urlopen('{}/orders'.format(BASE_URL))
        orders = get_result(rsp)

        # delete third order
        rsp = http_cmd_req('{}/order/{}'.format(BASE_URL, orders[2]['entity_id']), _method='DELETE')
        deleted = get_result(rsp)

        # check result
        self.assertTrue(deleted)

    def test_h_delete_third_customer(self):

        # load customers
        rsp = request.urlopen('{}/customers'.format(BASE_URL))
        customers = get_result(rsp)

        # delete third customer
        rsp = http_cmd_req('{}/customer/{}'.format(BASE_URL, customers[2]['entity_id']), _method='DELETE')
        deleted = get_result(rsp)

        # check result
        self.assertTrue(deleted)

    def test_i_perform_billing(self):

        # load orders
        rsp = request.urlopen('{}/orders'.format(BASE_URL))
        orders = get_result(rsp)

        # perform billing
        rsp = http_cmd_req('{}/billing'.format(BASE_URL), {'order_id': orders[0]['entity_id'], 'method': 'CC'})
        billing_id = get_result(rsp)

        # check result
        self.assertIsNotNone(billing_id)

    def test_j_get_unbilled_orders(self):

        # load unbilled orders
        rsp = request.urlopen('{}/orders/unbilled'.format(BASE_URL))
        unbilled_orders = get_result(rsp)

        # check result
        self.assertEqual(len(unbilled_orders), 8)

    def test_k_confirm_shipping(self):

        # load shippings
        rsp = request.urlopen('{}/shippings'.format(BASE_URL))
        shippings = get_result(rsp)

        # update first shipping
        shippings[0]['done'] = True
        rsp = http_cmd_req('{}/shipping/{}'.format(BASE_URL, shippings[0]['entity_id']), shippings[0], 'PUT')
        updated = get_result(rsp)

        # check result
        self.assertTrue(updated)

    def test_l_get_unshipped_orders(self):

        # load unshipped orders
        rsp = request.urlopen('{}/orders/unshipped'.format(BASE_URL))
        unshipped_orders = get_result(rsp)

        # check result
        self.assertEqual(len(unshipped_orders), 8)

    def test_z_print_report(self):

        # load customers
        rsp = request.urlopen('{}/report'.format(BASE_URL))
        report = get_result(rsp)

        # check result
        self.assertIsNotNone(report)

        # print result
        pprint.pprint(report)
