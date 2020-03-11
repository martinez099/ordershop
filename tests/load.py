import logging

from common import BASE_URL, create_customers, create_products, create_inventory, create_orders, http_cmd_req, \
    get_result


def merge(ids, entities):

    for i in range(0, len(ids)):
        entities[i]['entity_id'] = ids[i]

    return entities


def test():

    logging.info("creating customers ...")

    customers = create_customers(10)
    rsp = http_cmd_req('{}/customers'.format(BASE_URL), customers)
    customer_ids = get_result(rsp)
    merge(customer_ids, customers)

    logging.info("creating products ...")

    products = create_products(100)
    rsp = http_cmd_req('{}/products'.format(BASE_URL), products)
    product_ids = get_result(rsp)
    merge(product_ids, products)

    logging.info("creating inventory ...")

    inventory = create_inventory([product['entity_id'] for product in products], 100)
    rsp = http_cmd_req('{}/inventory'.format(BASE_URL), inventory)
    inventory_ids = get_result(rsp)
    merge(inventory_ids, inventory)

    while True:

        logging.info("creating orders ...")

        orders = create_orders(10, customers, products)
        rsp = http_cmd_req('{}/orders'.format(BASE_URL), orders)
        order_ids = get_result(rsp)
        merge(order_ids, orders)

        logging.info("created {} order.".format(len(order_ids)))

        logging.info("deleting orders ...")

        deleted = 0
        for i in range(0, 10):
            rsp = http_cmd_req('{}/order/{}'.format(BASE_URL, orders[i]['entity_id']), _method='DELETE')
            deleted += 1 if get_result(rsp) else 0

        logging.info("deleted {} orders.".format(deleted))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test()
