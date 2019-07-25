import json
import sys
import traceback
import threading


def log_info(_msg):
    print('INFO: {}'.format(_msg), file=sys.stdout, flush=True)


def log_error(_err):
    print('ERROR: {}'.format(str(_err)), file=sys.stderr)
    traceback.print_exc()
    sys.stderr.flush()


def create_receivers(mq, service_name, handler_funcs):
    return [threading.Thread(target=run_receiver,
                             name='{}.{}'.format(service_name, m.__name__),
                             args=(mq, service_name, m.__name__, m)) for m in handler_funcs]


def run_receiver(mq, service_name, func_name, handler_func):
    log_info('running receiver for {}.{}'.format(service_name, func_name))

    while True:

        req_id, req_payload = mq.recv_req(service_name, func_name)
        try:
            rsp = handler_func(req_payload, mq)
        except Exception as e:
            log_error(e)
            continue

        mq.ack_req(service_name, func_name, req_id)
        mq.send_rsp(service_name, func_name, req_id, rsp)


def send_message(mq, service_name, func_name, params={}):
    log_info('sending message to {}.{}'.format(service_name, func_name))

    req_id = mq.send_req(service_name, func_name, json.dumps(params))
    rsp = mq.recv_rsp(service_name, func_name, req_id)
    mq.ack_rsp(service_name, func_name, req_id, rsp)

    return rsp
