import json
import sys
import traceback


def log_info(_msg):
    print('INFO: {}'.format(_msg), file=sys.stdout, flush=True)


def log_error(_err):
    print('ERROR: {}'.format(str(_err)), file=sys.stderr)
    traceback.print_exc()
    sys.stderr.flush()


def run_receiver(mq, service_name, func_name, handler_func):

    running = True
    while running:

        req = mq.recv_req(service_name, func_name)
        if not req:
            continue

        rsp = handler_func(req, mq)

        mq.ack_req(service_name, func_name)
        mq.send_rsp(service_name, func_name, rsp)


def do_send(mq, service_name, func_name, params):
    req_id = mq.send_req(service_name, func_name, json.dumps(params))
    rsp = mq.recv_rsp(service_name, func_name, req_id)
    mq.ack_rsp(service_name, func_name, rsp)

    return rsp
