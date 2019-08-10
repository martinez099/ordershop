import json
import sys
import traceback


def log_info(_msg):
    print('INFO: {}'.format(_msg), file=sys.stdout, flush=True)


def log_error(_err):
    print('ERROR: {}'.format(str(_err)), file=sys.stderr)
    traceback.print_exc()
    sys.stderr.flush()


def send_message(mq, service_name, func_name, params={}):
    log_info('sending message to {}.{}'.format(service_name, func_name))

    req_id = mq.send_req(service_name, func_name, json.dumps(params))
    rsp = mq.recv_rsp(service_name, func_name, req_id)
    mq.ack_rsp(service_name, func_name, req_id, rsp)

    return rsp
