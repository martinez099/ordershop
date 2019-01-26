import sys
import traceback


def log_info(_msg):
    print('INFO: {}'.format(_msg))


def log_error(_err):
    print('ERROR: {}'.format(str(_err)))
    traceback.print_exc()
    sys.stdout.flush()
    sys.stderr.flush()


def is_key(_value):
    return '_' in _value and ':' in _value


def check_rsp(_rsp):
    if _rsp.status_code == 200:
        return _rsp.text
    else:
        raise Exception(str(_rsp))
