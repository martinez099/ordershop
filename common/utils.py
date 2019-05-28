import sys
import traceback


def log_info(_msg):
    print('INFO: {}'.format(_msg), file=sys.stdout, flush=True)


def log_error(_err):
    print('ERROR: {}'.format(str(_err)), file=sys.stderr)
    traceback.print_exc()
    sys.stderr.flush()


def check_rsp_code(_rsp):
    if _rsp.status_code == 200:
        return _rsp.text
    else:
        raise Exception(str(_rsp))
