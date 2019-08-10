import json

from common.utils import log_info
from common.receivers import Receivers
from message_queue.message_queue_client import MessageQueue


def send_email(_req, _mq):

    values = json.loads(_req)
    if not values['to'] or not values['msg']:
        raise ValueError("missing mandatory parameter 'to' and/or 'msg'")

    log_info('sent email with message "{}" to "{}"'.format(values['msg'], values['to']))
    return json.dumps(True)


mq = MessageQueue()

rs = Receivers(mq, 'messaging-service', [send_email])

rs.start()
rs.wait()
