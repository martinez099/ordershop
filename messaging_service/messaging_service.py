import json

import gevent
from gevent import monkey
monkey.patch_all()

from common.utils import log_info, run_receiver
from message_queue.message_queue_client import MessageQueue


def send_email(req):

    values = json.loads(req)
    if not values['to'] or not values['msg']:
        raise ValueError("missing mandatory parameter 'to' and/or 'msg'")

    log_info('sent email with message "{}" to "{}"'.format(values['msg'], values['to']))
    return json.dumps(True)


mq = MessageQueue()

gevent.joinall([
    gevent.spawn(run_receiver, mq, 'messaging-service', 'send-email', send_email),
])
