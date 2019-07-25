import json

from common.utils import log_info, create_receivers
from message_queue.message_queue_client import MessageQueue


def send_email(_req, _mq):

    values = json.loads(_req)
    if not values['to'] or not values['msg']:
        raise ValueError("missing mandatory parameter 'to' and/or 'msg'")

    log_info('sent email with message "{}" to "{}"'.format(values['msg'], values['to']))
    return json.dumps(True)


mq = MessageQueue()

threads = create_receivers(mq, 'messaging-service', [send_email])

log_info('spawning servers ...')

[t.start() for t in threads]
[t.join() for t in threads]

log_info('done.')
