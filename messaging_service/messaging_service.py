import json
import logging

from message_queue.message_queue_client import Receivers


class MessagingService(object):
    """
    Messaging Service class.
    """

    def __init__(self):
        self.rs = Receivers('messaging-service', [self.send_email])

    def start(self):
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()

    def send_email(self, _req):

        values = json.loads(_req)
        if not values['to'] or not values['msg']:
            raise ValueError("missing mandatory parameter 'to' and/or 'msg'")

        logging.info('sent email with message "{}" to "{}"'.format(values['msg'], values['to']))
        return json.dumps(True)


logging.basicConfig(level=logging.INFO)

m = MessagingService()
m.start()
