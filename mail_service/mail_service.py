import logging
import signal

from message_queue.message_queue_client import Consumers


class MailService(object):
    """
    Mail Service class.
    """

    def __init__(self):
        self.rs = Consumers('mail-service', [self.send_email])

    def start(self):
        logging.info('starting ...')
        self.rs.start()
        self.rs.wait()

    def stop(self):
        self.rs.stop()
        logging.info('stopped.')

    def send_email(self, _req):

        if not _req['to'] or not _req['msg']:
            return {
                "error": "missing mandatory parameter 'to' and/or 'msg'"
            }

        logging.info('sent email with message "{}" to "{}"'.format(_req['msg'], _req['to']))
        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

m = MailService()

signal.signal(signal.SIGINT, lambda n, h: m.stop())
signal.signal(signal.SIGTERM, lambda n, h: m.stop())

m.start()
