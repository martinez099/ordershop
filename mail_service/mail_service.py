import logging
import signal

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers


class MailService(object):
    """
    Mail Service class.
    """
    def __init__(self):
        self.consumers = Consumers('mail-service', [self.send])
        self.event_store = EventStoreClient()

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.consumers.stop()
        logging.info('stopped.')

    def send(self, _req):
        if not _req['to'] or not _req['msg']:
            return {
                "error": "missing mandatory parameter 'to' and/or 'msg'"
            }

        # trigger event
        self.event_store.publish('mail', create_event('mail_sent', {"recipient": _req['to'], "message": _req['msg']}))


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

m = MailService()

signal.signal(signal.SIGINT, lambda n, h: m.stop())
signal.signal(signal.SIGTERM, lambda n, h: m.stop())

m.start()
