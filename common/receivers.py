import threading

from common.utils import log_info, log_error


class Receivers(object):
    """
    Receivers class.
    """

    def __init__(self, mq, service_name, handler_funcs):
        """
        :param mq: a message queue
        :param service_name: a service name
        :param handler_funcs: a list of handler functions
        """
        self.mq = mq
        self.service_name = service_name
        self.handler_funcs = handler_funcs
        self.threads = [threading.Thread(target=self._run,
                                         name='{}.{}'.format(service_name, m.__name__),
                                         args=(m,)) for m in handler_funcs]
        self.running = False

    def start(self):
        """
        Spawn a receiver thread for all handler functions.

        :return: None
        """
        log_info('spawning receivers ...')
        self.running = True
        [t.start() for t in self.threads]

    def stop(self):
        """
        Stop all receiver threads.

        :return: None
        """
        log_info('stopping receivers ...')
        self.running = False

    def wait(self):
        """
        Wait for all receiver threads to finsih. N.B. This is a blocking operation.

        :return: None
        """
        log_info('awaiting receivers ...')
        [t.join() for t in self.threads]

    def _run(self, handler_func):
        log_info('running receiver for {}.{}'.format(self.service_name, handler_func.__name__))

        while self.running:

            req_id, req_payload = self.mq.recv_req(self.service_name, handler_func.__name__, 1)
            if req_payload:
                try:
                    rsp = handler_func(req_payload)
                except Exception as e:
                    log_error(e)
                    continue

                self.mq.ack_req(self.service_name, handler_func.__name__, req_id)
                self.mq.send_rsp(self.service_name, handler_func.__name__, req_id, rsp)

        log_info('receiver for {}.{} stopped.'.format(self.service_name, handler_func.__name__))
