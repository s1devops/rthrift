from thriftpy.thrift import TClient

from .rclient import RClient
from .transport import TTransport_R, TBinaryProtocol_R

class TClient_R(TClient):
    def __init__(self, *args, **kwargs):
        TClient.__init__(self, *args, **kwargs)
        self._close_actions = []

    def add_close_action(self, action):
        self._close_actions.append(action)

    def close(self):
        for action in self._close_actions:
            action()


def get_client(service, uri, exchange='amq.topic', read_timeout=None):
    rmq_client = RClient(uri)
    c_transport = TTransport_R(rmq_client, TTransport_R.CLIENT, amqp_exchange=exchange)
    c_proto = TBinaryProtocol_R(c_transport, transport_mode=TTransport_R.CLIENT, service=service)
    client = TClient_R(service, c_proto)
    client.add_close_action(c_transport.close)
    client.add_close_action(c_transport.shutdown)

    if read_timeout is not None:
        c_transport.set_read_timeout(read_timeout)
    c_transport.open()

    return client
