from .rabbit.client import RClient
from .thrift.transport import TTransport_R, TBinaryProtocol_R
from .thrift.client import TClient_R


def get_client(service, uri=None, exchange='amq.topic', read_timeout=None,
               transport_mode=TTransport_R.CLIENT, rmq_client=None):

    if rmq_client is None and uri is None:
        raise ValueError('either uri or rmq_client must be passed')

    if rmq_client is None:
        rmq_client = RClient(uri)

    c_transport = TTransport_R(rmq_client, transport_mode, amqp_exchange=exchange)
    c_proto = TBinaryProtocol_R(c_transport, transport_mode=transport_mode, service=service)
    client = TClient_R(service, c_proto)
    client.add_close_action(c_transport.close)
    client.add_close_action(c_transport.shutdown)

    if read_timeout is not None:
        c_transport.set_read_timeout(read_timeout)
    c_transport.open()

    return client
