from io import BytesIO
from queue import Queue, Empty as QueueEmpty
from uuid import uuid4

from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TTransportBase, TTransportException

from ..rabbit.message import Message


class TTransportFactory_R(object):
    def get_transport(self, trans):
        return trans

class TBinaryProtocolFactory_R(object):
    def __init__(self, strict_read=True, strict_write=True):
        self.strict_read = strict_read
        self.strict_write = strict_write

    def get_protocol(self, trans):
        return TBinaryProtocol_R(trans, self.strict_read, self.strict_write)

class TBinaryProtocol_R(TBinaryProtocol):

    def __init__(self, trans, strict_read=True, strict_write=True,
                 transport_mode=None, service=None):
        super().__init__(trans, strict_read=strict_read, strict_write=strict_write)

        self.transport_mode = transport_mode
        self.service = service

    def write_message_begin(self, name, ttype, seqid):
        if self.transport_mode in [TTransport_R.BROADCAST_SENDER, TTransport_R.CLIENT]:
            self.trans.rpc_function_name = '{}.{}.{}'.format(
                self.service.__module__, self.service.__name__, name)
        super().write_message_begin(name, ttype, seqid)


class TTransport_Dummy(object):
    def excp(self):
        raise TTransportException('rmq is closed')

    def listen(self):
        self.excp()

    def read(self, _):
        raise TTransportException('rmq is closed')

    def close(self):
        pass

class TTransport_R(TTransportBase):
    CLIENT = 'CLIENT'
    SERVER = 'SERVER'
    BROADCAST_SENDER = 'BROADCAST_SENDER'
    BROADCAST_LISTENER = 'BROADCAST_LISTENER'

    CLOSED = 'CLOSED'
    OPEN = 'OPEN'

    def __init__(self, amqp_client, role=None, amqp_exchange=None,
                 amqp_queue=None, routing_keys=None):
        if role is None:
            role = self.CLIENT
        self._amqp_client = amqp_client
        self.amqp_exchange = amqp_exchange
        self.amqp_queue = amqp_queue

        self._status = self.CLOSED

        self._accept_count = 0

        self._wbuf = None
        self._rbuf = None

        self._rpc_function_name = None
        if routing_keys is None:
            routing_keys = []
        self._routing_keys = routing_keys


        self._rpc_msg_id = None
        self._reply_to = None
        self._role = role

        self._rpc_queue = None

        self._read_timeout = None

        self._channel_id = None

    def set_read_timeout(self, timeout):
        self._read_timeout = timeout


    def _bind_queue(self, rmq_queue):
        for routing_key in self._routing_keys:
                results = rmq_queue.bind(self.amqp_exchange, routing_key)


    def listen(self):
        if self._role == self.SERVER:
            self._channel_id = self._amqp_client.consumer(self.msg_recv, setup_func=self._bind_queue, name=self.amqp_queue)
        elif self._role == self.BROADCAST_LISTENER:
            self._channel_id = self._amqp_client.consumer(self.msg_recv,
                                       setup_func=self._bind_queue,
                                       name=self.amqp_queue,
                                       no_ack=True,
                                       exclusive=True,
                                       auto_delete=True)
        self._amqp_client.start()
        self._status = self.OPEN

    def accept(self):
        #self._read_msg()
        if self._accept_count < 1:
            self._accept_count += 1
            return self
        else:
            for thread in self._amqp_client._threads:
                thread.join()
            return TTransport_Dummy()

    def open(self):
        if self._role == self.CLIENT:
            channel_id = self._amqp_client.consumer(self.msg_recv,
                                                    no_ack=True,
                                                    name='amq.rabbitmq.reply-to')
            self._channel_id = channel_id
        if self._role == self.CLIENT or self._role == self.BROADCAST_SENDER:
            self._status = self.OPEN

    def close(self):
        pass

    def shutdown(self):
        if self._status == self.CLOSED:
            return
        self._status = self.CLOSED
        #self._amqp_client.shutdown()
        self.rpc_queue.put(None)

    @property
    def wbuf(self):
        if self._wbuf is None:
            self._wbuf = BytesIO()
        return self._wbuf

    @property
    def rbuf(self):
        if self._rbuf is None:
            self._rbuf = BytesIO()
        return self._rbuf

    @property
    def rpc_function_name(self):
        return self._rpc_function_name

    @rpc_function_name.setter
    def rpc_function_name(self, value):
        self._rpc_function_name = value

    @property
    def rpc_queue(self):
        if self._rpc_queue is None:
            self._rpc_queue = Queue()
        return self._rpc_queue

    #def set_rpc_func(self, name):
    #    self._rpc_func = name

    @property
    def rpc_msg_id(self):
        return self._rpc_msg_id

    @rpc_msg_id.setter
    def rpc_msg_id(self, id):
        self._rpc_msg_id = id

    def wbuf_reset(self):
        self._wbuf = BytesIO()

    def write(self, buf):
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (write)')
        self.wbuf.write(buf)

    def msg_recv(self, msg):
        self.rpc_queue.put(msg)

        if self._role not in [self.CLIENT, self.BROADCAST_LISTENER] and msg is not None:
            msg.ack()

    def _read_msg(self):
        try:
            while True:
                msg = self.rpc_queue.get(timeout=self._read_timeout)
                if msg is None:
                    return False
                if self._role == self.SERVER or self._role == self.BROADCAST_LISTENER:
                    if 'correlation_id' in msg.properties:
                        self.rpc_msg_id = msg.properties['correlation_id']
                    self._reply_to = msg.properties['reply_to']
                    self._rbuf = BytesIO(msg.body)
                    return True
                elif self._role == self.CLIENT:
                    if self.rpc_msg_id is None \
                       or self.rpc_msg_id == msg.properties['correlation_id']:
                        self._rbuf = BytesIO(msg.body)
                        return True

        except QueueEmpty:
            return False

    def read(self, sz):
        data = self.__read(sz)
        return data

    def __read(self, sz):
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (read)')

        ret = self.rbuf.read(sz)
        if len(ret) != 0:
            return ret

        if not self._read_msg():
            raise TTransportException('RMQ read failed')
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (read)')

        return self.rbuf.read(sz)

    def flush(self):
        msg = self.wbuf.getvalue()
        self.wbuf_reset()

        if self._role == self.CLIENT:
            self.rpc_msg_id = str(uuid4()).encode('utf8')

        properties = {
            'content_type': 'application/x-thrift',
            'correlation_id': self.rpc_msg_id,
        }

        message = Message(msg, properties)

        if self._role == self.CLIENT:
            properties['reply_to'] = 'amq.rabbitmq.reply-to'
            exchange = self.amqp_exchange
            routing_key = self.rpc_function_name
            self._amqp_client.publish(message, exchange, routing_key, channel_id=self._channel_id)
        elif self._role == self.BROADCAST_SENDER:
            exchange = self.amqp_exchange
            routing_key = self.rpc_function_name
            self._amqp_client.publish(message, exchange, routing_key, channel_id=self._channel_id)
        elif self._role == self.SERVER:
            self._amqp_client.publish(message, "", self._reply_to, channel_id=self._channel_id)
