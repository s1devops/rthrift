import rabbitpy
import threading
import pamqp.specification
from queue import Queue
from uuid import uuid4

class Message(object):
    def __init__(self, body_value, properties=None, auto_id=False, opinionated=False):
        """Create a new instance of the Message object."""
        # Always have a dict of properties set
        self.properties = properties or {}

        self.body_value = body_value
        self.auto_id = auto_id
        self.opinionated = opinionated

    def to_rmq_msg(self, channel):
        return rabbitpy.Message(channel, self.body_value, self.properties, self.auto_id, self.opinionated)

class PacketTypes(object):
    UNKNOWN = 0x1
    PUBLISH = 0x2
    RECEIVE = 0x3
    CONSUME = 0x4
    SHUTDOWN = 0x5


class CommandPacket(object):
    TYPE = PacketTypes.UNKNOWN

    def __repr__(self):
        t = {PacketTypes.UNKNOWN: 'UNKNOWN',
             PacketTypes.PUBLISH: 'PUBLISH',
             PacketTypes.RECEIVE: 'RECEIVE',
             PacketTypes.CONSUME: 'CONSUME',
             PacketTypes.SHUTDOWN: 'SHUTDOWN',
         }[self.TYPE]
        return 'CommandPacket ({0})'.format(t)

class CommandPacketPublish(CommandPacket):
    TYPE = PacketTypes.PUBLISH

    def __init__(self, msg, exchange, routing_key='', mandatory=False):
        self.msg = msg
        self.exchange = exchange
        self.routing_key = routing_key
        self.mandatory = mandatory


class CommandPacketConsume(CommandPacket):
    TYPE = PacketTypes.CONSUME

    def __init__(self, queue_id, reply_queue = None):
        self.queue_id = queue_id
        self.reply_queue = reply_queue or Queue()

class CommandPacketReceive(CommandPacket):
    TYPE = PacketTypes.RECEIVE

    def __init__(self, msg, queue_id):
        self.msg = msg
        self.queue_id = queue_id

class CommandPacketShutdown(CommandPacket):
    TYPE = PacketTypes.SHUTDOWN


class RClient(object):

    def __init__(self, server_uri):
        self.server_uri = server_uri

        self._conn = None

        self._channel = None

        self.thread_actions = []
        self._threads = []

        self.cmd_queue = Queue()

    def _cmd_consume(self):
        reply_queues = {}
        while True:
            cmd = self.cmd_queue.get()
            self.cmd_queue.task_done()
            if cmd.TYPE == PacketTypes.PUBLISH:
                msg = cmd.msg.to_rmq_msg(self.channel)
                msg.publish(cmd.exchange, cmd.routing_key, cmd.mandatory)
            elif cmd.TYPE == PacketTypes.SHUTDOWN:
                #self.channel.close()
                for k, q in reply_queues.items():
                    q.put(cmd)
                reply_queues = {}
                return
            elif cmd.TYPE == PacketTypes.RECEIVE:
                if cmd.queue_id in reply_queues:
                    reply_queues[cmd.queue_id].put(cmd)
                else:
                    raise Exception('todo, unrouted packets')
            elif cmd.TYPE == PacketTypes.CONSUME:
                reply_queues[cmd.queue_id] = cmd.reply_queue
            else:
                raise Exception('unknown packet type')

    @property
    def conn(self):
        if self._conn is None:
            self._conn = rabbitpy.Connection(self.server_uri)
        return self._conn

    @property
    def channel(self):
        if self._channel is None:
            channel = self.conn.channel()
            self._channel = channel

        return self._channel

    def add_thread_action(self, func):
        self.thread_actions.append(func)

    def start(self):
        self.thread_actions.append(self._cmd_consume)
        threads = [threading.Thread(target=func) for func in self.thread_actions]
        [thread.start() for thread in threads]
        self._threads = threads

    def consumer(self, message_action, no_ack=False, **kwargs):
        rmq_queue = rabbitpy.Queue(self.channel, **kwargs)
        rmq_queue.declare()
        queue_id = str(uuid4())
        def consume_func():
            for msg in rmq_queue.consume_messages(no_ack=no_ack):
                if msg is None:
                    return
                self.cmd_queue.put(CommandPacketReceive(msg, queue_id))
        self.add_thread_action(consume_func)

        reply_queue = Queue()
        def action_func():
            while True:
                cmd = reply_queue.get()
                reply_queue.task_done()
                if cmd.TYPE == PacketTypes.SHUTDOWN:
                    return
                elif cmd.TYPE == PacketTypes.RECEIVE:
                    result = message_action(cmd.msg)
                    if result == False:
                        return
                else:
                    raise Exception('Unknown packet type in action_func')
        self.add_thread_action(action_func)

        cmd = CommandPacketConsume(queue_id, reply_queue)
        self.cmd_queue.put(cmd)

        return rmq_queue


    def shutdown(self):
        self.cmd_queue.put(CommandPacketShutdown())
        #Ugly, but the only reliable way to close the consumer from any thread
        self.channel._read_queue.put(pamqp.specification.Basic.CancelOk())
        #self.channel.close()

        #[thread.join() for thread in self._threads]
        self.cmd_queue.join()
        self.conn.close()

    def publish(self, message, exchange_name, routing_key):
        cmd = CommandPacketPublish(message, exchange=exchange_name, routing_key = routing_key)
        self.cmd_queue.put(cmd)
