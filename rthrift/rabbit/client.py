from queue import Queue
from uuid import uuid4
import threading

import rabbitpy

from .packets import (PacketTypes, CommandPacketReceive,
                      CommandPacketConsume, CommandPacketShutdown,
                      CommandPacketPublish)


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
                for _, target_queue in reply_queues.items():
                    target_queue.put(cmd)
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
        for thread in threads:
            thread.start()
        self._threads = threads

    def consumer(self, message_action, no_ack=False, **kwargs):
        rmq_queue = rabbitpy.Queue(self.channel, **kwargs)
        rmq_queue.declare()
        queue_id = str(uuid4())
        def consume_func():
            for msg in rmq_queue.consume(no_ack=no_ack):
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
                    rmq_queue.stop_consuming()
                    return
                elif cmd.TYPE == PacketTypes.RECEIVE:
                    result = message_action(cmd.msg)
                    if result is False:
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
        #self.channel._read_queue.put(pamqp.specification.Basic.Cancel())
        #self.channel.close()

        for thread in self._threads:
            thread.join()
        self.cmd_queue.join()
        #self.conn.close()

    def publish(self, message, exchange_name, routing_key):
        cmd = CommandPacketPublish(message, exchange=exchange_name, routing_key=routing_key)
        self.cmd_queue.put(cmd)
