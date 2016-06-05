from thriftpy.thrift import TClient

class TClient_R(TClient):
    def __init__(self, *args, **kwargs):
        TClient.__init__(self, *args, **kwargs)
        self._close_actions = []

    def add_close_action(self, action):
        self._close_actions.append(action)

    def close(self):
        for action in self._close_actions:
            action()
