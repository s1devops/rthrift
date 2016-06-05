from thriftpy.server import TThreadedServer


class TThreadedServer_R(TThreadedServer):
    def __init__(self, *args, **kwargs):
        TThreadedServer.__init__(self, *args, **kwargs)
        self._close_actions = []

    def add_close_action(self, action):
        self._close_actions.append(action)

    def close(self):
        self.closed = True
        for action in self._close_actions:
            action()
