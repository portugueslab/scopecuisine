
class StartTrigger(object):
    def __init__(self):
        self.names = []
        self._handle = 0
        self._task = 0

    def cfg_dig_edge_start_trig(self, name, edge=None):

        self.names.append(name)
