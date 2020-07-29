
class COChannelCollection(object):
    def __init__(self, task_handle):
        self.task_handle = task_handle
        self.name_chs = []
        self.line_groupings = []
        self._handle = 0
        self._task = 0

    def add_co_chan(self, name="", line_grouping=None):
        self.name_chs.append(name)
        self.line_groupings.append(line_grouping)
