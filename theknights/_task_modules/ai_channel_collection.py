class AIChannelCollection(object):
    def __init__(self, task_handle):
        self.task_handle = task_handle
        self.name_chs = []
        self._handle = 0
        self._task = 0

    def add_ai_voltage_chan(self, name="", min_val=-1, max_val=1):
        
        self.name_chs.append(name)
