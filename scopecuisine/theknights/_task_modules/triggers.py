from scopecuisine.theknights._task_modules.triggering.start_trigger import StartTrigger


class Triggers(object):
    def __init__(self, task_handle):
        self.task_handle = task_handle
        self.start_trigger = StartTrigger()
        self._handle = 0
        self._task = 0
