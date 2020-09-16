class Timing(object):
    def __init__(self, task_handle):
        self.task_handle = task_handle
        self._handle = 0
        self._task = 0

    def cfg_samp_clk_timing(
        self,
        rate=0.2,
        source="OnboardClock",
        active_edge=None,
        sample_mode=None,
        samps_per_chan=100,
    ):
        pass
