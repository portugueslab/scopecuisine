import numpy as np
from time import sleep


class ChannelWriterBase(object):
    """
    Defines base class for all NI-DAQmx stream writers.
    """

    def __init__(self, task_out_stream, auto_start=None):
        self._out_stream = task_out_stream
        self._task = task_out_stream._task
        self._handle = task_out_stream._task._handle
        self._verify_array_shape = True
        self._auto_start = auto_start


class AnalogMultiChannelWriter(ChannelWriterBase):
    def write_many_sample(self, buffer, timeout=10.0):
        sleep(1)
        data_in = np.random.rand(buffer.shape[1])
        buffer[0, :] = data_in
