import numpy as np
from time import sleep

class ChannelReaderBase(object):

    def __init__(self, task_in_stream):

        self._in_stream = task_in_stream
        self._task = task_in_stream._task
        self._handle = task_in_stream._task._handle
        self._verify_array_shape = True


class AnalogMultiChannelReader(ChannelReaderBase):

    def read_many_sample(self, buffer, number_of_samples_per_channel=1, timeout=10.0):
        sleep(1)
        data_in = np.random.rand(number_of_samples_per_channel)
        buffer[0, :] = data_in

class AnalogSingleChannelReader(ChannelReaderBase):

    def read_many_sample(self, buffer, number_of_samples_per_channel=1, timeout=10.0):
        sleep(1)
        data_in = np.random.rand(number_of_samples_per_channel)
        buffer[:] = data_in

