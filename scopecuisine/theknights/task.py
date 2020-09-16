from scopecuisine.theknights._task_modules.export_signals import ExportSignals
from scopecuisine.theknights._task_modules.in_stream import InStream
from scopecuisine.theknights._task_modules.timing import Timing
from scopecuisine.theknights._task_modules.triggers import Triggers
from scopecuisine.theknights._task_modules.out_stream import OutStream
from scopecuisine.theknights._task_modules.ai_channel_collection import (
    AIChannelCollection,
)
from scopecuisine.theknights._task_modules.ao_channel_collection import (
    AOChannelCollection,
)
from scopecuisine.theknights._task_modules.ci_channel_collection import (
    CIChannelCollection,
)
from scopecuisine.theknights._task_modules.co_channel_collection import (
    COChannelCollection,
)
from scopecuisine.theknights._task_modules.di_channel_collection import (
    DIChannelCollection,
)
from scopecuisine.theknights._task_modules.do_channel_collection import (
    DOChannelCollection,
)


class Task(object):
    """
    Represents a fake DAQmx Task.
    """

    def __init__(self, new_task_name="The_knights(who_say_NI)"):
        """
        Creates a fake DAQmx task.
        """
        self._handle = 0
        self.name = new_task_name
        self._initialize(self._handle)

    def __del__(self):
        pass

    def __enter__(self):
        return self

    def __eq__(self, other):
        return False

    def __exit__(self, type, value, traceback):
        self.close()

    def __hash__(self):
        return hash(self._handle)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Task(name={0})".format(self.name)

    @property
    def channels(self):

        return [0]

    @property
    def channel_names(self):

        return ["this_is_my_channel"]

    @property
    def number_of_channels(self):

        return 1

    @property
    def devices(self):

        pass

    @property
    def number_of_devices(self):

        return 1

    @property
    def ai_channels(self):

        return self._ai_channels

    @property
    def ao_channels(self):

        return self._ao_channels

    @property
    def ci_channels(self):

        return self._ci_channels

    @property
    def co_channels(self):

        return self._co_channels

    @property
    def di_channels(self):

        return self._di_channels

    @property
    def do_channels(self):

        return self._do_channels

    @property
    def export_signals(self):

        return self._export_signals

    @property
    def in_stream(self):

        return self._in_stream

    @property
    def out_stream(self):

        return self._out_stream

    @property
    def timing(self):

        return self._timing

    @property
    def triggers(self):

        return self._triggers

    def _initialize(self, task_handle):

        self._saved_name = self.name

        self._ai_channels = AIChannelCollection(task_handle)
        self._ao_channels = AOChannelCollection(task_handle)
        self._ci_channels = CIChannelCollection(task_handle)
        self._co_channels = COChannelCollection(task_handle)
        self._di_channels = DIChannelCollection(task_handle)
        self._do_channels = DOChannelCollection(task_handle)
        self._export_signals = ExportSignals(task_handle)
        self._in_stream = InStream(self)
        self._timing = Timing(task_handle)
        self._triggers = Triggers(task_handle)
        self._out_stream = OutStream(self)

        self._done_event_callbacks = []
        self._every_n_transferred_event_callbacks = []
        self._every_n_acquired_event_callbacks = []
        self._signal_event_callbacks = []

    def _calculate_num_samps_per_chan(self, num_samps_per_chan):

        return num_samps_per_chan

    def add_global_channels(self, global_channels):

        pass

    def close(self):

        self._handle = None

    def control(self, action):

        pass

    def is_task_done(self):

        return True

    def read(self, number_of_samples_per_channel=1, timeout=10.0):

        return [0.0] * number_of_samples_per_channel

    def register_done_event(self, callback_method):

        pass

    def register_every_n_samples_acquired_into_buffer_event(
        self, sample_interval, callback_method
    ):

        pass

    def register_every_n_samples_transferred_from_buffer_event(
        self, sample_interval, callback_method
    ):

        pass

    def register_signal_event(self, signal_type, callback_method):
        pass

    def save(
        self,
        save_as="",
        author="",
        overwrite_existing_task=False,
        allow_interactive_editing=True,
        allow_interactive_deletion=True,
    ):

        pass

    def start(self):

        pass

    def stop(self):

        pass

    def wait_until_done(self, timeout=10.0):

        pass

    def _raise_invalid_num_lines_error(self, num_lines_expected, num_lines_in_data):

        pass

    def _raise_invalid_write_num_chans_error(
        self, number_of_channels, number_of_channels_in_data
    ):

        pass

    def write(self, data, auto_start=None, timeout=10.0):

        pass
