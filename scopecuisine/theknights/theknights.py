"""Main module."""


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

    def __exit__(self):
        self.close()

    def __hash__(self):
        return hash(self._handle)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Task(name={0})".format(self.name)

    @property
    def channels(self):
        """
        :class:`nidaqmx._task_modules.channels.channel.Channel`: Specifies
            a channel object that represents the entire list of virtual
            channels in this task.
        """
        return [0]

    @property
    def channel_names(self):
        """
        List[str]: Indicates the names of all virtual channels in the task.
        """
        return ["this_is_my_channel"]

    @property
    def number_of_channels(self):
        """
        int: Indicates the number of virtual channels in the task.
        """
        return 1

    @property
    def devices(self):
        """
        List[:class:`nidaqmx.system.device.Device`]: Indicates a list
            of Device objects representing all the devices in the task.
        """

        pass

    @property
    def number_of_devices(self):
        """
        int: Indicates the number of devices in the task.
        """

        return 1

    @property
    def ai_channels(self):
        """
        :class:`nidaqmx._task_modules.ai_channel_collection.AIChannelCollection`:
            Gets the collection of analog input channels for this task.
        """
        return 1

    @property
    def ao_channels(self):
        """
        :class:`nidaqmx._task_modules.ao_channel_collection.AOChannelCollection`:
            Gets the collection of analog output channels for this task.
        """
        return self._ao_channels

    @property
    def ci_channels(self):
        """
        :class:`nidaqmx._task_modules.ci_channel_collection.CIChannelCollection`:
            Gets the collection of counter input channels for this task.
        """
        return self._ci_channels

    @property
    def co_channels(self):
        """
        :class:`nidaqmx._task_modules.co_channel_collection.COChannelCollection`:
            Gets the collection of counter output channels for this task.
        """
        return self._co_channels

    @property
    def di_channels(self):
        """
        :class:`nidaqmx._task_modules.di_channel_collection.DIChannelCollection`:
            Gets the collection of digital input channels for this task.
        """
        return self._di_channels

    @property
    def do_channels(self):
        """
        :class:`nidaqmx._task_modules.do_channel_collection.DOChannelCollection`:
            Gets the collection of digital output channels for this task.
        """
        return self._do_channels

    @property
    def export_signals(self):
        """
        :class:`nidaqmx._task_modules.export_signals.ExportSignals`: Gets the
            exported signal configurations for the task.
        """
        return self._export_signals

    @property
    def in_stream(self):
        """
        :class:`nidaqmx._task_modules.in_stream.InStream`: Gets the read
            configurations for the task.
        """
        return self._in_stream

    @property
    def out_stream(self):
        """
        :class:`nidaqmx._task_modules.out_stream.OutStream`: Gets the
            write configurations for the task.
        """
        return self._out_stream

    @property
    def timing(self):
        """
        :class:`nidaqmx._task_modules.timing.Timing`: Gets the timing
            configurations for the task.
        """
        return self._timing

    @property
    def triggers(self):
        """
        :class:`nidaqmx._task_modules.triggers.Triggers`: Gets the trigger
            configurations for the task.
        """
        return self._triggers

    def _initialize(self, task_handle):
        """
        Instantiates and populates various attributes used by this task.

        Args:
            task_handle (TaskHandle): Specifies the handle for this task.
        """
        # Saved name is used in self.close() to throw graceful error on
        # double closes.
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

        # These lists keep C callback objects in memory as ctypes doesn't.
        # Program will crash if callback is made after object is garbage
        # collected.
        self._done_event_callbacks = []
        self._every_n_transferred_event_callbacks = []
        self._every_n_acquired_event_callbacks = []
        self._signal_event_callbacks = []

    def _calculate_num_samps_per_chan(self, num_samps_per_chan):
        """
        Calculates the actual number of samples per channel to read.

        This method is necessary because the number of samples per channel
        can be set to NUM_SAMPLES_UNSET or -1, where each value entails a
        different method of calculating the actual number of samples per
        channel to read.

        Args:
            num_samps_per_chan (int): Specifies the number of samples per
                channel.
        """
        return 1

    def add_global_channels(self, global_channels):
        """
        Adds global virtual channels from MAX to the given task.

        Args:
            global_channels (List[nidaqmx.system.storage.persisted_channel.PersistedChannel]):
                Specifies the channels to add to the task.

                These channels must be valid channels available from MAX.
                If you pass an invalid channel, NI-DAQmx returns an error.
                This value is ignored if it is empty.
        """
        pass

    def close(self):
        """
        Clears the task.

        Before clearing, this method aborts the task, if necessary,
        and releases any resources the task reserved. You cannot use a task
        after you clear it unless you recreate the task.

        If you create a DAQmx Task object within a loop, use this method
        within the loop after you are finished with the task to avoid
        allocating unnecessary memory.
        """
        self._handle = None

    def control(self, action):
        """
        Alters the state of a task according to the action you specify.

        Args:
            action (nidaqmx.constants.TaskMode): Specifies how to alter
                the task state.
        """
        pass

    def is_task_done(self):
        """
        Queries the status of the task and indicates if it completed
        execution. Use this function to ensure that the specified
        operation is complete before you stop the task.

        Returns:
            bool:

            Indicates if the measurement or generation completed.
        """

        return True

    def read(self, number_of_samples_per_channel=1, timeout=10.0):
        """
        Reads samples from the task or virtual channels you specify.

        This read method is dynamic, and is capable of inferring an appropriate
        return type based on these factors:
        - The channel type of the task.
        - The number of channels to read.
        - The number of samples per channel.

        The data type of the samples returned is independently determined by
        the channel type of the task.

        For digital input measurements, the data type of the samples returned
        is determined by the line grouping format of the digital lines.
        If the line grouping format is set to "one channel for all lines", the
        data type of the samples returned is int. If the line grouping
        format is set to "one channel per line", the data type of the samples
        returned is boolean.

        If you do not set the number of samples per channel, this method
        assumes one sample was requested. This method then returns either a
        scalar (1 channel to read) or a list (N channels to read).

        If you set the number of samples per channel to ANY value (even 1),
        this method assumes multiple samples were requested. This method then
        returns either a list (1 channel to read) or a list of lists (N
        channels to read).

        Args:
            number_of_samples_per_channel (Optional[int]): Specifies the
                number of samples to read. If this input is not set,
                assumes samples to read is 1. Conversely, if this input
                is set, assumes there are multiple samples to read.

                If you set this input to nidaqmx.constants.
                READ_ALL_AVAILABLE, NI-DAQmx determines how many samples
                to read based on if the task acquires samples
                continuously or acquires a finite number of samples.

                If the task acquires samples continuously and you set
                this input to nidaqmx.constants.READ_ALL_AVAILABLE, this
                method reads all the samples currently available in the
                buffer.

                If the task acquires a finite number of samples and you
                set this input to nidaqmx.constants.READ_ALL_AVAILABLE,
                the method waits for the task to acquire all requested
                samples, then reads those samples. If you set the
                "read_all_avail_samp" property to True, the method reads
                the samples currently available in the buffer and does
                not wait for the task to acquire all requested samples.
            timeout (Optional[float]): Specifies the amount of time in
                seconds to wait for samples to become available. If the
                time elapses, the method returns an error and any
                samples read before the timeout elapsed. The default
                timeout is 10 seconds. If you set timeout to
                nidaqmx.constants.WAIT_INFINITELY, the method waits
                indefinitely. If you set timeout to 0, the method tries
                once to read the requested samples and returns an error
                if it is unable to.
        Returns:
            dynamic:

            The samples requested in the form of a scalar, a list, or a
            list of lists. See method docstring for more info.

            NI-DAQmx scales the data to the units of the measurement,
            including any custom scaling you apply to the channels. Use a
            DAQmx Create Channel method to specify these units.

        Example:
            >>> task = Task()
            >>> task.ai_channels.add_voltage_channel('Dev1/ai0:3')
            >>> data = task.read()
            >>> type(data)
            <type 'list'>
            >>> type(data[0])
            <type 'float'>
        """

        return [0.0] * number_of_samples_per_channel

    def register_done_event(self, callback_method):
        """
        Registers a callback function to receive an event when a task stops due
        to an error or when a finite acquisition task or finite generation task
        completes execution. A Done event does not occur when a task is stopped
        explicitly, such as by calling DAQmx Stop Task.

        Args:
            callback_method (function): Specifies the function that you want
                DAQmx to call when the event occurs. The function you pass in
                this parameter must have the following prototype:

                >>> def callback(task_handle, status, callback_data):
                >>>     return 0

                Upon entry to the callback, the task_handle parameter contains
                the handle to the task on which the event occurred. The status
                parameter contains the status of the task when the event
                occurred. If the status value is negative, it indicates an
                error. If the status value is zero, it indicates no error.
                If the status value is positive, it indicates a warning. The
                callbackData parameter contains the value you passed in the
                callbackData parameter of this function.

                Passing None for this parameter unregisters the event callback
                function.
        """
        pass

    def register_every_n_samples_acquired_into_buffer_event(
        self, sample_interval, callback_method
    ):
        """
        Registers a callback function to receive an event when the specified
        number of samples is written from the device to the buffer. This
        function only works with devices that support buffered tasks.

        When you stop a task explicitly any pending events are discarded. For
        example, if you call DAQmx Stop Task then you do not receive any
        pending events.

        Args:
            sample_interval (int): Specifies the number of samples after
                which each event should occur.
            callback_method (function): Specifies the function that you want
                DAQmx to call when the event occurs. The function you pass in
                this parameter must have the following prototype:

                >>> def callback(task_handle, every_n_samples_event_type,
                >>>         number_of_samples, callback_data):
                >>>     return 0

                Upon entry to the callback, the task_handle parameter contains
                the handle to the task on which the event occurred. The
                every_n_samples_event_type parameter contains the
                EveryNSamplesEventType.ACQUIRED_INTO_BUFFER value. The
                number_of_samples parameter contains the value you passed in
                the sample_interval parameter of this function. The
                callback_data parameter contains the value you passed in the
                callback_data parameter of this function.

                Passing None for this parameter unregisters the event callback
                function.
        """
        pass

    def register_every_n_samples_transferred_from_buffer_event(
        self, sample_interval, callback_method
    ):
        """
        Registers a callback function to receive an event when the specified
        number of samples is written from the buffer to the device. This
        function only works with devices that support buffered tasks.

        When you stop a task explicitly any pending events are discarded. For
        example, if you call DAQmx Stop Task then you do not receive any
        pending events.

        Args:
            sample_interval (int): Specifies the number of samples after
                which each event should occur.
            callback_method (function): Specifies the function that you want
                DAQmx to call when the event occurs. The function you pass in
                this parameter must have the following prototype:

                >>> def callback(task_handle, every_n_samples_event_type,
                >>>         number_of_samples, callback_data):
                >>>     return 0

                Upon entry to the callback, the task_handle parameter contains
                the handle to the task on which the event occurred. The
                every_n_samples_event_type parameter contains the
                EveryNSamplesEventType.TRANSFERRED_FROM_BUFFER value. The
                number_of_samples parameter contains the value you passed in
                the sample_interval parameter of this function. The
                callback_data parameter contains the value you passed in the
                callback_data parameter of this function.

                Passing None for this parameter unregisters the event callback
                function.
        """
        pass

    def register_signal_event(self, signal_type, callback_method):
        """
        Registers a callback function to receive an event when the specified
        hardware event occurs.

        When you stop a task explicitly any pending events are discarded. For
        example, if you call DAQmx Stop Task then you do not receive any
        pending events.

        Args:
            signal_type (nidaqmx.constants.Signal): Specifies the type of
                signal for which you want to receive results.
            callback_method (function): Specifies the function that you want
                DAQmx to call when the event occurs. The function you pass in
                this parameter must have the following prototype:

                >>> def callback(task_handle, signal_type, callback_data):
                >>>     return 0

                Upon entry to the callback, the task_handle parameter contains
                the handle to the task on which the event occurred. The
                signal_type parameter contains the integer value you passed in
                the signal_type parameter of this function. The callback_data
                parameter contains the value you passed in the callback_data
                parameter of this function.

                Passing None for this parameter unregisters the event callback
                function.
        """
        pass

    def save(
        self,
        save_as="",
        author="",
        overwrite_existing_task=False,
        allow_interactive_editing=True,
        allow_interactive_deletion=True,
    ):
        """
        Saves this task and any local channels it contains to MAX.

        This function does not save global channels. Use the DAQmx Save
        Global Channel function to save global channels.

        Args:
            save_as (Optional[str]): Is the name to save the task,
                global channel, or custom scale as. If you do not
                specify a value for this input, NI-DAQmx uses the name
                currently assigned to the task, global channel, or
                custom scale.
            author (Optional[str]): Is a name to store with the task,
                global channel, or custom scale.
            overwrite_existing_task (Optional[bool]): Specifies whether to
                overwrite a task of the same name if one is already saved in
                MAX. If this input is False and a task of the same name is
                already saved in MAX, this function returns an error.
            allow_interactive_editing (Optional[bool]): Specifies whether to
                allow the task, global channel, or custom scale to be edited
                in the DAQ Assistant. If allow_interactive_editing is True,
                the DAQ Assistant must support all task or global channel
                settings.
            allow_interactive_deletion (Optional[bool]): Specifies whether
                to allow the task, global channel, or custom scale to be
                deleted through MAX.
        """
        options = 0
        if overwrite_existing_task:
            options |= _Save.OVERWRITE.value
        if allow_interactive_editing:
            options |= _Save.ALLOW_INTERACTIVE_EDITING.value
        if allow_interactive_deletion:
            options |= _Save.ALLOW_INTERACTIVE_DELETION.value

        cfunc = lib_importer.windll.DAQmxSaveTask
        cfunc.argtypes = [
            lib_importer.task_handle,
            ctypes_byte_str,
            ctypes_byte_str,
            ctypes.c_uint,
        ]

        error_code = cfunc(self._handle, save_as, author, options)
        check_for_error(error_code)

    def start(self):
        """
        Transitions the task to the running state to begin the measurement
        or generation. Using this method is required for some applications and
        is optional for others.

        If you do not use this method, a measurement task starts automatically
        when the DAQmx Read method runs. The autostart input of the DAQmx Write
        method determines if a generation task starts automatically when the
        DAQmx Write method runs.

        If you do not use the DAQmx Start Task method and the DAQmx Stop Task
        method when you use the DAQmx Read method or the DAQmx Write method
        multiple times, such as in a loop, the task starts and stops
        repeatedly. Starting and stopping a task repeatedly reduces the
        performance of the application.
        """
        pass

    def stop(self):
        """
        Stops the task and returns it to the state the task was in before the
        DAQmx Start Task method ran or the DAQmx Write method ran with the
        autostart input set to TRUE.

        If you do not use the DAQmx Start Task method and the DAQmx Stop Task
        method when you use the DAQmx Read method or the DAQmx Write method
        multiple times, such as in a loop, the task starts and stops
        repeatedly. Starting and stopping a task repeatedly reduces the
        performance of the application.
        """
        pass

    def wait_until_done(self, timeout=10.0):
        """
        Waits for the measurement or generation to complete.

        Use this method to ensure that the specified operation is complete
        before you stop the task.

        Args:
            timeout (Optional[float]): Specifies the maximum amount of time in
                seconds to wait for the measurement or generation to complete.
                This method returns an error if the time elapses. The
                default is 10. If you set timeout (sec) to
                nidaqmx.WAIT_INFINITELY, the method waits indefinitely. If you
                set timeout (sec) to 0, the method checks once and returns
                an error if the measurement or generation is not done.
        """
        pass

    def _raise_invalid_num_lines_error(self, num_lines_expected, num_lines_in_data):
        pass

    def _raise_invalid_write_num_chans_error(
        self, number_of_channels, number_of_channels_in_data
    ):

        pass

    def write(self, data, auto_start=None, timeout=10.0):
        """
        Writes samples to the task or virtual channels you specify.

        This write method is dynamic, and is capable of accepting the
        samples to write in the various forms for most operations:

        - Scalar: Single sample for 1 channel.
        - List/1D numpy.ndarray: Multiple samples for 1 channel or 1
          sample for multiple channels.
        - List of lists/2D numpy.ndarray: Multiple samples for multiple
          channels.

        The data type of the samples passed in must be appropriate for
        the channel type of the task.

        For counter output pulse operations, this write method only
        accepts samples in these forms:

        - Scalar CtrFreq, CtrTime, CtrTick (from nidaqmx.types):
          Single sample for 1 channel.
        - List of CtrFreq, CtrTime, CtrTick (from nidaqmx.types):
          Multiple samples for 1 channel or 1 sample for multiple
          channels.

        If the task uses on-demand timing, this method returns only
        after the device generates all samples. On-demand is the default
        timing type if you do not use the timing property on the task to
        configure a sample timing type. If the task uses any timing type
        other than on-demand, this method returns immediately and does
        not wait for the device to generate all samples. Your
        application must determine if the task is done to ensure that
        the device generated all samples.

        Args:
            data (dynamic): Contains the samples to write to the task.

                The data you write must be in the units of the
                generation, including any custom scales. Use the DAQmx
                Create Channel methods to specify these units.
            auto_start (Optional[bool]): Specifies if this method
                automatically starts the task if you did not explicitly
                start it with the DAQmx Start Task method.

                The default value of this parameter depends on whether
                you specify one sample or many samples to write to each
                channel. If one sample per channel was specified, the
                default value is True. If multiple samples per channel
                were specified, the default value is False.
            timeout (Optional[float]): Specifies the amount of time in
                seconds to wait for the method to write all samples.
                NI-DAQmx performs a timeout check only if the method
                must wait before it writes data. This method returns an
                error if the time elapses. The default timeout is 10
                seconds. If you set timeout to
                nidaqmx.constants.WAIT_INFINITELY, the method waits
                indefinitely. If you set timeout to 0, the method tries
                once to write the submitted samples. If the method could
                not write all the submitted samples, it returns an error
                and the number of samples successfully written.
        Returns:
            int:

            Specifies the actual number of samples this method
            successfully wrote.
        """

    pass
