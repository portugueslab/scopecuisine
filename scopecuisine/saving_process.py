from multiprocessing import Process, Event, Queue
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from queue import Empty
import flammkuchen as fl
import numpy as np
import shutil
import json
from arrayqueues.shared_arrays import ArrayQueue
from functools import wraps


B_TO_MB = 1048576


@dataclass
class SavingParameters:
    output_dir: Path = Path.home()
    n_planes: int = 1
    n_volumes: int = 10000
    chunk_size: int = 20
    optimal_chunk_MB_RAM: int = 200  # TODO check this random number
    volumerate: float = 1
    voxel_size: tuple = (1, 1, 1)


@dataclass
class SavingStatus:
    target_params: SavingParameters
    i_in_chunk: int = 0
    i_volume: int = 0
    i_chunk: int = 0

#TODO maybe parameters that are sent via a queue could be come properties that
# automatically retrieve from the queue until empty?
def pass_if_empty(method):
    """Decorator for a method retrieving data from a queue, catching Empty error.
    """
    @wraps(method)
    def decorated(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Empty:
            return

    return decorated


class StackSaver(Process):
    """A Process that takes volumes from an input queue and save them in a SplitDataset
    or similar format.

    Parameters
    ----------
    stop_event : Event obj
        Event that, when set, will stop the process to wrap up.
    duration_queue : Queue obj
        Queue where the duration of the experiment (in n of volumes) is updated.
    max_queue_size : int
        Maximum queue size, in #TODO ?

    Queues
    ------

    Events
    ------
    """
    def __init__(self, stop_event, duration_queue, max_queue_size=2000):

        super().__init__()
        self.stop_event = stop_event
        self.saving_event = Event()
        self.saver_stopped_event = Event()

        self.duration_queue = duration_queue
        self.save_queue = ArrayQueue(max_mbytes=max_queue_size)
        self.saving_parameter_queue = Queue()
        self.saved_status_queue = Queue()

        self.save_parameters: Optional[SavingParameters] = SavingParameters()

        # Some counters:
        self.i_in_chunk = 0
        self.i_chunk = 0
        self.i_plane = 0
        self.i_volume = 0
        self.current_data = None
        self.frame_shape = None
        self.dtype = np.uint16
        self.saving = False

    def run(self):
        """Process loop.
        """
        while not self.stop_event.is_set():
            # Enter here when starting signal is set:
            if self.saving_event.is_set() and self.save_parameters is not None:
                self.save_loop()
            # Otherwise, simply keep updated the parameters
            else:
                self.receive_parameters()
                self.receive_duration()

    @property
    def output_dir(self):
        output_dir = Path(self.save_parameters.output_dir) / "original"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def save_loop(self):
        """Main saving loop.
        """

        # Start-up code
        self.i_in_chunk = 0
        self.i_chunk = 0
        self.i_volume = 0
        self.current_data = None

        # remove files if some are found at the save location:
        if (self.output_dir / "stack_metadata.json").is_file():
            shutil.rmtree(self.output_dir)

        # Main saving loop, until stack is completed or acquisition is aborted:
        while (
            self.i_volume < self.save_parameters.n_volumes
            and self.saving_event.is_set()
            and not self.stop_event.is_set()  # TODO duplications of state signals here?
        ):
            # TODO do we want these parameters to potentially change during the run?
            self.receive_parameters()
            self.receive_duration()

            self.fill_dataset(self.receive_frame)

        # If at least one volume was acquired, finalize the dataset:
        if self.i_volume > 0:
            # Save leftover volumes:
            if self.i_in_chunk != 0:
                self.save_chunk()
            self.update_saved_status_queue()
            self.finalize_dataset()

        # Finally, reset everything:
        self.current_data = None
        self.saving_event.clear()
        self.save_parameters = None
        self.saver_stopped_event.set()

    def fill_dataset(self, volume):
        if self.current_data is None:
            self.calculate_optimal_size(volume)
            self.current_data = np.empty(
                (self.save_parameters.chunk_size, *volume.shape), dtype=self.dtype,
            )

        self.current_data[self.i_in_chunk, :, :, :] = volume

        self.i_volume += 1
        self.i_in_chunk += 1
        self.update_saved_status_queue()

        if self.i_in_chunk == self.save_parameters.chunk_size:
            self.save_chunk()

    def update_saved_status_queue(self):
        self.saved_status_queue.put(
            SavingStatus(
                target_params=self.save_parameters,
                i_in_chunk=self.i_in_chunk,
                i_chunk=self.i_chunk,
                i_volume=self.i_volume,
            )
        )

    def finalize_dataset(self):
        """Write metadata json for the SplitDataset.
        """
        with open(self.output_dir / "stack_metadata.json", "w") as f:
            json.dump(
                {
                    "shape_full": (
                        self.save_parameters.n_volumes,
                        *self.current_data.shape[1:],
                    ),
                    "shape_block": (
                        self.save_parameters.chunk_size,
                        *self.current_data.shape[1:],
                    ),
                    "crop_start": [0, 0, 0, 0],
                    "crop_end": [0, 0, 0, 0],
                    "padding": [0, 0, 0, 0],
                    "voxel_size": self.save_parameters.voxel_size,
                },
                f,
            )

    def save_chunk(self):
        """Save new block in the SplitDataset.
        """
        fl.save(self.output_dir / "{:04d}.h5".format(self.i_chunk),
                dict(stack=self.current_data[: self.i_in_chunk, :, :, :]),
                compression="blosc",
        )
        self.i_in_chunk = 0
        self.i_chunk += 1

    def calculate_optimal_size(self, volume):
        if self.dtype == np.uint16:
            array_megabytes = (
                2 * volume.shape[0] * volume.shape[1] * volume.shape[2] / B_TO_MB
            )
        else:
            raise TypeError("Saving data type not supported. Only uint16 is supported")
        self.save_parameters.chunk_size = int(
            self.save_parameters.optimal_chunk_MB_RAM / array_megabytes
        )

    @pass_if_empty
    def receive_frame(self):
        return self.save_queue.get(timeout=0.01)

    @pass_if_empty
    def receive_parameters(self):
        self.save_parameters = self.saving_parameter_queue.get(timeout=0.001)

    @pass_if_empty
    def receive_duration(self):
        new_duration = self.duration_queue.get(timeout=0.001)
        self.save_parameters.n_volumes = int(
            np.ceil(self.save_parameters.volumerate * new_duration)
        )
