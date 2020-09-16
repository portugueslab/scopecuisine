"""Main module."""
from queue import Empty


def get_last_parameters(parameter_queue, timeout=0.001):
    params = None
    while True:
        try:
            params = parameter_queue.get(timeout=timeout)
        except Empty:
            break
    return params
