from collections.abc import Callable
from time import time


def timer(func: Callable):
    def wrapper(*args, **kwargs):
        start_time = time()

        func(*args, **kwargs)

        end_time = time()

        print("Function took", end_time - start_time)

    return wrapper
