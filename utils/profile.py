from timeit import default_timer
from contextlib import contextmanager


@contextmanager
def timer():
    start = default_timer()
    yield
    end = default_timer()
    print(f"Time: {end - start}")
    return end - start
