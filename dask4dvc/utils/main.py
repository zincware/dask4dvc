"""General 'dask4dvc' utils."""
import functools
import logging
import shutil
import time
import typing

log = logging.getLogger(__name__)


def timeit(func: typing.Callable) -> typing.Callable:
    """Timeit decorator for benchmarking."""

    @functools.wraps(func)
    def wrapper(*args: tuple, **kwargs: dict) -> typing.Any:
        """Wrapper for timeit."""
        start_time = time.time()
        result = func(*args, **kwargs)
        stop_time = time.time()
        log.debug(f"'{func.__name__}' took {stop_time - start_time:4f} s")
        return result

    return wrapper


def remove_paths(paths: list) -> None:
    """Remove all directories in paths."""
    for path in paths:
        shutil.rmtree(path)


def wait() -> None:
    """Wait until user input."""
    _ = input("Press Enter to close the client")
