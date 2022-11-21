"""Utils that are related to 'dask'."""
import typing

from dask.distributed import Future


def wait_for_futures(futures: typing.Union[Future, typing.Dict[str, Future]]) -> None:
    """Wait for all given future objects to complete.

    Raises
    ------
    This will fail if one of them fails.
    """
    if isinstance(futures, Future):
        futures = {"main": futures}

    for future in futures.values():
        _ = future.result()
