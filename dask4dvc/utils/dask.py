"""Utils that are related to 'dask'."""
import logging
import typing

from dask.distributed import Future

log = logging.getLogger(__name__)


def wait_for_futures(futures: typing.Union[Future, typing.Dict[str, Future]]) -> None:
    """Wait for all given future objects to complete.

    Raises
    ------
    This will fail if one of them fails.
    """
    if isinstance(futures, Future):
        futures = {"main": futures}

    for future in futures.values():
        try:
            _ = future.result()
        except Exception as err:
            log.critical(f"Waiting for result from '{future}' failed with {err}")
