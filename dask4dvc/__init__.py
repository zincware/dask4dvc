import importlib.metadata
import logging
import sys

from dask4dvc import cli, utils

__all__ = ["cli", "utils"]

__version__ = importlib.metadata.version("dask4dvc")

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Formatter for advanced logging
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

channel = logging.StreamHandler(sys.stdout)
channel.setLevel(logging.INFO)
channel.setFormatter(formatter)

log.addHandler(channel)
