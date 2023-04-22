"""The 'dask4dvc' package."""
import importlib.metadata
import logging
import sys

import dvc.logger

from dask4dvc import cli, dvc_repro, utils

__all__ = ["cli", "utils", "dvc_repro"]

__version__ = importlib.metadata.version("dask4dvc")

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Formatter for advanced logging
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

channel = logging.StreamHandler(sys.stdout)
channel.setLevel(logging.INFO)
channel.setFormatter(formatter)

log.addHandler(channel)

# I haven't found a way of temporarily disabling the DVC logger
dvc.logger.set_loggers_level(logging.CRITICAL)
