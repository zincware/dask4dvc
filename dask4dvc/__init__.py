import importlib.metadata
import logging
import sys

import dask4dvc.cli
import dask4dvc.dvc_handling
import dask4dvc.utils

__version__ = importlib.metadata.version("dask4dvc")

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Formatter for advanced logging
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

channel = logging.StreamHandler(sys.stdout)
channel.setLevel(logging.INFO)
channel.setFormatter(formatter)

log.addHandler(channel)
