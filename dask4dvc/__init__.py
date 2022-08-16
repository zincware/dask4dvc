import importlib.metadata
import logging

import dask4dvc.cli
import dask4dvc.dvc_handling
import dask4dvc.utils

log = logging.getLogger(__name__)

__version__ = importlib.metadata.version("dask4dvc")
