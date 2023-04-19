"""global config for 'dask4dvc'."""
import dataclasses


@dataclasses.dataclass
class Config:
    """dask4dvc config object.

    Attributes
    ----------
    retries : int
        number of retries for acquiring lock
    """

    retries: int = 1000


CONFIG = Config()
