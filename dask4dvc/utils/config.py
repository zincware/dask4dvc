"""global config for 'dask4dvc'."""
import dataclasses


@dataclasses.dataclass
class Config:
    """dask4dvc config object.

    Attributes
    ----------
    tqdm_threshold: int
        The minimum number of experiments to show a tqdm bar.
    """

    tqdm_threshold: int = 10


CONFIG = Config()
