"""global config for 'dask4dvc'."""
import dataclasses


@dataclasses.dataclass
class Config:
    """dask4dvc config object.

    Attributes
    ----------
    tqdm_threshold: int
        The minimum number of experiments to show a tqdm bar.
    use_dvc_api: bool
        Instead of subprocess calls use the internal DVC API.
    """

    tqdm_threshold: int = 10
    use_dvc_api: bool = True


CONFIG = Config()
