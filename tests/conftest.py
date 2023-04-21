"""global pytest fixtures."""
import os
import pathlib
import dvc.cli
import git
import pytest
import shutil


@pytest.fixture
def repo_path(tmp_path: pathlib.Path, request: pytest.FixtureRequest) -> pathlib.Path:
    """Temporary directory for testing DVC calls."""
    shutil.copy(request.module.__file__, tmp_path)
    os.chdir(tmp_path)
    git.Repo.init()
    assert dvc.cli.main(["init"]) == 0

    return tmp_path
