import os
import pathlib
import shutil
import subprocess

import git
import pytest
from typer.testing import CliRunner

from dask4dvc.cli import app

runner = CliRunner()


def test_dvc_repository(dvc_repository):
    """Test that the repository is created correctly"""
    subprocess.check_call(["dvc", "repro"], cwd=dvc_repository.working_dir)


@pytest.mark.parametrize("branch_name", (None, "test", False))
def test_clone_branch(dvc_repository, tmp_path, branch_name):
    target_dir = tmp_path / "branch1"

    cmd = ["clone", dvc_repository.working_dir, str(target_dir)]
    if branch_name is False:
        cmd += ["--branch"]
    elif branch_name is not None:
        cmd += ["--branch-name", branch_name]
    result = runner.invoke(app, cmd)

    assert result.exit_code == 0
    target_repo = git.Repo(target_dir)
    if branch_name is None:
        assert target_repo.active_branch.name in ["master", "main"]
    elif branch_name is False:
        assert target_repo.active_branch.name == target_dir.name
    else:
        assert target_repo.active_branch.name == branch_name


def test_clone_patch(dvc_repository, tmp_path):
    (pathlib.Path(dvc_repository.working_dir) / "script").write_text("Hello World")
    # modify the file after it was added, so it appears in the diff

    # now we have uncommitted changes
    target_dir = tmp_path / "branch1"
    result = runner.invoke(
        app, ["clone", dvc_repository.working_dir, str(target_dir), "--apply-diff"]
    )
    assert result.exit_code == 0

    assert (target_dir / "script").read_text().startswith("Hello World")
    target_repo = git.Repo(target_dir)
    assert target_repo.is_dirty()


def test_clone_cache(dvc_repository, tmp_path):
    target_dir = tmp_path / "branch1"
    result = runner.invoke(app, ["clone", dvc_repository.working_dir, str(target_dir)])
    assert result.exit_code == 0

    dvc_repository.git.execute(["dvc", "repro"])

    target_repo = git.Repo(target_dir)
    dvc_repro_out = target_repo.git.execute(["dvc", "repro"])
    assert "is cached - skipping run, checking out outputs" in dvc_repro_out
    assert "Running stage" not in dvc_repro_out


def test_clone_and_patch(tmp_path, examples):
    main = tmp_path / "main"
    main_repo = git.Repo.init(main)
    main_repo.git.execute(["dvc", "init"])
    shutil.copy(examples, main)

    # build graph in some way

    main_repo.git.execute(
        [
            "python",
            "-c",
            "from examples import InputToOutput; InputToOutput(inputs=1).write_graph()",
        ]
    )
    print(main_repo.git.execute(["dvc", "repro"]))
    main_repo.git.execute(["git", "add", "."])
    # main_repo.index.add(".") # does not work
    main_repo.index.commit("Initial Commit")
    main_repo.git.execute(
        [
            "python",
            "-c",
            "from examples import InputToOutput; InputToOutput(inputs=2).write_graph()",
        ]
    )

    os.chdir(main)

    result = runner.invoke(app, ["repro", "--no-wait"])
    assert result.exit_code == 0
