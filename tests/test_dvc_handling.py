import pathlib
from unittest import mock

import git
import networkx as nx

import dask4dvc

DOT_GRAPH = """strict digraph  {
"Node1";
"Node2";
"Node3";
"Node1" -> "Node2";
"Node1" -> "Node3";
}
"""


@mock.patch("dask4dvc.dvc_handling.subprocess.run")
def test_get_dvc_graph(mock_run):
    # ?? How to mock a subprocess call
    mock_stdout = mock.MagicMock()
    mock_stdout.configure_mock(**{"stdout.decode.return_value": DOT_GRAPH})

    mock_run.return_value = mock_stdout
    nx_graph = dask4dvc.dvc_handling.get_dvc_graph()

    assert isinstance(nx_graph, nx.DiGraph)

    assert list(nx_graph.predecessors("Node1")) == []
    assert list(nx_graph.successors("Node1")) == ["Node2", "Node3"]

    assert list(nx_graph.predecessors("Node2")) == ["Node1"]
    assert list(nx_graph.predecessors("Node3")) == ["Node1"]
    assert list(nx_graph.successors("Node2")) == []
    assert list(nx_graph.successors("Node3")) == []


def test_apply_git_diff(tmp_path):
    main = git.Repo.init(tmp_path / "main")
    main_file = tmp_path / "main" / "test.txt"
    main_file.write_text("Hello World")
    main.index.add([main_file.name])
    main.index.commit("Add File")

    assert main_file.read_text() == "Hello World"
    assert not main.is_dirty()

    clone = main.clone(tmp_path / "clone")
    main_file.write_text("Lorem Ipsum")

    dask4dvc.dvc_handling.apply_git_diff(main, clone)

    assert (tmp_path / "clone" / "test.txt").read_text() == "Lorem Ipsum"


def test_apply_git_diff_real(dvc_repository):
    tmp_path = pathlib.Path(dvc_repository.working_dir).parent
    dvc_repository.git.execute(["dvc", "repro"])

    dvc_repository.index.add(["dvc.lock"])
    dvc_repository.index.commit("dvc repro")
    (tmp_path / "main" / "script").write_text("Change Dependency")
    dvc_repository.git.execute(["dvc", "repro"])
    source_repo, target_repo = dask4dvc.dvc_handling.clone(
        tmp_path / "main", tmp_path / "clone"
    )

    dask4dvc.dvc_handling.apply_git_diff(source_repo, target_repo)

    assert (tmp_path / "clone" / "script").read_text() == "Change Dependency"


def test_load_exp_to_dict(dvc_repository):
    dvc_repository.git.execute(["dvc", "exp", "run", "--queue"])

    output_dict = dask4dvc.dvc_handling.get_queued_exp_names(
        cwd=dvc_repository.working_dir
    )

    assert len(output_dict) == 1
    assert next(iter(output_dict.values())) is None


def test_load_exp_to_dict_names(dvc_repository):
    for idx in range(5):
        dvc_repository.git.execute(
            ["dvc", "exp", "run", "--name", f"test_{idx}", "--queue"]
        )

    assert set(
        dask4dvc.dvc_handling.get_queued_exp_names(
            cwd=dvc_repository.working_dir
        ).values()
    ) == {f"test_{idx}" for idx in range(5)}
