import re
from unittest import mock

import networkx as nx
import pytest

import dask4dvc


def graph_from_dot_data(dot_graph):
    with mock.patch("dask4dvc.dvc_handling.subprocess.run") as mock_run:
        mock_stdout = mock.MagicMock()
        mock_stdout.configure_mock(**{"stdout.decode.return_value": dot_graph})

        mock_run.return_value = mock_stdout
        return dask4dvc.dvc_handling.get_dvc_graph()


@pytest.fixture()
def nx_graph_1() -> nx.DiGraph:
    dot_graph = """strict digraph  {
    "Node1";
    "Node2";
    "Node3";
    "Node1" -> "Node2";
    "Node1" -> "Node3";
    }
    """
    return graph_from_dot_data(dot_graph)


@pytest.fixture()
def nx_graph_2() -> nx.DiGraph:
    dot_graph = """strict digraph  {
    "Node1";
    "Node2";
    "Node3";
    "Node4";
    "Node1" -> "Node2";
    "Node1" -> "Node3";
    }
    """
    return graph_from_dot_data(dot_graph)


def test_get_dvc_graph(nx_graph_1, nx_graph_2):
    assert dask4dvc.utils.get_starting_nodes(nx_graph_1) == ["Node1"]
    assert dask4dvc.utils.get_starting_nodes(nx_graph_2) == ["Node1", "Node4"]


def test_iterate_over_nodes(nx_graph_1, nx_graph_2):
    result = dask4dvc.utils.iterate_over_nodes(nx_graph_1)
    assert result == [("Node1", None), ("Node2", ["Node1"]), ("Node3", ["Node1"])]

    result = dask4dvc.utils.iterate_over_nodes(nx_graph_2)
    assert result == [
        ("Node1", None),
        ("Node4", None),
        ("Node2", ["Node1"]),
        ("Node3", ["Node1"]),
    ]


def test_submit_to_dask():
    node_pairs = [("Node1", None), ("Node2", ["Node1"]), ("Node3", ["Node1"])]
    client = mock.MagicMock()

    def cmd(**kwargs):
        return kwargs

    results = dask4dvc.utils.submit_to_dask(client=client, node_pairs=node_pairs, cmd=cmd)

    for node in ["Node1", "Node2", "Node2"]:
        # Test that all Node names occur.
        # The regex tests for e.g. 5c9dc848_Node1 with exactly 8 leading characters
        r = re.compile(r"^[a-zA-Z0-9]{8}_" + node)
        assert any(filter(r.match, results))
    # assert client.submit.assert_called_with(name="Node3")
