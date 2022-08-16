from unittest import mock

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
